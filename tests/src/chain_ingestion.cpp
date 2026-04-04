#include "unit-tests.hpp"

#include <algorithm>
#include <chrono>
#include <cctype>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <future>
#include <optional>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <nlohmann/json.hpp>

#ifndef DECENTRALISED_ART_TEST_BINARY_DIR
    #error "DECENTRALISED_ART_TEST_BINARY_DIR is not defined"
#endif

using namespace dcn;
using namespace dcn::tests;

namespace
{
    using json = nlohmann::json;

    template<class AwaitableT>
    auto runAwaitable(asio::io_context & io_context, AwaitableT awaitable)
    {
        auto future = asio::co_spawn(io_context, std::move(awaitable), asio::use_future);
        io_context.restart();
        io_context.run();
        return future.get();
    }

    chain::Address makeAddressFromSuffix(const char* suffix)
    {
        chain::Address address{};
        const std::size_t suffix_len = std::strlen(suffix);
        if(suffix_len <= 20)
        {
            std::memcpy(address.bytes + (20 - suffix_len), suffix, suffix_len);
        }
        return address;
    }

    std::string toLower(std::string value)
    {
        std::ranges::transform(value, value.begin(), [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
        return value;
    }

    std::string hexPrefixed(const evmc::bytes32 & value)
    {
        return std::string("0x") + evmc::hex(value);
    }

    std::string hexPrefixed(const chain::Address & value)
    {
        return std::string("0x") + evmc::hex(value);
    }

    std::string hexPrefixed(const std::vector<std::uint8_t> & value)
    {
        return std::string("0x") + evmc::hex(evmc::bytes_view{value.data(), value.size()});
    }

    std::vector<std::uint8_t> encodeUint256Word(std::uint64_t value)
    {
        std::vector<std::uint8_t> out(32, 0);
        for(int i = 0; i < 8; ++i)
        {
            out[31 - i] = static_cast<std::uint8_t>((value >> (8 * i)) & 0xFFu);
        }
        return out;
    }

    std::vector<std::uint8_t> encodeAddressWord(const chain::Address & value)
    {
        std::vector<std::uint8_t> out(32, 0);
        std::memcpy(out.data() + 12, value.bytes, 20);
        return out;
    }

    std::vector<std::uint8_t> encodeInt32Word(std::int32_t value)
    {
        std::vector<std::uint8_t> out(32, value < 0 ? 0xFFu : 0x00u);
        const std::uint32_t raw = static_cast<std::uint32_t>(value);
        out[28] = static_cast<std::uint8_t>((raw >> 24) & 0xFFu);
        out[29] = static_cast<std::uint8_t>((raw >> 16) & 0xFFu);
        out[30] = static_cast<std::uint8_t>((raw >> 8) & 0xFFu);
        out[31] = static_cast<std::uint8_t>(raw & 0xFFu);
        return out;
    }

    std::vector<std::uint8_t> encodeStringTail(const std::string & value)
    {
        std::vector<std::uint8_t> out = encodeUint256Word(value.size());
        out.insert(out.end(), value.begin(), value.end());
        const std::size_t pad = (32 - (value.size() % 32)) % 32;
        out.insert(out.end(), pad, 0);
        return out;
    }

    std::vector<std::uint8_t> encodeStringArrayTail(const std::vector<std::string> & values)
    {
        std::vector<std::uint8_t> out = encodeUint256Word(values.size());
        std::vector<std::vector<std::uint8_t>> tails;
        tails.reserve(values.size());

        std::size_t current_offset = values.size() * 32;
        for(const std::string & value : values)
        {
            const auto offset_word = encodeUint256Word(current_offset);
            out.insert(out.end(), offset_word.begin(), offset_word.end());

            tails.push_back(encodeStringTail(value));
            current_offset += tails.back().size();
        }

        for(const auto & tail : tails)
        {
            out.insert(out.end(), tail.begin(), tail.end());
        }

        return out;
    }

    std::vector<std::uint8_t> encodeInt32ArrayTail(const std::vector<std::int32_t> & values)
    {
        std::vector<std::uint8_t> out = encodeUint256Word(values.size());
        for(const std::int32_t value : values)
        {
            const auto encoded = encodeInt32Word(value);
            out.insert(out.end(), encoded.begin(), encoded.end());
        }
        return out;
    }

    std::vector<std::uint8_t> encodeUint32ArrayTail(const std::vector<std::uint32_t> & values)
    {
        std::vector<std::uint8_t> out = encodeUint256Word(values.size());
        for(const std::uint32_t value : values)
        {
            out.insert(out.end(), 28, 0);
            out.push_back(static_cast<std::uint8_t>((value >> 24) & 0xFFu));
            out.push_back(static_cast<std::uint8_t>((value >> 16) & 0xFFu));
            out.push_back(static_cast<std::uint8_t>((value >> 8) & 0xFFu));
            out.push_back(static_cast<std::uint8_t>(value & 0xFFu));
        }
        return out;
    }

    std::string encodeSimpleAddedEventDataLegacy(const chain::Address & caller,
                                                 const std::string & name,
                                                 const chain::Address & entity_address)
    {
        std::vector<std::uint8_t> out;
        const auto name_tail = encodeStringTail(name);

        const auto caller_word = encodeAddressWord(caller);
        const auto name_offset_word = encodeUint256Word(96);
        const auto entity_word = encodeAddressWord(entity_address);

        out.insert(out.end(), caller_word.begin(), caller_word.end());
        out.insert(out.end(), name_offset_word.begin(), name_offset_word.end());
        out.insert(out.end(), entity_word.begin(), entity_word.end());
        out.insert(out.end(), name_tail.begin(), name_tail.end());

        return hexPrefixed(out);
    }

    std::string encodeSimpleAddedEventDataV2(const chain::Address & caller,
                                             const std::string & name,
                                             const chain::Address & entity_address,
                                             const chain::Address & owner,
                                             std::uint32_t count)
    {
        std::vector<std::uint8_t> out;
        const auto name_tail = encodeStringTail(name);

        const auto caller_word = encodeAddressWord(caller);
        const auto name_offset_word = encodeUint256Word(160);
        const auto entity_word = encodeAddressWord(entity_address);
        const auto owner_word = encodeAddressWord(owner);
        const auto count_word = encodeUint256Word(count);

        out.insert(out.end(), caller_word.begin(), caller_word.end());
        out.insert(out.end(), name_offset_word.begin(), name_offset_word.end());
        out.insert(out.end(), entity_word.begin(), entity_word.end());
        out.insert(out.end(), owner_word.begin(), owner_word.end());
        out.insert(out.end(), count_word.begin(), count_word.end());
        out.insert(out.end(), name_tail.begin(), name_tail.end());

        return hexPrefixed(out);
    }

    std::string encodeConnectorAddedEventData(const std::string & name,
                                             const chain::Address & connector_address,
                                             const std::uint32_t dimensions_count,
                                             const std::vector<std::uint32_t> & composite_dim_ids,
                                             const std::vector<std::string> & composite_names,
                                             const std::vector<std::uint32_t> & binding_dim_ids,
                                             const std::vector<std::uint32_t> & binding_slot_ids,
                                             const std::vector<std::string> & binding_names,
                                             const std::string & condition_name,
                                             const std::vector<std::int32_t> & condition_args)
    {
        const auto name_tail = encodeStringTail(name);
        const auto dimensions_count_word = encodeUint256Word(dimensions_count);
        const auto composite_dim_ids_tail = encodeUint32ArrayTail(composite_dim_ids);
        const auto composite_names_tail = encodeStringArrayTail(composite_names);
        const auto binding_dim_ids_tail = encodeUint32ArrayTail(binding_dim_ids);
        const auto binding_slot_ids_tail = encodeUint32ArrayTail(binding_slot_ids);
        const auto binding_names_tail = encodeStringArrayTail(binding_names);
        const auto condition_tail = encodeStringTail(condition_name);
        const auto condition_args_tail = encodeInt32ArrayTail(condition_args);

        const std::size_t head_size = 10 * 32;
        const std::size_t name_offset = head_size;
        const std::size_t composite_dim_ids_offset = name_offset + name_tail.size();
        const std::size_t composite_names_offset = composite_dim_ids_offset + composite_dim_ids_tail.size();
        const std::size_t binding_dim_ids_offset = composite_names_offset + composite_names_tail.size();
        const std::size_t binding_slot_ids_offset = binding_dim_ids_offset + binding_dim_ids_tail.size();
        const std::size_t binding_names_offset = binding_slot_ids_offset + binding_slot_ids_tail.size();
        const std::size_t condition_offset = binding_names_offset + binding_names_tail.size();
        const std::size_t condition_args_offset = condition_offset + condition_tail.size();

        std::vector<std::uint8_t> out;
        const auto name_offset_word = encodeUint256Word(name_offset);
        const auto connector_word = encodeAddressWord(connector_address);
        const auto composite_dim_ids_offset_word = encodeUint256Word(composite_dim_ids_offset);
        const auto composite_names_offset_word = encodeUint256Word(composite_names_offset);
        const auto binding_dim_ids_offset_word = encodeUint256Word(binding_dim_ids_offset);
        const auto binding_slot_ids_offset_word = encodeUint256Word(binding_slot_ids_offset);
        const auto binding_names_offset_word = encodeUint256Word(binding_names_offset);
        const auto condition_offset_word = encodeUint256Word(condition_offset);
        const auto condition_args_offset_word = encodeUint256Word(condition_args_offset);

        out.insert(out.end(), name_offset_word.begin(), name_offset_word.end());
        out.insert(out.end(), connector_word.begin(), connector_word.end());
        out.insert(out.end(), dimensions_count_word.begin(), dimensions_count_word.end());
        out.insert(out.end(), composite_dim_ids_offset_word.begin(), composite_dim_ids_offset_word.end());
        out.insert(out.end(), composite_names_offset_word.begin(), composite_names_offset_word.end());
        out.insert(out.end(), binding_dim_ids_offset_word.begin(), binding_dim_ids_offset_word.end());
        out.insert(out.end(), binding_slot_ids_offset_word.begin(), binding_slot_ids_offset_word.end());
        out.insert(out.end(), binding_names_offset_word.begin(), binding_names_offset_word.end());
        out.insert(out.end(), condition_offset_word.begin(), condition_offset_word.end());
        out.insert(out.end(), condition_args_offset_word.begin(), condition_args_offset_word.end());

        out.insert(out.end(), name_tail.begin(), name_tail.end());
        out.insert(out.end(), composite_dim_ids_tail.begin(), composite_dim_ids_tail.end());
        out.insert(out.end(), composite_names_tail.begin(), composite_names_tail.end());
        out.insert(out.end(), binding_dim_ids_tail.begin(), binding_dim_ids_tail.end());
        out.insert(out.end(), binding_slot_ids_tail.begin(), binding_slot_ids_tail.end());
        out.insert(out.end(), binding_names_tail.begin(), binding_names_tail.end());
        out.insert(out.end(), condition_tail.begin(), condition_tail.end());
        out.insert(out.end(), condition_args_tail.begin(), condition_args_tail.end());

        return hexPrefixed(out);
    }

    std::string encodeOwnerCallResult(const chain::Address & owner)
    {
        return hexPrefixed(encodeAddressWord(owner));
    }

    std::string topicForEvent(const std::string & signature)
    {
        return hexPrefixed(crypto::constructEventTopic(signature));
    }

    std::string topicForAddress(const chain::Address & address)
    {
        evmc::bytes32 topic_word{};
        std::memcpy(topic_word.bytes + 12, address.bytes, 20);
        return hexPrefixed(topic_word);
    }

    std::filesystem::path buildPath()
    {
        return std::filesystem::path(DECENTRALISED_ART_TEST_BINARY_DIR);
    }

    std::filesystem::path makeStoragePath(const std::string & test_name)
    {
        const std::string unique_suffix = std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
        const auto storage_path = buildPath() / "tests" / "chain_storage" / (test_name + "_" + unique_suffix);

        std::error_code ec;
        std::filesystem::remove_all(storage_path, ec);
        ec.clear();
        std::filesystem::create_directories(storage_path, ec);
        EXPECT_FALSE(ec);

        return storage_path;
    }

    json readJsonFile(const std::filesystem::path & file_path)
    {
        std::ifstream input(file_path);
        if(!input.is_open())
        {
            return json::object();
        }

        return json::parse(input, nullptr, false);
    }

    struct MockRpcNode
    {
        std::string block_number_hex;
        json logs;
        std::unordered_map<std::string, std::string> owner_results;

        std::size_t block_number_calls = 0;
        std::size_t get_logs_calls = 0;
        std::size_t eth_call_calls = 0;

        std::optional<json> call(const std::string &, const json & request)
        {
            const std::string method = request.value("method", "");
            if(method == "eth_blockNumber")
            {
                ++block_number_calls;
                return json{
                    {"jsonrpc", "2.0"},
                    {"id", 1},
                    {"result", block_number_hex}
                };
            }

            if(method == "eth_getLogs")
            {
                ++get_logs_calls;
                return json{
                    {"jsonrpc", "2.0"},
                    {"id", 1},
                    {"result", logs}
                };
            }

            if(method == "eth_call")
            {
                ++eth_call_calls;
                if(!request.contains("params") || !request["params"].is_array() || request["params"].empty() || !request["params"][0].is_object())
                {
                    return std::nullopt;
                }

                const std::string to_address = toLower(request["params"][0].value("to", ""));
                const auto it = owner_results.find(to_address);
                if(it == owner_results.end())
                {
                    return std::nullopt;
                }

                return json{
                    {"jsonrpc", "2.0"},
                    {"id", 1},
                    {"result", it->second}
                };
            }

            return std::nullopt;
        }
    };
}

TEST_F(UnitTest, Chain_Ingestion_RegistersConnectorFromFetchedEvents)
{
    asio::io_context io_context{};
    storage::Registry registry(io_context);

    const auto storage_path = makeStoragePath("connector_events");

    const chain::Address registry_address = makeAddressFromSuffix("registry");
    const chain::Address caller = makeAddressFromSuffix("caller");
    const chain::Address owner = makeAddressFromSuffix("owner");
    const chain::Address connector_address = makeAddressFromSuffix("connector");

    const json connector_log = {
        {"blockNumber", "0x64"},
        {"logIndex", "0x1"},
        {"topics", json::array({
            topicForEvent("ConnectorAdded(address,address,string,address,uint32,uint32[],string[],uint32[],uint32[],string[],string,int32[])"),
            topicForAddress(caller),
            topicForAddress(owner)
        })},
        {"data", encodeConnectorAddedEventData(
            "ConnectorAlpha",
            connector_address,
            2,
            {},
            {},
            {},
            {},
            {},
            "",
            {7, -3})}
    };

    MockRpcNode rpc;
    rpc.block_number_hex = "0x64";
    rpc.logs = json::array({connector_log});

    chain::IngestionConfig config;
    config.enabled = true;
    config.rpc_url = "mock://rpc";
    config.registry_address = registry_address;
    config.start_block = 100;
    config.poll_interval_ms = 0;
    config.confirmations = 0;
    config.block_batch_size = 100;
    config.storage_path = storage_path;

    chain::IngestionRuntimeOptions runtime_options;
    runtime_options.rpc_call = [&rpc](const std::string & rpc_url, const json & request)
    {
        return rpc.call(rpc_url, request);
    };
    runtime_options.max_polls = 1;
    runtime_options.skip_sleep = true;

    runAwaitable(io_context, chain::runEventIngestion(config, registry, runtime_options));

    const auto connector_res = runAwaitable(io_context, registry.getConnectorRecordHandle("ConnectorAlpha"));
    ASSERT_TRUE(connector_res.has_value());
    ASSERT_TRUE(*connector_res);
    ASSERT_EQ((*connector_res)->connector().dimensions_size(), 2);
    EXPECT_EQ((*connector_res)->connector().condition_name(), "");
    ASSERT_EQ((*connector_res)->connector().condition_args_size(), 2);
    EXPECT_EQ((*connector_res)->connector().condition_args(0), 7);
    EXPECT_EQ((*connector_res)->connector().condition_args(1), -3);

    const auto owned_connectors = runAwaitable(
        io_context,
        registry.getOwnedConnectorsCursor(owner, std::nullopt, 128));
    EXPECT_TRUE(std::find(
        owned_connectors.entries.begin(),
        owned_connectors.entries.end(),
        "ConnectorAlpha") != owned_connectors.entries.end());

    EXPECT_TRUE(std::filesystem::exists(storage_path / "connectors" / "ConnectorAlpha.json"));

    const json cursor_state = readJsonFile(storage_path / "chain" / "cursor.json");
    ASSERT_TRUE(cursor_state.is_object());
    EXPECT_EQ(cursor_state.value("next_block", ""), "0x65");

    EXPECT_EQ(rpc.block_number_calls, 1);
    EXPECT_EQ(rpc.get_logs_calls, 1);
    EXPECT_EQ(rpc.eth_call_calls, 0);
}

TEST_F(UnitTest, Chain_Ingestion_RegistersTransformationAndConditionFromFetchedEvents)
{
    asio::io_context io_context{};
    storage::Registry registry(io_context);

    const auto storage_path = makeStoragePath("transformation_condition_events");

    const chain::Address registry_address = makeAddressFromSuffix("registry");
    const chain::Address transformation_caller = makeAddressFromSuffix("txcaller");
    const chain::Address condition_caller = makeAddressFromSuffix("ccaller");
    const chain::Address owner = makeAddressFromSuffix("owner");
    const chain::Address transformation_address = makeAddressFromSuffix("transform");
    const chain::Address condition_address = makeAddressFromSuffix("condition");

    const json transformation_log = {
        {"blockNumber", "0x2a"},
        {"logIndex", "0x0"},
        {"topics", json::array({
            topicForEvent("TransformationAdded(address,string,address,address,uint32)")
        })},
        {"data", encodeSimpleAddedEventDataV2(
            transformation_caller,
            "TransformAlpha",
            transformation_address,
            owner,
            2)}
    };

    const json condition_log = {
        {"blockNumber", "0x2a"},
        {"logIndex", "0x1"},
        {"topics", json::array({
            topicForEvent("ConditionAdded(address,string,address,address,uint32)")
        })},
        {"data", encodeSimpleAddedEventDataV2(
            condition_caller,
            "ConditionAlpha",
            condition_address,
            owner,
            1)}
    };

    MockRpcNode rpc;
    rpc.block_number_hex = "0x2a";
    rpc.logs = json::array({condition_log, transformation_log});
    rpc.owner_results.emplace(
        toLower(hexPrefixed(transformation_address)),
        encodeOwnerCallResult(owner));

    chain::IngestionConfig config;
    config.enabled = true;
    config.rpc_url = "mock://rpc";
    config.registry_address = registry_address;
    config.start_block = 42;
    config.poll_interval_ms = 0;
    config.confirmations = 0;
    config.block_batch_size = 100;
    config.storage_path = storage_path;

    chain::IngestionRuntimeOptions runtime_options;
    runtime_options.rpc_call = [&rpc](const std::string & rpc_url, const json & request)
    {
        return rpc.call(rpc_url, request);
    };
    runtime_options.max_polls = 1;
    runtime_options.skip_sleep = true;

    runAwaitable(io_context, chain::runEventIngestion(config, registry, runtime_options));

    const auto transformation_res =
        runAwaitable(io_context, registry.getTransformationRecordHandle("TransformAlpha"));
    ASSERT_TRUE(transformation_res.has_value());
    ASSERT_TRUE(*transformation_res);

    const auto condition_res =
        runAwaitable(io_context, registry.getConditionRecordHandle("ConditionAlpha"));
    ASSERT_TRUE(condition_res.has_value());
    ASSERT_TRUE(*condition_res);

    const auto owned_transformations = runAwaitable(
        io_context,
        registry.getOwnedTransformationsCursor(owner, std::nullopt, 128));
    EXPECT_TRUE(std::find(
        owned_transformations.entries.begin(),
        owned_transformations.entries.end(),
        "TransformAlpha") != owned_transformations.entries.end());

    const auto owned_conditions = runAwaitable(
        io_context,
        registry.getOwnedConditionsCursor(condition_caller, std::nullopt, 128));
    EXPECT_TRUE(std::find(
        owned_conditions.entries.begin(),
        owned_conditions.entries.end(),
        "ConditionAlpha") != owned_conditions.entries.end());

    EXPECT_TRUE(std::filesystem::exists(storage_path / "transformations" / "TransformAlpha.json"));
    EXPECT_TRUE(std::filesystem::exists(storage_path / "conditions" / "ConditionAlpha.json"));

    const json cursor_state = readJsonFile(storage_path / "chain" / "cursor.json");
    ASSERT_TRUE(cursor_state.is_object());
    EXPECT_EQ(cursor_state.value("next_block", ""), "0x2b");

    EXPECT_EQ(rpc.block_number_calls, 1);
    EXPECT_EQ(rpc.get_logs_calls, 1);
    EXPECT_EQ(rpc.eth_call_calls, 2);
}

