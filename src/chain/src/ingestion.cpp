#include "ingestion.hpp"

#include <algorithm>
#include <cctype>
#include <chrono>
#include <cstring>
#include <fstream>
#include <format>
#include <limits>
#include <utility>

#include <nlohmann/json.hpp>

#include <spdlog/spdlog.h>

#include "native.h"
#include "parser.hpp"
#include "utils.hpp"

namespace dcn::chain
{
    using json = nlohmann::json;

    namespace
    {
        struct SimpleAddedEvent
        {
            chain::Address caller{};
            std::string name;
            chain::Address entity_address{};
        };

        // struct PendingParticle
        // {
        //     chain::Address address{};
        //     ParticleRecord record;
        // };

        std::string _toLower(std::string value)
        {
            std::ranges::transform(value, value.begin(), [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
            return value;
        }

        std::string _withHexPrefix(std::string value)
        {
            if(value.rfind("0x", 0) == 0 || value.rfind("0X", 0) == 0)
            {
                return value;
            }
            return std::string("0x") + value;
        }

        std::string _toHexQuantity(const std::uint64_t value)
        {
            return std::format("0x{:x}", value);
        }

        std::optional<std::uint64_t> _parseHexQuantity(const std::string & value)
        {
            try
            {
                if(value.empty())
                {
                    return std::nullopt;
                }

                if(value.rfind("0x", 0) == 0 || value.rfind("0X", 0) == 0)
                {
                    return static_cast<std::uint64_t>(std::stoull(value.substr(2), nullptr, 16));
                }

                return static_cast<std::uint64_t>(std::stoull(value, nullptr, 10));
            }
            catch(...)
            {
                return std::nullopt;
            }
        }

        std::optional<std::size_t> _readWordAsSizeT(const std::uint8_t* data, std::size_t data_size, std::size_t offset)
        {
            if(data == nullptr || offset + 32 > data_size)
            {
                return std::nullopt;
            }

            std::size_t value = 0;
            constexpr std::size_t prefix = 32 - sizeof(std::size_t);
            for(std::size_t i = 0; i < prefix; ++i)
            {
                if(data[offset + i] != 0)
                {
                    return std::nullopt;
                }
            }

            for(std::size_t i = prefix; i < 32; ++i)
            {
                value = (value << 8) | data[offset + i];
            }

            return value;
        }

        std::optional<SimpleAddedEvent> _decodeSimpleAddedEvent(const std::string & data_hex)
        {
            const auto bytes_res = evmc::from_hex(data_hex);
            if(!bytes_res || bytes_res->size() < 96)
            {
                return std::nullopt;
            }

            const auto & bytes = *bytes_res;
            const auto caller_res = chain::readAddressWord(bytes.data(), bytes.size(), 0);
            const auto name_offset_res = _readWordAsSizeT(bytes.data(), bytes.size(), 32);
            const auto entity_res = chain::readAddressWord(bytes.data(), bytes.size(), 64);
            if(!caller_res || !name_offset_res || !entity_res)
            {
                return std::nullopt;
            }

            const auto name_res = utils::decodeAbiString(bytes.data(), bytes.size(), *name_offset_res);
            if(!name_res)
            {
                return std::nullopt;
            }

            return SimpleAddedEvent{
                .caller = *caller_res,
                .name = *name_res,
                .entity_address = *entity_res
            };
        }

        std::optional<json> _rpcCallWithCurl(const std::string & rpc_url, const json & request)
        {
            std::vector<std::string> args{
                "-sS",
                "-X", "POST",
                rpc_url,
                "-H", "Content-Type: application/json",
                "--data", request.dump()
            };

            try
            {
                const auto [exit_code, output] = native::runProcess("curl", std::move(args));
                if(exit_code != 0)
                {
                    spdlog::error("Chain RPC call failed (exit={}): {}", exit_code, output);
                    return std::nullopt;
                }

                return json::parse(output);
            }
            catch(const std::exception & e)
            {
                spdlog::error("Chain RPC call failed: {}", e.what());
                return std::nullopt;
            }
        }

        std::optional<json> _rpcResult(const RpcCall & rpc_call, const std::string & rpc_url, const std::string & method, json params)
        {
            json request{
                {"jsonrpc", "2.0"},
                {"id", 1},
                {"method", method},
                {"params", std::move(params)}
            };

            const auto response = rpc_call(rpc_url, request);
            if(!response)
            {
                return std::nullopt;
            }

            if(response->contains("error"))
            {
                spdlog::error("Chain RPC error on '{}': {}", method, (*response)["error"].dump());
                return std::nullopt;
            }

            if(!response->contains("result"))
            {
                spdlog::error("Chain RPC malformed response on '{}': missing result", method);
                return std::nullopt;
            }

            return (*response)["result"];
        }

        std::optional<std::uint64_t> _ethBlockNumber(const RpcCall & rpc_call, const std::string & rpc_url)
        {
            const auto result = _rpcResult(rpc_call, rpc_url, "eth_blockNumber", json::array());
            if(!result || !result->is_string())
            {
                return std::nullopt;
            }

            return _parseHexQuantity(result->get<std::string>());
        }

        std::optional<json> _ethGetLogs(const RpcCall & rpc_call,
                                        const std::string & rpc_url,
                                        const chain::Address & registry_address,
                                        const std::uint64_t from_block,
                                        const std::uint64_t to_block,
                                        const std::vector<std::string> & topic0_or_filter)
        {
            json filter{
                {"address", _withHexPrefix(evmc::hex(registry_address))},
                {"fromBlock", _toHexQuantity(from_block)},
                {"toBlock", _toHexQuantity(to_block)},
                {"topics", json::array({topic0_or_filter})}
            };

            const auto result = _rpcResult(rpc_call, rpc_url, "eth_getLogs", json::array({std::move(filter)}));
            if(!result || !result->is_array())
            {
                return std::nullopt;
            }

            return *result;
        }

        std::optional<chain::Address> _ethGetOwner(const RpcCall & rpc_call, const std::string & rpc_url, const chain::Address & contract_address)
        {
            const auto selector = crypto::constructSelector("getOwner()");
            const std::string selector_hex = _withHexPrefix(evmc::hex(evmc::bytes_view{selector.data(), selector.size()}));

            json call_obj{
                {"to", _withHexPrefix(evmc::hex(contract_address))},
                {"data", selector_hex}
            };

            const auto result = _rpcResult(rpc_call, rpc_url, "eth_call", json::array({std::move(call_obj), "latest"}));
            if(!result || !result->is_string())
            {
                return std::nullopt;
            }

            const auto output_res = evmc::from_hex<evmc::bytes32>(result->get<std::string>());
            if(!output_res)
            {
                return std::nullopt;
            }

            chain::Address owner{};
            std::memcpy(owner.bytes, output_res->bytes + 12, 20);
            return owner;
        }
        
        // TODO we want to get multiple chains possible, so Sepolia and Mainnet for example ect.
        std::filesystem::path _stateFilePath(const IngestionConfig & cfg)
        {
            return cfg.storage_path / "chain" / "cursor.json";
        }

        std::optional<std::uint64_t> _loadNextBlock(const std::filesystem::path & state_path)
        {
            if(!std::filesystem::exists(state_path))
            {
                return std::nullopt;
            }

            std::ifstream input(state_path);
            if(!input.is_open())
            {
                return std::nullopt;
            }

            try
            {
                json state = json::parse(input);
                if(!state.contains("next_block"))
                {
                    return std::nullopt;
                }

                if(state["next_block"].is_string())
                {
                    return _parseHexQuantity(state["next_block"].get<std::string>());
                }

                if(state["next_block"].is_number_unsigned())
                {
                    return state["next_block"].get<std::uint64_t>();
                }
            }
            catch(const std::exception & e)
            {
                spdlog::warn("Failed to parse chain cursor '{}': {}", state_path.string(), e.what());
            }

            return std::nullopt;
        }

        bool _saveNextBlock(const std::filesystem::path & state_path, const std::uint64_t next_block)
        {
            try
            {
                std::filesystem::create_directories(state_path.parent_path());
                std::ofstream output(state_path, std::ios::out | std::ios::trunc);
                if(!output.is_open())
                {
                    return false;
                }

                json state{
                    {"next_block", _toHexQuantity(next_block)}
                };
                output << state.dump(2);
                return true;
            }
            catch(const std::exception & e)
            {
                spdlog::warn("Failed to write chain cursor '{}': {}", state_path.string(), e.what());
                return false;
            }
        }

        std::string _sanitizeRecordName(std::string name)
        {
            for(char & c : name)
            {
                if(c == '\\' || c == '/' || c == ':' || c == '*' || c == '?' || c == '"' || c == '<' || c == '>' || c == '|')
                {
                    c = '_';
                }
            }
            return name;
        }

        template<class RecordT>
        bool _saveRecordJson(const std::filesystem::path & out_dir, const std::string & name, const RecordT & record)
        {
            try
            {
                std::filesystem::create_directories(out_dir);
            }
            catch(const std::exception & e)
            {
                spdlog::warn("Failed to create storage directory '{}': {}", out_dir.string(), e.what());
                return false;
            }

            const auto json_res = parse::parseToJson(record, parse::use_protobuf);
            if(!json_res)
            {
                spdlog::warn("Failed to serialize synced record '{}'", name);
                return false;
            }

            const auto safe_name = _sanitizeRecordName(name);
            const auto output_path = out_dir / (safe_name + ".json");
            std::ofstream output(output_path, std::ios::out | std::ios::trunc);
            if(!output.is_open())
            {
                spdlog::warn("Failed to open synced record output '{}'", output_path.string());
                return false;
            }

            output << *json_res;
            return true;
        }

        // asio::awaitable<bool> _addFeatureRecord(registry::Registry & registry, const std::filesystem::path & storage_path,
        //                                         const chain::Address & address, const FeatureRecord & record)
        // {
        //     bool added = co_await registry.addFeature(address, record);
        //     if(!added)
        //     {
        //         const auto existing = co_await registry.getFeature(record.feature().name(), address);
        //         added = existing.has_value();
        //     }

        //     if(added)
        //     {
        //         _saveRecordJson(storage_path / "features", record.feature().name(), record);
        //     }

        //     co_return added;
        // }

        // asio::awaitable<bool> _addTransformationRecord(registry::Registry & registry, const std::filesystem::path & storage_path,
        //                                                const chain::Address & address, const TransformationRecord & record)
        // {
        //     bool added = co_await registry.addTransformation(address, record);
        //     if(!added)
        //     {
        //         const auto existing = co_await registry.getTransformation(record.transformation().name(), address);
        //         added = existing.has_value();
        //     }

        //     if(added)
        //     {
        //         _saveRecordJson(storage_path / "transformations", record.transformation().name(), record);
        //     }

        //     co_return added;
        // }

        // asio::awaitable<bool> _addConditionRecord(registry::Registry & registry, const std::filesystem::path & storage_path,
        //                                           const chain::Address & address, const ConditionRecord & record)
        // {
        //     bool added = co_await registry.addCondition(address, record);
        //     if(!added)
        //     {
        //         const auto existing = co_await registry.getCondition(record.condition().name(), address);
        //         added = existing.has_value();
        //     }

        //     if(added)
        //     {
        //         _saveRecordJson(storage_path / "conditions", record.condition().name(), record);
        //     }

        //     co_return added;
        // }

        // asio::awaitable<bool> _addParticleRecord(registry::Registry & registry, const std::filesystem::path & storage_path,
        //                                          const chain::Address & address, const ParticleRecord & record)
        // {
        //     bool added = co_await registry.addParticle(address, record);
        //     if(!added)
        //     {
        //         const auto existing = co_await registry.getParticle(record.particle().name(), address);
        //         added = existing.has_value();
        //     }

        //     if(added)
        //     {
        //         _saveRecordJson(storage_path / "particles", record.particle().name(), record);
        //     }

        //     co_return added;
        // }

        // bool _isSamePendingParticle(const PendingParticle & item, const chain::Address & address, const std::string & name)
        // {
        //     return item.address == address && item.record.particle().name() == name;
        // }

        // asio::awaitable<void> _retryPendingParticles(std::vector<PendingParticle> & pending_particles,
        //                                              registry::Registry & registry,
        //                                              const std::filesystem::path & storage_path)
        // {
        //     std::vector<PendingParticle> unresolved;
        //     unresolved.reserve(pending_particles.size());

        //     for(PendingParticle & pending : pending_particles)
        //     {
        //         if(!co_await _addParticleRecord(registry, storage_path, pending.address, pending.record))
        //         {
        //             unresolved.push_back(std::move(pending));
        //         }
        //     }

        //     pending_particles = std::move(unresolved);
        // }

        asio::awaitable<void> _sleepFor(const std::uint64_t ms)
        {
            asio::steady_timer timer(co_await asio::this_coro::executor);
            timer.expires_after(std::chrono::milliseconds(ms));
            co_await timer.async_wait(asio::use_awaitable);
        }
    }

    // asio::awaitable<void> runEventIngestion(IngestionConfig cfg, registry::Registry & registry, IngestionRuntimeOptions runtime_options)
    // {
    //     if(!cfg.enabled)
    //     {
    //         co_return;
    //     }

    //     if(cfg.rpc_url.empty())
    //     {
    //         spdlog::error("Chain sync enabled but RPC URL is empty");
    //         co_return;
    //     }

    //     if(cfg.block_batch_size == 0)
    //     {
    //         cfg.block_batch_size = 1;
    //     }

    //     const RpcCall rpc_call = runtime_options.rpc_call ? runtime_options.rpc_call : RpcCall{_rpcCallWithCurl};
    //     if(!rpc_call)
    //     {
    //         spdlog::error("Chain sync has no RPC call provider");
    //         co_return;
    //     }

    //     const auto state_path = _stateFilePath(cfg);
    //     std::optional<std::uint64_t> next_block = _loadNextBlock(state_path);
    //     std::optional<std::size_t> polls_remaining = runtime_options.max_polls;

    //     const std::string feature_added_topic = _toLower(_withHexPrefix(evmc::hex(
    //         evm::constructEventTopic("FeatureAdded(address,string,address,address,uint32)"))));
    //     const std::string transformation_added_topic = _toLower(_withHexPrefix(evmc::hex(
    //         evm::constructEventTopic("TransformationAdded(address,string,address,address,uint32)"))));
    //     const std::string condition_added_topic = _toLower(_withHexPrefix(evmc::hex(
    //         evm::constructEventTopic("ConditionAdded(address,string,address,address,uint32)"))));
    //     const std::string feature_added_topic_legacy = _toLower(_withHexPrefix(evmc::hex(
    //         evm::constructEventTopic("FeatureAdded(address,string,address)"))));
    //     const std::string transformation_added_topic_legacy = _toLower(_withHexPrefix(evmc::hex(
    //         evm::constructEventTopic("TransformationAdded(address,string,address)"))));
    //     const std::string condition_added_topic_legacy = _toLower(_withHexPrefix(evmc::hex(
    //         evm::constructEventTopic("ConditionAdded(address,string,address)"))));
    //     const std::string particle_added_topic = _toLower(_withHexPrefix(evmc::hex(
    //         evm::constructEventTopic("ParticleAdded(address,address,string,address,string,string[],string,int32[])"))));

    //     const std::vector<std::string> tracked_topics{
    //         feature_added_topic,
    //         transformation_added_topic,
    //         condition_added_topic,
    //         feature_added_topic_legacy,
    //         transformation_added_topic_legacy,
    //         condition_added_topic_legacy,
    //         particle_added_topic
    //     };

    //     std::vector<PendingParticle> pending_particles;

    //     spdlog::info("Chain ingestion started for registry {}", evmc::hex(cfg.registry_address));

    //     while(true)
    //     {
    //         if(polls_remaining)
    //         {
    //             if(*polls_remaining == 0)
    //             {
    //                 break;
    //             }
    //             --(*polls_remaining);
    //         }

    //         const auto head_block_res = _ethBlockNumber(rpc_call, cfg.rpc_url);
    //         if(!head_block_res)
    //         {
    //             if(!runtime_options.skip_sleep)
    //             {
    //                 co_await _sleepFor(cfg.poll_interval_ms);
    //             }
    //             continue;
    //         }

    //         const std::uint64_t head_block = *head_block_res;
    //         const std::uint64_t safe_head = (head_block > cfg.confirmations)
    //             ? (head_block - cfg.confirmations)
    //             : 0;

    //         if(!next_block)
    //         {
    //             next_block = cfg.start_block.value_or(safe_head);
    //             _saveNextBlock(state_path, *next_block);
    //             spdlog::info("Chain ingestion cursor initialized at block {}", *next_block);
    //         }

    //         if(*next_block > safe_head)
    //         {
    //             if(!runtime_options.skip_sleep)
    //             {
    //                 co_await _sleepFor(cfg.poll_interval_ms);
    //             }
    //             continue;
    //         }

    //         std::uint64_t to_block = safe_head;
    //         if(*next_block <= std::numeric_limits<std::uint64_t>::max() - (cfg.block_batch_size - 1))
    //         {
    //             to_block = std::min(*next_block + (cfg.block_batch_size - 1), safe_head);
    //         }

    //         const auto logs_res = _ethGetLogs(rpc_call, cfg.rpc_url, cfg.registry_address, *next_block, to_block, tracked_topics);
    //         if(!logs_res)
    //         {
    //             if(!runtime_options.skip_sleep)
    //             {
    //                 co_await _sleepFor(cfg.poll_interval_ms);
    //             }
    //             continue;
    //         }

    //         std::vector<json> logs;
    //         logs.reserve(logs_res->size());
    //         for(const auto & item : *logs_res)
    //         {
    //             logs.push_back(item);
    //         }

    //         std::ranges::sort(logs, [](const json & a, const json & b)
    //         {
    //             const std::uint64_t a_block = _parseHexQuantity(a.value("blockNumber", "0x0")).value_or(0);
    //             const std::uint64_t b_block = _parseHexQuantity(b.value("blockNumber", "0x0")).value_or(0);
    //             if(a_block != b_block)
    //             {
    //                 return a_block < b_block;
    //             }

    //             const std::uint64_t a_index = _parseHexQuantity(a.value("logIndex", "0x0")).value_or(0);
    //             const std::uint64_t b_index = _parseHexQuantity(b.value("logIndex", "0x0")).value_or(0);
    //             return a_index < b_index;
    //         });

    //         for(const json & log : logs)
    //         {
    //             if(!log.contains("topics") || !log["topics"].is_array() || log["topics"].empty() || !log.contains("data") || !log["data"].is_string())
    //             {
    //                 continue;
    //             }

    //             std::vector<std::string> topics_hex;
    //             for(const auto & topic : log["topics"])
    //             {
    //                 if(!topic.is_string())
    //                 {
    //                     continue;
    //                 }
    //                 topics_hex.push_back(topic.get<std::string>());
    //             }

    //             if(topics_hex.empty())
    //             {
    //                 continue;
    //             }

    //             const std::string topic0 = _toLower(_withHexPrefix(topics_hex.front()));
    //             const std::string data_hex = log["data"].get<std::string>();

    //             if(topic0 == feature_added_topic || topic0 == feature_added_topic_legacy)
    //             {
    //                 const auto event = _decodeSimpleAddedEvent(data_hex);
    //                 if(!event)
    //                 {
    //                     continue;
    //                 }

    //                 FeatureRecord record;
    //                 record.mutable_feature()->set_name(event->name);
    //                 record.set_owner(evmc::hex(_ethGetOwner(rpc_call, cfg.rpc_url, event->entity_address).value_or(event->caller)));

    //                 co_await _addFeatureRecord(registry, cfg.storage_path, event->entity_address, record);
    //                 continue;
    //             }

    //             if(topic0 == transformation_added_topic || topic0 == transformation_added_topic_legacy)
    //             {
    //                 const auto event = _decodeSimpleAddedEvent(data_hex);
    //                 if(!event)
    //                 {
    //                     continue;
    //                 }

    //                 TransformationRecord record;
    //                 record.mutable_transformation()->set_name(event->name);
    //                 record.mutable_transformation()->set_sol_src("");
    //                 record.set_owner(evmc::hex(_ethGetOwner(rpc_call, cfg.rpc_url, event->entity_address).value_or(event->caller)));

    //                 co_await _addTransformationRecord(registry, cfg.storage_path, event->entity_address, record);
    //                 continue;
    //             }

    //             if(topic0 == condition_added_topic || topic0 == condition_added_topic_legacy)
    //             {
    //                 const auto event = _decodeSimpleAddedEvent(data_hex);
    //                 if(!event)
    //                 {
    //                     continue;
    //                 }

    //                 ConditionRecord record;
    //                 record.mutable_condition()->set_name(event->name);
    //                 record.mutable_condition()->set_sol_src("");
    //                 record.set_owner(evmc::hex(_ethGetOwner(rpc_call, cfg.rpc_url, event->entity_address).value_or(event->caller)));

    //                 co_await _addConditionRecord(registry, cfg.storage_path, event->entity_address, record);
    //                 continue;
    //             }

    //             if(topic0 == particle_added_topic)
    //             {
    //                 const auto event = evm::decodeParticleAddedEvent(data_hex, topics_hex);
    //                 if(!event)
    //                 {
    //                     continue;
    //                 }

    //                 ParticleRecord record;
    //                 record.mutable_particle()->set_name(event->name);
    //                 record.mutable_particle()->set_feature_name(event->feature_name);
    //                 for(const std::string & composite_name : event->composite_names)
    //                 {
    //                     record.mutable_particle()->add_composite_names(composite_name);
    //                 }
    //                 record.mutable_particle()->set_condition_name(event->condition_name);
    //                 for(const std::int32_t arg : event->condition_args)
    //                 {
    //                     record.mutable_particle()->add_condition_args(arg);
    //                 }
    //                 record.set_owner(evmc::hex(event->owner));

    //                 if(!co_await _addParticleRecord(registry, cfg.storage_path, event->particle_address, record))
    //                 {
    //                     if(std::ranges::none_of(pending_particles, [&](const PendingParticle & item)
    //                         {
    //                             return _isSamePendingParticle(item, event->particle_address, event->name);
    //                         }))
    //                     {
    //                         pending_particles.push_back(PendingParticle{
    //                             .address = event->particle_address,
    //                             .record = std::move(record)
    //                         });
    //                     }
    //                 }
    //             }
    //         }

    //         if(!pending_particles.empty())
    //         {
    //             co_await _retryPendingParticles(pending_particles, registry, cfg.storage_path);
    //         }

    //         if(to_block == std::numeric_limits<std::uint64_t>::max())
    //         {
    //             break;
    //         }

    //         next_block = to_block + 1;
    //         _saveNextBlock(state_path, *next_block);

    //         if(*next_block > safe_head)
    //         {
    //             if(!runtime_options.skip_sleep)
    //             {
    //                 co_await _sleepFor(cfg.poll_interval_ms);
    //             }
    //         }
    //     }
    // }

    // asio::awaitable<void> runEventIngestion(IngestionConfig cfg, registry::Registry & registry)
    // {
    //     co_await runEventIngestion(std::move(cfg), registry, IngestionRuntimeOptions{});
    // }
}
