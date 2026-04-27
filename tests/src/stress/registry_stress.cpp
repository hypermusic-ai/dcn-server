#include "unit-tests.hpp"
#include "test_connector_helpers.hpp"

#include <algorithm>
#include <charconv>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <optional>
#include <string>
#include <vector>

#include <absl/container/flat_hash_set.h>

using namespace dcn;
using namespace dcn::tests;

namespace
{
    using dcn::tests::helpers::addDimension;
    using dcn::tests::helpers::addScalarConnector;
    using dcn::tests::helpers::makeAddressFromByte;
    using dcn::tests::helpers::makeConnectorRecord;
    using dcn::tests::helpers::runAwaitable;

    std::size_t readEnvSizeOrDefault(const char * env_name, std::size_t default_value)
    {
        const char * raw = std::getenv(env_name);
        if(raw == nullptr || raw[0] == '\0')
        {
            return default_value;
        }

        std::size_t parsed = 0;
        const std::size_t raw_size = std::strlen(raw);
        const auto [ptr, ec] = std::from_chars(raw, raw + raw_size, parsed, 10);
        if(ec != std::errc{} || ptr != (raw + raw_size) || parsed == 0)
        {
            spdlog::warn(
                "Invalid stress env {}='{}'; using default {}",
                env_name,
                raw,
                default_value);
            return default_value;
        }

        return parsed;
    }

    chain::Address makeAddressFromIndex(std::uint64_t index)
    {
        chain::Address address{};
        for(std::size_t i = 0; i < 8; ++i)
        {
            const std::size_t shift = (7 - i) * 8;
            address.bytes[12 + i] = static_cast<std::uint8_t>((index >> shift) & 0xFFu);
        }

        return address;
    }

    TransformationRecord makeTransformationRecord(const std::string & name, const std::string & owner_hex)
    {
        TransformationRecord record;
        record.mutable_transformation()->set_name(name);
        record.mutable_transformation()->set_sol_src("return x;");
        record.set_owner(owner_hex);
        return record;
    }

    ConditionRecord makeConditionRecord(const std::string & name, const std::string & owner_hex)
    {
        ConditionRecord record;
        record.mutable_condition()->set_name(name);
        record.mutable_condition()->set_sol_src("return true;");
        record.set_owner(owner_hex);
        return record;
    }
}

TEST_F(StressTest, Stress_Registry_LargeFormatBucketCursorPagination)
{
    const std::size_t connector_count = readEnvSizeOrDefault("DCN_STRESS_CONNECTOR_COUNT", 500000);

    asio::io_context io_context;
    registry::Registry registry(io_context);

    const std::string owner_hex = evmc::hex(makeAddressFromByte(0x11));
    ASSERT_TRUE(addScalarConnector(io_context, registry, "TIME", owner_hex, 0x12));
    ASSERT_TRUE(addScalarConnector(io_context, registry, "PITCH", owner_hex, 0x13));

    std::vector<std::pair<chain::Address, ConnectorRecord>> batch;
    batch.reserve(connector_count);
    for(std::size_t i = 0; i < connector_count; ++i)
    {
        ConnectorRecord record = makeConnectorRecord("STRESS_FMT_" + std::to_string(i), owner_hex);
        addDimension(record, "TIME");
        addDimension(record, "PITCH");
        batch.emplace_back(makeAddressFromIndex(10'000 + i), std::move(record));
    }

    ASSERT_TRUE(runAwaitable(io_context, registry.addConnectorsBatch(std::move(batch), true)));

    const auto first_hash = runAwaitable(io_context, registry.getFormatHash("STRESS_FMT_0"));
    ASSERT_TRUE(first_hash.has_value());

    const std::size_t total_count = runAwaitable(io_context, registry.getFormatConnectorNamesCount(*first_hash));
    EXPECT_EQ(total_count, connector_count);

    std::optional<registry::NameCursor> after;
    std::size_t seen_count = 0;
    absl::flat_hash_set<std::string> seen_names;
    seen_names.reserve(connector_count);
    std::optional<std::string> previous;
    while(true)
    {
        const auto page = runAwaitable(io_context, registry.getFormatConnectorNamesCursor(*first_hash, after, 257));
        for(const std::string & name : page.entries)
        {
            if(previous.has_value())
            {
                EXPECT_LT(*previous, name);
            }
            previous = name;
            seen_names.insert(name);
            ++seen_count;
        }

        if(!page.has_more)
        {
            EXPECT_FALSE(page.next_after.has_value());
            break;
        }

        ASSERT_TRUE(page.next_after.has_value());
        after = page.next_after;
    }

    EXPECT_EQ(seen_count, connector_count);
    EXPECT_EQ(seen_names.size(), connector_count);
}

TEST_F(StressTest, Stress_Registry_DeepConnectorDependencyChainBatch)
{
    const std::size_t chain_depth = readEnvSizeOrDefault("DCN_STRESS_CHAIN_DEPTH", 2500);

    asio::io_context io_context;
    registry::Registry registry(io_context);

    const std::string owner_hex = evmc::hex(makeAddressFromByte(0x21));
    const chain::Address base_address = makeAddressFromIndex(200'001);
    ConnectorRecord base = makeConnectorRecord("CHAIN_BASE", owner_hex);
    addDimension(base, "");
    ASSERT_TRUE(runAwaitable(io_context, registry.addConnector(base_address, std::move(base))));

    std::vector<std::pair<chain::Address, ConnectorRecord>> batch;
    batch.reserve(chain_depth);
    std::string parent_name = "CHAIN_BASE";
    for(std::size_t i = 1; i <= chain_depth; ++i)
    {
        const std::string name = "CHAIN_" + std::to_string(i);
        ConnectorRecord record = makeConnectorRecord(name, owner_hex);
        addDimension(record, parent_name);
        batch.emplace_back(makeAddressFromIndex(200'001 + i), std::move(record));
        parent_name = name;
    }

    ASSERT_TRUE(runAwaitable(io_context, registry.addConnectorsBatch(std::move(batch), true)));

    const std::string last_name = "CHAIN_" + std::to_string(chain_depth);
    const auto last_connector = runAwaitable(io_context, registry.getConnectorRecordHandle(last_name));
    ASSERT_TRUE(last_connector.has_value());

    const auto base_hash = runAwaitable(io_context, registry.getFormatHash("CHAIN_BASE"));
    const auto last_hash = runAwaitable(io_context, registry.getFormatHash(last_name));
    ASSERT_TRUE(base_hash.has_value());
    ASSERT_TRUE(last_hash.has_value());
    EXPECT_TRUE(chain::equalBytes32(*base_hash, *last_hash));
}

TEST_F(StressTest, Stress_Registry_LargeOwnerCursorForTransformationsAndConditions)
{
    const std::size_t transformations_count =
        readEnvSizeOrDefault("DCN_STRESS_TRANSFORMATIONS_COUNT", 4000);
    const std::size_t conditions_count =
        readEnvSizeOrDefault("DCN_STRESS_CONDITIONS_COUNT", 4000);

    asio::io_context io_context;
    registry::Registry registry(io_context);

    const std::string owner_hex = evmc::hex(makeAddressFromByte(0x31));

    std::vector<std::pair<chain::Address, TransformationRecord>> transformations_batch;
    transformations_batch.reserve(transformations_count);
    for(std::size_t i = 0; i < transformations_count; ++i)
    {
        transformations_batch.emplace_back(
            makeAddressFromIndex(300'000 + i),
            makeTransformationRecord("TX_STRESS_" + std::to_string(i), owner_hex));
    }
    ASSERT_TRUE(runAwaitable(io_context, registry.addTransformationsBatch(std::move(transformations_batch), true)));

    std::vector<std::pair<chain::Address, ConditionRecord>> conditions_batch;
    conditions_batch.reserve(conditions_count);
    for(std::size_t i = 0; i < conditions_count; ++i)
    {
        conditions_batch.emplace_back(
            makeAddressFromIndex(400'000 + i),
            makeConditionRecord("COND_STRESS_" + std::to_string(i), owner_hex));
    }
    ASSERT_TRUE(runAwaitable(io_context, registry.addConditionsBatch(std::move(conditions_batch), true)));

    const auto owner_opt = evmc::from_hex<chain::Address>(owner_hex);
    ASSERT_TRUE(owner_opt.has_value());
    const chain::Address owner = *owner_opt;

    std::optional<registry::NameCursor> tx_after;
    std::size_t tx_seen = 0;
    while(true)
    {
        const auto page = runAwaitable(io_context, registry.getOwnedTransformationsCursor(owner, tx_after, 193));
        tx_seen += page.entries.size();
        if(!page.has_more)
        {
            break;
        }
        ASSERT_TRUE(page.next_after.has_value());
        tx_after = page.next_after;
    }
    EXPECT_EQ(tx_seen, transformations_count);

    std::optional<registry::NameCursor> cond_after;
    std::size_t cond_seen = 0;
    while(true)
    {
        const auto page = runAwaitable(io_context, registry.getOwnedConditionsCursor(owner, cond_after, 193));
        cond_seen += page.entries.size();
        if(!page.has_more)
        {
            break;
        }
        ASSERT_TRUE(page.next_after.has_value());
        cond_after = page.next_after;
    }
    EXPECT_EQ(cond_seen, conditions_count);
}

TEST_F(StressTest, DISABLED_Stress_Registry_MillionConnectorsSingleFormat)
{
    const std::size_t connector_count =
        readEnvSizeOrDefault("DCN_STRESS_HEAVY_CONNECTOR_COUNT", 1'000'000);

    asio::io_context io_context;
    registry::Registry registry(io_context);

    const std::string owner_hex = evmc::hex(makeAddressFromByte(0x41));
    ASSERT_TRUE(addScalarConnector(io_context, registry, "TIME", owner_hex, 0x42));
    ASSERT_TRUE(addScalarConnector(io_context, registry, "PITCH", owner_hex, 0x43));

    std::vector<std::pair<chain::Address, ConnectorRecord>> batch;
    batch.reserve(connector_count);
    for(std::size_t i = 0; i < connector_count; ++i)
    {
        ConnectorRecord record = makeConnectorRecord("HEAVY_FMT_" + std::to_string(i), owner_hex);
        addDimension(record, "TIME");
        addDimension(record, "PITCH");
        batch.emplace_back(makeAddressFromIndex(500'000 + i), std::move(record));
    }

    ASSERT_TRUE(runAwaitable(io_context, registry.addConnectorsBatch(std::move(batch), true)));

    const auto hash_opt = runAwaitable(io_context, registry.getFormatHash("HEAVY_FMT_0"));
    ASSERT_TRUE(hash_opt.has_value());

    const std::size_t count = runAwaitable(io_context, registry.getFormatConnectorNamesCount(*hash_opt));
    EXPECT_EQ(count, connector_count);
}

TEST_F(StressTest, DISABLED_Stress_Registry_DeepConnectorDependencyChain10k)
{
    const std::size_t chain_depth =
        readEnvSizeOrDefault("DCN_STRESS_HEAVY_CHAIN_DEPTH", 10'000);

    asio::io_context io_context;
    registry::Registry registry(io_context);

    const std::string owner_hex = evmc::hex(makeAddressFromByte(0x51));
    const chain::Address base_address = makeAddressFromIndex(700'000);
    ConnectorRecord base = makeConnectorRecord("HEAVY_CHAIN_BASE", owner_hex);
    addDimension(base, "");
    ASSERT_TRUE(runAwaitable(io_context, registry.addConnector(base_address, std::move(base))));

    std::vector<std::pair<chain::Address, ConnectorRecord>> batch;
    batch.reserve(chain_depth);
    std::string parent_name = "HEAVY_CHAIN_BASE";
    for(std::size_t i = 1; i <= chain_depth; ++i)
    {
        const std::string name = "HEAVY_CHAIN_" + std::to_string(i);
        ConnectorRecord record = makeConnectorRecord(name, owner_hex);
        addDimension(record, parent_name);
        batch.emplace_back(makeAddressFromIndex(700'000 + i), std::move(record));
        parent_name = name;
    }

    ASSERT_TRUE(runAwaitable(io_context, registry.addConnectorsBatch(std::move(batch), true)));
    const auto record_handle = runAwaitable(
        io_context,
        registry.getConnectorRecordHandle("HEAVY_CHAIN_" + std::to_string(chain_depth)));
    ASSERT_TRUE(record_handle.has_value());
}

TEST_F(StressTest, DISABLED_Stress_Registry_MillionOwnerEntriesForTransformationsAndConditions)
{
    const std::size_t transformations_count =
        readEnvSizeOrDefault("DCN_STRESS_HEAVY_TRANSFORMATIONS_COUNT", 1'000'000);
    const std::size_t conditions_count =
        readEnvSizeOrDefault("DCN_STRESS_HEAVY_CONDITIONS_COUNT", 1'000'000);

    asio::io_context io_context;
    registry::Registry registry(io_context);

    const std::string owner_hex = evmc::hex(makeAddressFromByte(0x61));
    const auto owner_opt = evmc::from_hex<chain::Address>(owner_hex);
    ASSERT_TRUE(owner_opt.has_value());
    const chain::Address owner = *owner_opt;

    std::vector<std::pair<chain::Address, TransformationRecord>> tx_batch;
    tx_batch.reserve(transformations_count);
    for(std::size_t i = 0; i < transformations_count; ++i)
    {
        tx_batch.emplace_back(
            makeAddressFromIndex(800'000 + i),
            makeTransformationRecord("HEAVY_TX_" + std::to_string(i), owner_hex));
    }
    ASSERT_TRUE(runAwaitable(io_context, registry.addTransformationsBatch(std::move(tx_batch), true)));

    std::vector<std::pair<chain::Address, ConditionRecord>> cond_batch;
    cond_batch.reserve(conditions_count);
    for(std::size_t i = 0; i < conditions_count; ++i)
    {
        cond_batch.emplace_back(
            makeAddressFromIndex(1'800'000 + i),
            makeConditionRecord("HEAVY_COND_" + std::to_string(i), owner_hex));
    }
    ASSERT_TRUE(runAwaitable(io_context, registry.addConditionsBatch(std::move(cond_batch), true)));

    std::optional<registry::NameCursor> tx_after;
    std::size_t tx_seen = 0;
    while(true)
    {
        const auto page = runAwaitable(io_context, registry.getOwnedTransformationsCursor(owner, tx_after, 2048));
        tx_seen += page.entries.size();
        if(!page.has_more)
        {
            break;
        }
        ASSERT_TRUE(page.next_after.has_value());
        tx_after = page.next_after;
    }
    EXPECT_EQ(tx_seen, transformations_count);

    std::optional<registry::NameCursor> cond_after;
    std::size_t cond_seen = 0;
    while(true)
    {
        const auto page = runAwaitable(io_context, registry.getOwnedConditionsCursor(owner, cond_after, 2048));
        cond_seen += page.entries.size();
        if(!page.has_more)
        {
            break;
        }
        ASSERT_TRUE(page.next_after.has_value());
        cond_after = page.next_after;
    }
    EXPECT_EQ(cond_seen, conditions_count);
}

