#include "unit-tests.hpp"
#include "test_connector_helpers.hpp"

#include <algorithm>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

using namespace dcn;
using namespace dcn::tests;

namespace
{
    using dcn::tests::helpers::addDimension;
    using dcn::tests::helpers::addScalarConnector;
    using dcn::tests::helpers::makeAddressFromByte;
    using dcn::tests::helpers::makeConnectorRecord;
    using dcn::tests::helpers::runAwaitable;

    evmc::bytes32 pathHash(std::initializer_list<std::uint32_t> dim_ids)
    {
        auto it = dim_ids.begin();
        EXPECT_TRUE(it != dim_ids.end());
        if(it == dim_ids.end())
        {
            return {};
        }

        evmc::bytes32 out = chain::dimPathHash(*it);
        ++it;
        for(; it != dim_ids.end(); ++it)
        {
            out = chain::concatPathHash(out, chain::dimPathHash(*it));
        }

        return out;
    }

    evmc::bytes32 expectedFormatHash(std::initializer_list<std::pair<std::string_view, evmc::bytes32>> labels)
    {
        std::vector<chain::ScalarHashEntry> hash_entries;
        hash_entries.reserve(labels.size());
        for(const auto & [scalar_name, path_hash] : labels)
        {
            hash_entries.push_back(chain::ScalarHashEntry{
                .scalar_hash = chain::keccakString(scalar_name),
                .path_hash = path_hash
            });
        }

        return chain::computeFormatHash(hash_entries);
    }

    bool containsScalarLabel(
        const std::vector<registry::ScalarLabel> & labels,
        std::string_view scalar,
        const evmc::bytes32 & path_hash)
    {
        for(const registry::ScalarLabel & label : labels)
        {
            if(label.scalar == scalar && chain::equalBytes32(label.path_hash, path_hash))
            {
                return true;
            }
        }

        return false;
    }

    std::vector<std::string> scalarNamesFromLabels(const std::vector<registry::ScalarLabel> & labels)
    {
        std::vector<std::string> names;
        names.reserve(labels.size());
        for(const registry::ScalarLabel & label : labels)
        {
            names.push_back(label.scalar);
        }

        std::sort(names.begin(), names.end());
        return names;
    }

    std::vector<chain::Address> getAllFormatConnectors(
        asio::io_context & io_context,
        registry::Registry & registry,
        const evmc::bytes32 & format_hash)
    {
        const std::size_t connectors_count =
            runAwaitable(io_context, registry.getFormatConnectorsCount(format_hash));
        return runAwaitable(
            io_context,
            registry.getFormatConnectorsPage(format_hash, 0, connectors_count));
    }

    bool containsAddress(
        const std::vector<chain::Address> & addresses,
        const chain::Address & address)
    {
        return std::find(addresses.begin(), addresses.end(), address) != addresses.end();
    }
}

TEST_F(UnitTest, Registry_FormatHash_IsOrderIndependentAcrossDimensionOrder)
{
    asio::io_context io_context;
    registry::Registry registry(io_context);

    const std::string owner_hex = evmc::hex(makeAddressFromByte(0xF0));

    ASSERT_TRUE(addScalarConnector(io_context, registry, "TIME", owner_hex, 0x11));
    ASSERT_TRUE(addScalarConnector(io_context, registry, "PITCH", owner_hex, 0x12));

    const chain::Address left_address = makeAddressFromByte(0x21);
    ConnectorRecord left = makeConnectorRecord("LEFT", owner_hex);
    addDimension(left, "TIME");
    addDimension(left, "PITCH");
    ASSERT_TRUE(runAwaitable(io_context, registry.addConnector(left_address, std::move(left))));

    const chain::Address right_address = makeAddressFromByte(0x22);
    ConnectorRecord right = makeConnectorRecord("RIGHT", owner_hex);
    addDimension(right, "PITCH");
    addDimension(right, "TIME");
    ASSERT_TRUE(runAwaitable(io_context, registry.addConnector(right_address, std::move(right))));

    const auto left_hash = runAwaitable(io_context, registry.getFormatHash("LEFT", left_address));
    const auto right_hash = runAwaitable(io_context, registry.getFormatHash("RIGHT", right_address));
    ASSERT_TRUE(left_hash.has_value());
    ASSERT_TRUE(right_hash.has_value());

    EXPECT_TRUE(chain::equalBytes32(*left_hash, *right_hash));

    const evmc::bytes32 expected_left = expectedFormatHash({
        {"TIME", pathHash({0})},
        {"PITCH", pathHash({0})}
    });
    const evmc::bytes32 expected_right = expectedFormatHash({
        {"PITCH", pathHash({0})},
        {"TIME", pathHash({0})}
    });

    EXPECT_TRUE(chain::equalBytes32(*left_hash, expected_left));
    EXPECT_TRUE(chain::equalBytes32(*right_hash, expected_right));

    const auto left_connectors = getAllFormatConnectors(io_context, registry, *left_hash);
    EXPECT_TRUE(containsAddress(left_connectors, left_address));
    EXPECT_TRUE(containsAddress(left_connectors, right_address));
}

TEST_F(UnitTest, Registry_FormatHash_IsPathSensitiveAcrossBindingSlots)
{
    asio::io_context io_context;
    registry::Registry registry(io_context);

    const std::string owner_hex = evmc::hex(makeAddressFromByte(0xE0));

    ASSERT_TRUE(addScalarConnector(io_context, registry, "PITCH", owner_hex, 0x31));
    ASSERT_TRUE(addScalarConnector(io_context, registry, "BASE2", owner_hex, 0x32, 2));

    const chain::Address slot0_address = makeAddressFromByte(0x41);
    ConnectorRecord slot0 = makeConnectorRecord("BIND_SLOT0", owner_hex);
    addDimension(slot0, "BASE2", {{"0", "PITCH"}});
    ASSERT_TRUE(runAwaitable(io_context, registry.addConnector(slot0_address, std::move(slot0))));

    const chain::Address slot1_address = makeAddressFromByte(0x42);
    ConnectorRecord slot1 = makeConnectorRecord("BIND_SLOT1", owner_hex);
    addDimension(slot1, "BASE2", {{"1", "PITCH"}});
    ASSERT_TRUE(runAwaitable(io_context, registry.addConnector(slot1_address, std::move(slot1))));

    const auto slot0_hash = runAwaitable(io_context, registry.getFormatHash("BIND_SLOT0", slot0_address));
    const auto slot1_hash = runAwaitable(io_context, registry.getFormatHash("BIND_SLOT1", slot1_address));
    ASSERT_TRUE(slot0_hash.has_value());
    ASSERT_TRUE(slot1_hash.has_value());

    EXPECT_FALSE(chain::equalBytes32(*slot0_hash, *slot1_hash));

    const auto slot0_labels = runAwaitable(io_context, registry.getScalarLabelsByFormatHash(*slot0_hash));
    const auto slot1_labels = runAwaitable(io_context, registry.getScalarLabelsByFormatHash(*slot1_hash));
    ASSERT_TRUE(slot0_labels.has_value());
    ASSERT_TRUE(slot1_labels.has_value());
    EXPECT_EQ(scalarNamesFromLabels(*slot0_labels), scalarNamesFromLabels(*slot1_labels));

    EXPECT_TRUE(containsScalarLabel(*slot0_labels, "PITCH", pathHash({0})));
    EXPECT_TRUE(containsScalarLabel(*slot0_labels, "BASE2", pathHash({1})));

    EXPECT_TRUE(containsScalarLabel(*slot1_labels, "BASE2", pathHash({0})));
    EXPECT_TRUE(containsScalarLabel(*slot1_labels, "PITCH", pathHash({0})));
}

TEST_F(UnitTest, Registry_FormatHash_MatchesForSeparateConnectorsWithSameProducedLabels)
{
    asio::io_context io_context;
    registry::Registry registry(io_context);

    const std::string owner_hex = evmc::hex(makeAddressFromByte(0xD0));

    ASSERT_TRUE(addScalarConnector(io_context, registry, "TIME", owner_hex, 0x51));
    ASSERT_TRUE(addScalarConnector(io_context, registry, "PITCH", owner_hex, 0x52));

    const chain::Address first_address = makeAddressFromByte(0x61);
    ConnectorRecord first = makeConnectorRecord("SAME_A", owner_hex);
    addDimension(first, "TIME");
    addDimension(first, "PITCH");
    ASSERT_TRUE(runAwaitable(io_context, registry.addConnector(first_address, std::move(first))));

    const chain::Address second_address = makeAddressFromByte(0x62);
    ConnectorRecord second = makeConnectorRecord("SAME_B", owner_hex);
    addDimension(second, "TIME");
    addDimension(second, "PITCH");
    ASSERT_TRUE(runAwaitable(io_context, registry.addConnector(second_address, std::move(second))));

    const auto first_hash = runAwaitable(io_context, registry.getFormatHash("SAME_A", first_address));
    const auto second_hash = runAwaitable(io_context, registry.getFormatHash("SAME_B", second_address));
    ASSERT_TRUE(first_hash.has_value());
    ASSERT_TRUE(second_hash.has_value());

    EXPECT_TRUE(chain::equalBytes32(*first_hash, *second_hash));

    const auto connectors = getAllFormatConnectors(io_context, registry, *first_hash);
    EXPECT_EQ(connectors.size(), 2u);
    EXPECT_TRUE(containsAddress(connectors, first_address));
    EXPECT_TRUE(containsAddress(connectors, second_address));
}

TEST_F(UnitTest, Registry_GetNewestFormatHash_TracksNewestConnectorAddress)
{
    asio::io_context io_context;
    registry::Registry registry(io_context);

    const std::string owner_hex = evmc::hex(makeAddressFromByte(0xC0));

    ASSERT_TRUE(addScalarConnector(io_context, registry, "TIME", owner_hex, 0x71));
    ASSERT_TRUE(addScalarConnector(io_context, registry, "PITCH", owner_hex, 0x72));

    const chain::Address first_address = makeAddressFromByte(0x73);
    ConnectorRecord first = makeConnectorRecord("DUP", owner_hex);
    addDimension(first, "TIME");
    ASSERT_TRUE(runAwaitable(io_context, registry.addConnector(first_address, std::move(first))));

    const chain::Address second_address = makeAddressFromByte(0x74);
    ConnectorRecord second = makeConnectorRecord("DUP", owner_hex);
    addDimension(second, "PITCH");
    ASSERT_TRUE(runAwaitable(io_context, registry.addConnector(second_address, std::move(second))));

    const auto first_hash = runAwaitable(io_context, registry.getFormatHash("DUP", first_address));
    const auto second_hash = runAwaitable(io_context, registry.getFormatHash("DUP", second_address));
    const auto newest_hash = runAwaitable(io_context, registry.getNewestFormatHash("DUP"));

    ASSERT_TRUE(first_hash.has_value());
    ASSERT_TRUE(second_hash.has_value());
    ASSERT_TRUE(newest_hash.has_value());

    EXPECT_TRUE(chain::equalBytes32(*newest_hash, *second_hash));
    EXPECT_FALSE(chain::equalBytes32(*newest_hash, *first_hash));
}

TEST_F(UnitTest, Registry_AddConnector_RejectsAddressReuseAcrossDifferentNames)
{
    asio::io_context io_context;
    registry::Registry registry(io_context);

    const std::string owner_hex = evmc::hex(makeAddressFromByte(0xC1));

    const chain::Address reused_address = makeAddressFromByte(0xC2);
    ConnectorRecord first = makeConnectorRecord("ADDR_A", owner_hex);
    addDimension(first, "");
    ASSERT_TRUE(runAwaitable(io_context, registry.addConnector(reused_address, std::move(first))));

    ConnectorRecord second = makeConnectorRecord("ADDR_B", owner_hex);
    addDimension(second, "");
    EXPECT_FALSE(runAwaitable(io_context, registry.addConnector(reused_address, std::move(second))));

    const auto first_hash = runAwaitable(io_context, registry.getFormatHash("ADDR_A", reused_address));
    const auto second_hash = runAwaitable(io_context, registry.getFormatHash("ADDR_B", reused_address));
    const auto second_newest_hash = runAwaitable(io_context, registry.getNewestFormatHash("ADDR_B"));
    const auto connector_name = runAwaitable(io_context, registry.getConnectorName(reused_address));

    EXPECT_TRUE(first_hash.has_value());
    EXPECT_FALSE(second_hash.has_value());
    EXPECT_FALSE(second_newest_hash.has_value());
    ASSERT_TRUE(connector_name.has_value());
    EXPECT_EQ(*connector_name, "ADDR_A");
}

TEST_F(UnitTest, Registry_FormatHash_PreservesScalarMultiplicity)
{
    asio::io_context io_context;
    registry::Registry registry(io_context);

    const std::string owner_hex = evmc::hex(makeAddressFromByte(0xB0));

    ASSERT_TRUE(addScalarConnector(io_context, registry, "PITCH", owner_hex, 0x81));

    const chain::Address one_address = makeAddressFromByte(0x82);
    ConnectorRecord one = makeConnectorRecord("ONE_PITCH", owner_hex);
    addDimension(one, "PITCH");
    ASSERT_TRUE(runAwaitable(io_context, registry.addConnector(one_address, std::move(one))));

    const chain::Address two_address = makeAddressFromByte(0x83);
    ConnectorRecord two = makeConnectorRecord("TWO_PITCH", owner_hex);
    addDimension(two, "PITCH");
    addDimension(two, "PITCH");
    ASSERT_TRUE(runAwaitable(io_context, registry.addConnector(two_address, std::move(two))));

    const auto one_hash = runAwaitable(io_context, registry.getFormatHash("ONE_PITCH", one_address));
    const auto two_hash = runAwaitable(io_context, registry.getFormatHash("TWO_PITCH", two_address));
    ASSERT_TRUE(one_hash.has_value());
    ASSERT_TRUE(two_hash.has_value());

    EXPECT_FALSE(chain::equalBytes32(*one_hash, *two_hash));

    const evmc::bytes32 expected_one = expectedFormatHash({
        {"PITCH", pathHash({0})}
    });
    const evmc::bytes32 expected_two = expectedFormatHash({
        {"PITCH", pathHash({0})},
        {"PITCH", pathHash({0})}
    });
    EXPECT_TRUE(chain::equalBytes32(*one_hash, expected_one));
    EXPECT_TRUE(chain::equalBytes32(*two_hash, expected_two));
}

TEST_F(UnitTest, Registry_FormatHash_MatchesForSameScalarNamesWhenTailLabelsMatch)
{
    asio::io_context io_context;
    registry::Registry registry(io_context);

    const std::string owner_hex = evmc::hex(makeAddressFromByte(0xA0));

    ASSERT_TRUE(addScalarConnector(io_context, registry, "TIME", owner_hex, 0x91));
    ASSERT_TRUE(addScalarConnector(io_context, registry, "PITCH", owner_hex, 0x92));
    ASSERT_TRUE(addScalarConnector(io_context, registry, "BASE2", owner_hex, 0x93, 2));

    const chain::Address direct_address = makeAddressFromByte(0x94);
    ConnectorRecord direct = makeConnectorRecord("DIRECT", owner_hex);
    addDimension(direct, "TIME");
    addDimension(direct, "PITCH");
    ASSERT_TRUE(runAwaitable(io_context, registry.addConnector(direct_address, std::move(direct))));

    const chain::Address bound_address = makeAddressFromByte(0x95);
    ConnectorRecord bound = makeConnectorRecord("BOUND", owner_hex);
    addDimension(bound, "BASE2", {{"0", "TIME"}, {"1", "PITCH"}});
    ASSERT_TRUE(runAwaitable(io_context, registry.addConnector(bound_address, std::move(bound))));

    const auto direct_hash = runAwaitable(io_context, registry.getFormatHash("DIRECT", direct_address));
    const auto bound_hash = runAwaitable(io_context, registry.getFormatHash("BOUND", bound_address));
    ASSERT_TRUE(direct_hash.has_value());
    ASSERT_TRUE(bound_hash.has_value());

    EXPECT_TRUE(chain::equalBytes32(*direct_hash, *bound_hash));

    const auto direct_labels = runAwaitable(io_context, registry.getScalarLabelsByFormatHash(*direct_hash));
    const auto bound_labels = runAwaitable(io_context, registry.getScalarLabelsByFormatHash(*bound_hash));
    ASSERT_TRUE(direct_labels.has_value());
    ASSERT_TRUE(bound_labels.has_value());
    EXPECT_EQ(scalarNamesFromLabels(*direct_labels), scalarNamesFromLabels(*bound_labels));

    const auto format_connectors = getAllFormatConnectors(io_context, registry, *direct_hash);
    EXPECT_TRUE(containsAddress(format_connectors, direct_address));
    EXPECT_TRUE(containsAddress(format_connectors, bound_address));
}
