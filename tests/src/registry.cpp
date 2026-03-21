#include "unit-tests.hpp"

#include <algorithm>
#include <array>
#include <cstdint>
#include <cstring>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

using namespace dcn;
using namespace dcn::tests;

namespace
{
    constexpr std::uint8_t PATH_DIM_DOMAIN = 0x10;
    constexpr std::uint8_t PATH_CONCAT_DOMAIN = 0x11;
    constexpr std::uint8_t SCALAR_PATH_LABEL_DOMAIN = 0x12;

    template<class AwaitableT>
    auto runAwaitable(asio::io_context & io_context, AwaitableT awaitable)
    {
        auto future = asio::co_spawn(io_context, std::move(awaitable), asio::use_future);
        io_context.restart();
        io_context.run();
        return future.get();
    }

    chain::Address makeAddressFromByte(std::uint8_t value)
    {
        chain::Address address{};
        address.bytes[19] = value;
        return address;
    }

    ConnectorRecord makeConnectorRecord(const std::string & name, const std::string & owner_hex)
    {
        ConnectorRecord record;
        record.mutable_connector()->set_name(name);
        record.set_owner(owner_hex);
        return record;
    }

    void addDimension(
        ConnectorRecord & record,
        const std::string & composite,
        std::initializer_list<std::pair<std::string, std::string>> bindings = {})
    {
        auto * dimension = record.mutable_connector()->add_dimensions();
        dimension->set_composite(composite);

        for(const auto & [slot, binding_target] : bindings)
        {
            (*dimension->mutable_bindings())[slot] = binding_target;
        }
    }

    bool addConnectorRecord(
        asio::io_context & io_context,
        registry::Registry & registry,
        std::uint8_t address_byte,
        ConnectorRecord record)
    {
        return runAwaitable(
            io_context,
            registry.addConnector(makeAddressFromByte(address_byte), std::move(record)));
    }

    bool addScalarConnector(
        asio::io_context & io_context,
        registry::Registry & registry,
        const std::string & name,
        const std::string & owner_hex,
        std::uint8_t address_byte,
        std::uint32_t dimensions_count = 1)
    {
        ConnectorRecord record = makeConnectorRecord(name, owner_hex);
        for(std::uint32_t i = 0; i < dimensions_count; ++i)
        {
            addDimension(record, "");
        }

        return addConnectorRecord(io_context, registry, address_byte, std::move(record));
    }

    bool equalBytes32(const evmc::bytes32 & lhs, const evmc::bytes32 & rhs)
    {
        return std::memcmp(lhs.bytes, rhs.bytes, sizeof(lhs.bytes)) == 0;
    }

    std::uint64_t readUint64BE(const std::uint8_t * ptr)
    {
        std::uint64_t out = 0;
        for(std::size_t i = 0; i < sizeof(std::uint64_t); ++i)
        {
            out = (out << 8) | static_cast<std::uint64_t>(ptr[i]);
        }
        return out;
    }

    void writeUint64BE(std::uint8_t * ptr, std::uint64_t value)
    {
        for(std::size_t i = 0; i < sizeof(std::uint64_t); ++i)
        {
            const std::size_t shift = (sizeof(std::uint64_t) - 1 - i) * 8;
            ptr[i] = static_cast<std::uint8_t>((value >> shift) & 0xFFu);
        }
    }

    void writeUint32BE(std::uint8_t * ptr, std::uint32_t value)
    {
        for(std::size_t i = 0; i < sizeof(std::uint32_t); ++i)
        {
            const std::size_t shift = (sizeof(std::uint32_t) - 1 - i) * 8;
            ptr[i] = static_cast<std::uint8_t>((value >> shift) & 0xFFu);
        }
    }

    evmc::bytes32 keccakBytes(const std::uint8_t * data, std::size_t size)
    {
        evmc::bytes32 hash{};
        crypto::Keccak256::getHash(data, size, hash.bytes);
        return hash;
    }

    evmc::bytes32 composeFormatHash(const evmc::bytes32 & lhs, const evmc::bytes32 & rhs)
    {
        const std::uint64_t lhs0 = readUint64BE(lhs.bytes + 24);
        const std::uint64_t lhs1 = readUint64BE(lhs.bytes + 16);
        const std::uint64_t lhs2 = readUint64BE(lhs.bytes + 8);
        const std::uint64_t lhs3 = readUint64BE(lhs.bytes + 0);

        const std::uint64_t rhs0 = readUint64BE(rhs.bytes + 24);
        const std::uint64_t rhs1 = readUint64BE(rhs.bytes + 16);
        const std::uint64_t rhs2 = readUint64BE(rhs.bytes + 8);
        const std::uint64_t rhs3 = readUint64BE(rhs.bytes + 0);

        evmc::bytes32 out{};
        writeUint64BE(out.bytes + 24, lhs0 + rhs0);
        writeUint64BE(out.bytes + 16, lhs1 + rhs1);
        writeUint64BE(out.bytes + 8, lhs2 + rhs2);
        writeUint64BE(out.bytes + 0, lhs3 + rhs3);
        return out;
    }

    evmc::bytes32 dimPathHash(std::uint32_t dim_id)
    {
        std::array<std::uint8_t, 5> input{};
        input[0] = PATH_DIM_DOMAIN;
        writeUint32BE(input.data() + 1, dim_id);
        return keccakBytes(input.data(), input.size());
    }

    evmc::bytes32 concatPathHash(const evmc::bytes32 & left, const evmc::bytes32 & right)
    {
        std::array<std::uint8_t, 65> input{};
        input[0] = PATH_CONCAT_DOMAIN;
        std::memcpy(input.data() + 1, left.bytes, sizeof(left.bytes));
        std::memcpy(input.data() + 33, right.bytes, sizeof(right.bytes));
        return keccakBytes(input.data(), input.size());
    }

    evmc::bytes32 scalarPathLabelHash(const evmc::bytes32 & scalar_hash, const evmc::bytes32 & path_hash)
    {
        std::array<std::uint8_t, 65> input{};
        input[0] = SCALAR_PATH_LABEL_DOMAIN;
        std::memcpy(input.data() + 1, scalar_hash.bytes, sizeof(scalar_hash.bytes));
        std::memcpy(input.data() + 33, path_hash.bytes, sizeof(path_hash.bytes));
        return keccakBytes(input.data(), input.size());
    }

    evmc::bytes32 labelHashToFormatHash(const evmc::bytes32 & label_hash)
    {
        std::array<std::uint8_t, 33> input{};
        std::memcpy(input.data() + 1, label_hash.bytes, sizeof(label_hash.bytes));

        std::uint64_t lanes[4]{};
        for(std::uint8_t domain = 0; domain < 4; ++domain)
        {
            input[0] = domain;
            const evmc::bytes32 lane_hash = keccakBytes(input.data(), input.size());
            lanes[domain] = readUint64BE(lane_hash.bytes + 24);
        }

        evmc::bytes32 out{};
        writeUint64BE(out.bytes + 24, lanes[0]);
        writeUint64BE(out.bytes + 16, lanes[1]);
        writeUint64BE(out.bytes + 8, lanes[2]);
        writeUint64BE(out.bytes + 0, lanes[3]);
        return out;
    }

    evmc::bytes32 pathHash(std::initializer_list<std::uint32_t> dim_ids)
    {
        auto it = dim_ids.begin();
        EXPECT_TRUE(it != dim_ids.end());
        if(it == dim_ids.end())
        {
            return {};
        }

        evmc::bytes32 out = dimPathHash(*it);
        ++it;
        for(; it != dim_ids.end(); ++it)
        {
            out = concatPathHash(out, dimPathHash(*it));
        }

        return out;
    }

    evmc::bytes32 expectedFormatHash(std::initializer_list<std::pair<std::string_view, evmc::bytes32>> labels)
    {
        evmc::bytes32 format_hash{};
        for(const auto & [scalar_name, path_hash] : labels)
        {
            const evmc::bytes32 scalar_hash = keccakBytes(
                reinterpret_cast<const std::uint8_t *>(scalar_name.data()),
                scalar_name.size());
            const evmc::bytes32 label_hash = scalarPathLabelHash(scalar_hash, path_hash);
            format_hash = composeFormatHash(format_hash, labelHashToFormatHash(label_hash));
        }

        return format_hash;
    }

    bool containsScalarLabel(
        const std::vector<registry::ScalarLabel> & labels,
        std::string_view scalar,
        const evmc::bytes32 & path_hash)
    {
        for(const registry::ScalarLabel & label : labels)
        {
            if(label.scalar == scalar && equalBytes32(label.path_hash, path_hash))
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

    EXPECT_TRUE(equalBytes32(*left_hash, *right_hash));

    const evmc::bytes32 expected_left = expectedFormatHash({
        {"TIME", pathHash({0})},
        {"PITCH", pathHash({0})}
    });
    const evmc::bytes32 expected_right = expectedFormatHash({
        {"PITCH", pathHash({0})},
        {"TIME", pathHash({0})}
    });

    EXPECT_TRUE(equalBytes32(*left_hash, expected_left));
    EXPECT_TRUE(equalBytes32(*right_hash, expected_right));

    const auto left_connectors = runAwaitable(io_context, registry.getConnectorsByFormatHash(*left_hash));
    EXPECT_TRUE(left_connectors.contains(left_address));
    EXPECT_TRUE(left_connectors.contains(right_address));
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

    EXPECT_FALSE(equalBytes32(*slot0_hash, *slot1_hash));

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

    EXPECT_TRUE(equalBytes32(*first_hash, *second_hash));

    const auto connectors = runAwaitable(io_context, registry.getConnectorsByFormatHash(*first_hash));
    EXPECT_EQ(connectors.size(), 2u);
    EXPECT_TRUE(connectors.contains(first_address));
    EXPECT_TRUE(connectors.contains(second_address));
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

    EXPECT_TRUE(equalBytes32(*newest_hash, *second_hash));
    EXPECT_FALSE(equalBytes32(*newest_hash, *first_hash));
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

    EXPECT_FALSE(equalBytes32(*one_hash, *two_hash));

    const evmc::bytes32 expected_one = expectedFormatHash({
        {"PITCH", pathHash({0})}
    });
    const evmc::bytes32 expected_two = expectedFormatHash({
        {"PITCH", pathHash({0})},
        {"PITCH", pathHash({0})}
    });
    EXPECT_TRUE(equalBytes32(*one_hash, expected_one));
    EXPECT_TRUE(equalBytes32(*two_hash, expected_two));
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

    EXPECT_TRUE(equalBytes32(*direct_hash, *bound_hash));

    const auto direct_labels = runAwaitable(io_context, registry.getScalarLabelsByFormatHash(*direct_hash));
    const auto bound_labels = runAwaitable(io_context, registry.getScalarLabelsByFormatHash(*bound_hash));
    ASSERT_TRUE(direct_labels.has_value());
    ASSERT_TRUE(bound_labels.has_value());
    EXPECT_EQ(scalarNamesFromLabels(*direct_labels), scalarNamesFromLabels(*bound_labels));

    const auto format_connectors = runAwaitable(io_context, registry.getConnectorsByFormatHash(*direct_hash));
    EXPECT_TRUE(format_connectors.contains(direct_address));
    EXPECT_TRUE(format_connectors.contains(bound_address));
}
