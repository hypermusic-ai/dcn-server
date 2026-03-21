#include "unit-tests.hpp"

#include <nlohmann/json.hpp>

#include <cstdint>
#include <initializer_list>
#include <optional>
#include <string>
#include <utility>
#include <vector>

using namespace dcn;
using namespace dcn::tests;

namespace
{
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

    server::RouteArg makeStringRouteArg(const std::string & value)
    {
        return server::RouteArg(
            server::RouteArgDef(server::RouteArgType::string, server::RouteArgRequirement::required),
            value);
    }

    server::RouteArg makeUintRouteArg(std::size_t value)
    {
        return server::RouteArg(
            server::RouteArgDef(server::RouteArgType::unsigned_integer, server::RouteArgRequirement::required),
            std::to_string(value));
    }
}

TEST_F(UnitTest, API_Format_Get_ReturnsPaginatedConnectorsForGivenHash)
{
    asio::io_context io_context;
    registry::Registry registry(io_context);

    const std::string owner_hex = evmc::hex(makeAddressFromByte(0xF0));

    ASSERT_TRUE(addScalarConnector(io_context, registry, "TIME", owner_hex, 0x11));
    ASSERT_TRUE(addScalarConnector(io_context, registry, "PITCH", owner_hex, 0x12));

    const chain::Address fmt_a_address = makeAddressFromByte(0x21);
    ConnectorRecord fmt_a = makeConnectorRecord("FMT_A", owner_hex);
    addDimension(fmt_a, "TIME");
    addDimension(fmt_a, "PITCH");
    ASSERT_TRUE(runAwaitable(io_context, registry.addConnector(fmt_a_address, std::move(fmt_a))));

    const chain::Address fmt_b_address = makeAddressFromByte(0x22);
    ConnectorRecord fmt_b = makeConnectorRecord("FMT_B", owner_hex);
    addDimension(fmt_b, "TIME");
    addDimension(fmt_b, "PITCH");
    ASSERT_TRUE(runAwaitable(io_context, registry.addConnector(fmt_b_address, std::move(fmt_b))));

    const chain::Address fmt_c_address = makeAddressFromByte(0x23);
    ConnectorRecord fmt_c = makeConnectorRecord("FMT_C", owner_hex);
    addDimension(fmt_c, "TIME");
    addDimension(fmt_c, "PITCH");
    ASSERT_TRUE(runAwaitable(io_context, registry.addConnector(fmt_c_address, std::move(fmt_c))));

    const chain::Address other_address = makeAddressFromByte(0x24);
    ConnectorRecord other = makeConnectorRecord("FMT_OTHER", owner_hex);
    addDimension(other, "TIME");
    ASSERT_TRUE(runAwaitable(io_context, registry.addConnector(other_address, std::move(other))));

    const auto fmt_a_hash = runAwaitable(io_context, registry.getFormatHash("FMT_A", fmt_a_address));
    ASSERT_TRUE(fmt_a_hash.has_value());
    const std::string format_hash_hex = evmc::hex(*fmt_a_hash);

    {
        http::Request request;
        request.setMethod(http::Method::GET)
               .setPath(http::URL("/format/" + format_hash_hex + "?limit=2&page=0"))
               .setVersion("HTTP/1.1");

        std::vector<server::RouteArg> args{makeStringRouteArg(format_hash_hex)};
        server::QueryArgsList query_args;
        query_args.emplace("limit", makeUintRouteArg(2));
        query_args.emplace("page", makeUintRouteArg(0));

        const auto response = runAwaitable(io_context, GET_format(request, std::move(args), std::move(query_args), registry));
        ASSERT_EQ(response.getCode(), http::Code::OK);

        const auto body = nlohmann::json::parse(response.getBody());
        EXPECT_EQ(body["format_hash"], format_hash_hex);
        EXPECT_EQ(body["page"], 0);
        EXPECT_EQ(body["limit"], 2);
        EXPECT_EQ(body["total_connectors"], 3);
        ASSERT_TRUE(body["scalars"].is_array());
        ASSERT_EQ(body["scalars"].size(), 2);
        EXPECT_EQ(body["scalars"][0], "PITCH:0");
        EXPECT_EQ(body["scalars"][1], "TIME:0");
        EXPECT_FALSE(body.contains("scalar_labels"));

        ASSERT_TRUE(body["connectors"].is_array());
        ASSERT_EQ(body["connectors"].size(), 2);
        EXPECT_EQ(body["connectors"][0]["name"], "FMT_A");
        EXPECT_EQ(body["connectors"][0]["local_address"], evmc::hex(fmt_a_address));
        EXPECT_EQ(body["connectors"][1]["name"], "FMT_B");
        EXPECT_EQ(body["connectors"][1]["local_address"], evmc::hex(fmt_b_address));
    }

    {
        http::Request request;
        request.setMethod(http::Method::GET)
               .setPath(http::URL("/format/" + format_hash_hex + "?limit=2&page=1"))
               .setVersion("HTTP/1.1");

        std::vector<server::RouteArg> args{makeStringRouteArg(format_hash_hex)};
        server::QueryArgsList query_args;
        query_args.emplace("limit", makeUintRouteArg(2));
        query_args.emplace("page", makeUintRouteArg(1));

        const auto response = runAwaitable(io_context, GET_format(request, std::move(args), std::move(query_args), registry));
        ASSERT_EQ(response.getCode(), http::Code::OK);

        const auto body = nlohmann::json::parse(response.getBody());
        EXPECT_EQ(body["total_connectors"], 3);
        ASSERT_EQ(body["connectors"].size(), 1);
        EXPECT_EQ(body["connectors"][0]["name"], "FMT_C");
        EXPECT_EQ(body["connectors"][0]["local_address"], evmc::hex(fmt_c_address));
    }
}

TEST_F(UnitTest, API_Format_Get_UnknownHashReturnsEmptyConnectorsList)
{
    asio::io_context io_context;
    registry::Registry registry(io_context);

    evmc::bytes32 unknown_hash{};
    const std::string format_hash_hex = evmc::hex(unknown_hash);

    http::Request request;
    request.setMethod(http::Method::GET)
           .setPath(http::URL("/format/" + format_hash_hex + "?limit=10&page=0"))
           .setVersion("HTTP/1.1");

    std::vector<server::RouteArg> args{makeStringRouteArg(format_hash_hex)};
    server::QueryArgsList query_args;
    query_args.emplace("limit", makeUintRouteArg(10));
    query_args.emplace("page", makeUintRouteArg(0));

    const auto response = runAwaitable(io_context, GET_format(request, std::move(args), std::move(query_args), registry));
    ASSERT_EQ(response.getCode(), http::Code::OK);

    const auto body = nlohmann::json::parse(response.getBody());
    EXPECT_EQ(body["format_hash"], format_hash_hex);
    EXPECT_EQ(body["page"], 0);
    EXPECT_EQ(body["limit"], 10);
    EXPECT_EQ(body["total_connectors"], 0);
    ASSERT_TRUE(body["scalars"].is_array());
    EXPECT_TRUE(body["scalars"].empty());
    EXPECT_FALSE(body.contains("scalar_labels"));
    ASSERT_TRUE(body["connectors"].is_array());
    EXPECT_TRUE(body["connectors"].empty());
}
