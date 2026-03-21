#include "api.hpp"

#include <algorithm>
#include <cstring>
#include <limits>
#include <vector>

namespace dcn
{
    asio::awaitable<http::Response> OPTIONS_format(const http::Request & request, std::vector<server::RouteArg>, server::QueryArgsList)
    {
        http::Response response;
        response.setCode(http::Code::NoContent)
                .setVersion("HTTP/1.1")
                .setHeader(http::Header::AccessControlAllowOrigin, "*")
                .setHeader(http::Header::AccessControlAllowMethods, "GET, OPTIONS")
                .setHeader(http::Header::AccessControlAllowHeaders, "Content-Type")
                .setHeader(http::Header::AccessControlMaxAge, "600")
                .setHeader(http::Header::Connection, "close");

        co_return response;
    }

    asio::awaitable<http::Response> GET_format(
        const http::Request & request,
        std::vector<server::RouteArg> args,
        server::QueryArgsList query_args,
        registry::Registry & registry)
    {
        http::Response response;
        response.setCode(http::Code::Unknown)
                .setVersion("HTTP/1.1")
                .setHeader(http::Header::AccessControlAllowOrigin, "*")
                .setHeader(http::Header::Connection, "close")
                .setHeader(http::Header::ContentType, "application/json");

        if(args.size() != 1 || query_args.size() != 2)
        {
            response.setCode(http::Code::BadRequest)
                .setBodyWithContentLength(json{
                    {"message", "Invalid number of arguments"}
                }.dump());

            co_return response;
        }

        const auto format_hash_arg = parse::parseRouteArgAs<std::string>(args.at(0));
        if(!format_hash_arg)
        {
            response.setCode(http::Code::BadRequest)
                .setBodyWithContentLength(json{
                    {"message", "Invalid format hash argument"}
                }.dump());

            co_return response;
        }

        if(query_args.contains("limit") == false || query_args.contains("page") == false)
        {
            response.setCode(http::Code::BadRequest)
                .setBodyWithContentLength(json{
                    {"message", "Missing arguments limit or page"}
                }.dump());

            co_return response;
        }

        const std::optional<evmc::bytes32> format_hash_res = evmc::from_hex<evmc::bytes32>(format_hash_arg.value());
        if(!format_hash_res)
        {
            response.setCode(http::Code::BadRequest)
                .setBodyWithContentLength(json{
                    {"message", "Invalid format hash"}
                }.dump());

            co_return response;
        }

        static const std::size_t MAX_LIMIT = 256;

        const auto limit_res = parse::parseRouteArgAs<std::size_t>(query_args.at("limit"))
            .and_then([](std::size_t limit) -> parse::Result<std::size_t>
            {
                if(limit > MAX_LIMIT) return std::unexpected(parse::ParseError{parse::ParseError::Kind::OUT_OF_RANGE});
                return limit;
            });

        const auto page_res = parse::parseRouteArgAs<std::size_t>(query_args.at("page"));

        if(!limit_res || !page_res)
        {
            std::string msg_str = "Invalid arguments limit or page.";
            if(!limit_res) msg_str += std::format(" limit error: {}.", limit_res.error().kind);
            if(!page_res) msg_str += std::format(" page error: {}.", page_res.error().kind);

            response.setCode(http::Code::BadRequest)
                .setBodyWithContentLength(json{
                    {"message", msg_str}
                }.dump());

            co_return response;
        }

        const std::size_t limit = limit_res.value();
        const std::size_t page = page_res.value();

        absl::flat_hash_set<chain::Address> connectors_set = co_await registry.getConnectorsByFormatHash(*format_hash_res);
        std::vector<chain::Address> connector_addresses;
        connector_addresses.reserve(connectors_set.size());
        for(const auto & address : connectors_set)
        {
            connector_addresses.push_back(address);
        }

        std::sort(
            connector_addresses.begin(),
            connector_addresses.end(),
            [](const chain::Address & lhs, const chain::Address & rhs)
            {
                return std::memcmp(lhs.bytes, rhs.bytes, sizeof(lhs.bytes)) < 0;
            });

        const std::size_t total_connectors = connector_addresses.size();
        const std::size_t start =
            (limit == 0 || page > std::numeric_limits<std::size_t>::max() / limit)
                ? total_connectors
                : std::min(page * limit, total_connectors);
        const std::size_t end =
            (limit > (total_connectors - start))
                ? total_connectors
                : (start + limit);

        json json_output;
        json_output["format_hash"] = evmc::hex(*format_hash_res);
        json_output["page"] = page;
        json_output["limit"] = limit;
        json_output["total_connectors"] = total_connectors;
        json_output["scalars"] = json::array();
        const auto scalar_labels_res = co_await registry.getScalarLabelsByFormatHash(*format_hash_res);
        if(scalar_labels_res.has_value())
        {
            std::vector<std::string> scalar_tail_entries;
            scalar_tail_entries.reserve(scalar_labels_res->size());
            for(const auto & scalar_label : *scalar_labels_res)
            {
                scalar_tail_entries.push_back(std::format("{}:{}", scalar_label.scalar, scalar_label.tail_id));
            }

            std::sort(scalar_tail_entries.begin(), scalar_tail_entries.end());
            for(const auto & entry : scalar_tail_entries)
            {
                json_output["scalars"].emplace_back(entry);
            }
        }
        json_output["connectors"] = json::array();

        for(std::size_t i = start; i < end; ++i)
        {
            const auto & connector_address = connector_addresses[i];
            json connector_json;
            connector_json["local_address"] = evmc::hex(connector_address);
            connector_json["address"] = "0x0";

            const auto name_res = co_await registry.getConnectorName(connector_address);
            if(name_res.has_value())
            {
                connector_json["name"] = *name_res;
            }

            json_output["connectors"].emplace_back(std::move(connector_json));
        }

        response.setCode(http::Code::OK)
            .setBodyWithContentLength(json_output.dump());

        co_return response;
    }
}
