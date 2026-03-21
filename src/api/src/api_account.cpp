#include "api.hpp"

namespace dcn
{
    namespace
    {
        constexpr std::size_t MAX_LIMIT = 256;

        std::optional<registry::NameCursor> parseNameCursor(const std::string & name_token)
        {
            if(name_token.empty())
            {
                return std::nullopt;
            }

            return name_token;
        }

        std::string serializeNameCursor(const registry::NameCursor & cursor)
        {
            return cursor;
        }
    }

    asio::awaitable<http::Response> OPTIONS_accountInfo(const http::Request & request, std::vector<server::RouteArg>, server::QueryArgsList)
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

    asio::awaitable<http::Response> GET_accountInfo(const http::Request & request, std::vector<server::RouteArg> args, server::QueryArgsList query_args, registry::Registry & registry)
    {
        http::Response response;
        response.setCode(http::Code::Unknown)
                .setVersion("HTTP/1.1")
                .setHeader(http::Header::AccessControlAllowOrigin, "*")
                .setHeader(http::Header::Connection, "close")
                .setHeader(http::Header::ContentType, "application/json");

        if(args.size() != 1)
        {
            response.setCode(http::Code::BadRequest)
                .setBodyWithContentLength(json{
                    {"message", "Invalid number of arguments"}
                }.dump());

            co_return response;
        }
        auto address_arg = parse::parseRouteArgAs<std::string>(args.at(0));

        if(!address_arg)
        {
            response.setCode(http::Code::BadRequest)
                .setBodyWithContentLength(json{
                    {"message", "Invalid address argument"}
                }.dump());

            co_return response;
        }

        if(query_args.contains("limit") == false)
        {
            response.setCode(http::Code::BadRequest)
                .setBodyWithContentLength(json {
                    {"message", "Missing argument limit"}
                }.dump());
            
            co_return response;
        }

        std::optional<chain::Address> address_res = evmc::from_hex<chain::Address>(address_arg.value());
        if(!address_res)
        {
            response.setCode(http::Code::BadRequest)
                .setBodyWithContentLength(json{
                    {"message", "Invalid address"}
                }.dump());
            
            co_return response;
        }
        const auto & address = address_res.value();

        auto limit_res = parse::parseRouteArgAs<std::size_t>(query_args.at("limit"));
        if(limit_res && limit_res.value() > MAX_LIMIT)
        {
            limit_res = std::unexpected(parse::ParseError{parse::ParseError::Kind::OUT_OF_RANGE});
        }
        if(!limit_res)
        {
            std::string msg_str = "Invalid argument limit.";
            if(!limit_res) msg_str += std::format(" limit error: {}.", limit_res.error().kind);

            response.setCode(http::Code::BadRequest)
                .setBodyWithContentLength(json {
                    {"message", msg_str}
                }.dump());

            co_return response;
        }

        std::optional<registry::NameCursor> after_connectors;
        std::optional<registry::NameCursor> after_transformations;
        std::optional<registry::NameCursor> after_conditions;

        if(query_args.contains("after_connectors"))
        {
            const auto token = parse::parseRouteArgAs<std::string>(query_args.at("after_connectors"));
            if(!token || !(after_connectors = parseNameCursor(token.value())).has_value())
            {
                response.setCode(http::Code::BadRequest)
                    .setBodyWithContentLength(json{
                        {"message", "Invalid after_connectors cursor"}
                    }.dump());
                co_return response;
            }
        }
        if(query_args.contains("after_transformations"))
        {
            const auto token = parse::parseRouteArgAs<std::string>(query_args.at("after_transformations"));
            if(!token || !(after_transformations = parseNameCursor(token.value())).has_value())
            {
                response.setCode(http::Code::BadRequest)
                    .setBodyWithContentLength(json{
                        {"message", "Invalid after_transformations cursor"}
                    }.dump());
                co_return response;
            }
        }
        if(query_args.contains("after_conditions"))
        {
            const auto token = parse::parseRouteArgAs<std::string>(query_args.at("after_conditions"));
            if(!token || !(after_conditions = parseNameCursor(token.value())).has_value())
            {
                response.setCode(http::Code::BadRequest)
                    .setBodyWithContentLength(json{
                        {"message", "Invalid after_conditions cursor"}
                    }.dump());
                co_return response;
            }
        }

        const std::size_t limit = limit_res.value();
        const auto connectors_page = co_await registry.getOwnedConnectorsCursor(address, after_connectors, limit);
        const auto transformations_page = co_await registry.getOwnedTransformationsCursor(address, after_transformations, limit);
        const auto conditions_page = co_await registry.getOwnedConditionsCursor(address, after_conditions, limit);

        json json_output;

        json_output["owned_connectors"] = json::array();
        for(const auto & name : connectors_page.entries)
        {
            json_output["owned_connectors"].push_back(name);
        }

        json_output["owned_transformations"] = json::array();
        for(const auto & name : transformations_page.entries)
        {
            json_output["owned_transformations"].push_back(name);
        }

        json_output["owned_conditions"] = json::array();
        for(const auto & name : conditions_page.entries)
        {
            json_output["owned_conditions"].push_back(name);
        }

        json_output["address"] = evmc::hex(address);
        json_output["limit"] = limit;
        json_output["connectors_has_more"] = connectors_page.has_more;
        json_output["transformations_has_more"] = transformations_page.has_more;
        json_output["conditions_has_more"] = conditions_page.has_more;
        json_output["next_after_connectors"] =
            connectors_page.next_after.has_value() ? json(serializeNameCursor(*connectors_page.next_after)) : json(nullptr);
        json_output["next_after_transformations"] =
            transformations_page.next_after.has_value() ? json(serializeNameCursor(*transformations_page.next_after)) : json(nullptr);
        json_output["next_after_conditions"] =
            conditions_page.next_after.has_value() ? json(serializeNameCursor(*conditions_page.next_after)) : json(nullptr);

        response.setCode(http::Code::OK)
            .setBodyWithContentLength(json_output.dump());
        
        co_return response;
    }
}
