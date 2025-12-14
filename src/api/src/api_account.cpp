#include "api.hpp"

namespace dcn
{
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

        if(args.size() != 1 || query_args.size() != 2)
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

        if(query_args.contains("limit") == false || query_args.contains("page") == false)
        {
            response.setCode(http::Code::BadRequest)
                .setBodyWithContentLength(json {
                    {"message", "Missing arguments limit or page"}
                }.dump());
            
            co_return response;
        }

        std::optional<evm::Address> address_res = evmc::from_hex<evm::Address>(address_arg.value());
        if(!address_res)
        {
            response.setCode(http::Code::BadRequest)
                .setBodyWithContentLength(json{
                    {"message", "Invalid address"}
                }.dump());
            
            co_return response;
        }
        const auto & address = address_res.value();

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
                .setBodyWithContentLength(json {
                    {"message", msg_str}
                }.dump());

            co_return response;
        }

        const auto & limit = limit_res.value();
        const auto & page = page_res.value();
        const std::size_t start = page * limit;

        const auto features = co_await registry.getOwnedFeatures(address);
        const auto transformations = co_await registry.getOwnedTransformations(address);
        const auto conditions = co_await registry.getOwnedConditions(address);

        // Subrange features
        auto features_sub = features 
            | std::views::drop(start)
            | std::views::take(limit);

        // Subrange transformations
        auto transformations_sub = transformations 
            | std::views::drop(start)
            | std::views::take(limit);

        // Subrange conditions
        auto conditions_sub = conditions 
            | std::views::drop(start)
            | std::views::take(limit);

        // implement subrange 
        json json_output;
        json_output["owned_features"] = json::array();
        for (const auto& f : features_sub) json_output["owned_features"].push_back(f);

        json_output["owned_transformations"] = json::array();
        for (const auto& t : transformations_sub) json_output["owned_transformations"].push_back(t);

        json_output["owned_conditions"] = json::array();
        for (const auto& c : conditions_sub) json_output["owned_conditions"].push_back(c);

        json_output["address"] = evmc::hex(address);
        json_output["page"] = page;
        json_output["limit"] = limit;
        json_output["total_features"] = features.size();
        json_output["total_transformations"] = transformations.size();
        json_output["total_conditions"] = conditions.size();

        response.setCode(http::Code::OK)
            .setBodyWithContentLength(json_output.dump());
        
        co_return response;
    }
}