#include "api.hpp"

namespace dcn
{
    void setCORSHeaders(const http::Request & request, http::Response& response)
    {
        const auto origin_header = request.getHeader(http::Header::Origin);

        if (origin_header.empty() == false)
        {
            response.setHeader(http::Header::AccessControlAllowOrigin, origin_header.at(0));
        }
        else 
        {
            response.setHeader(http::Header::AccessControlAllowOrigin, "*");
        }
        
        response.setHeader(http::Header::AccessControlAllowCredentials, "true");
    }

    asio::awaitable<http::Response> GET_version(const http::Request & request, std::vector<server::RouteArg>, server::QueryArgsList, const std::string & build_timestamp)
    {
        http::Response response;
        response.setVersion("HTTP/1.1");
        response.setHeader(http::Header::Connection, "close");

        setCORSHeaders(request, response);

        response.setHeader(http::Header::ContentType, "application/json");
        response.setCode(http::Code::OK);
        
        json json_output;
        json_output["version"] = std::format("{}.{}.{}", dcn::MAJOR_VERSION, dcn::MINOR_VERSION, dcn::PATCH_VERSION);
        json_output["build_timestamp"] = build_timestamp;
        response.setBodyWithContentLength(json_output.dump());
        co_return response;
    }
}