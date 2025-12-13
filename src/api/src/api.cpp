#include "api.hpp"

namespace dcn
{
    asio::awaitable<http::Response> GET_version(const http::Request & request, std::vector<server::RouteArg>, server::QueryArgsList, const std::string & build_timestamp)
    {
        http::Response response;
        response.setCode(http::Code::OK)
                .setVersion("HTTP/1.1")
                .setHeader(http::Header::AccessControlAllowOrigin, "*")
                .setHeader(http::Header::CacheControl, "public, max-age=60")
                .setHeader(http::Header::ContentType, "application/json")
                .setHeader(http::Header::Connection, "close")
                .setBodyWithContentLength(json {
                    {"version", std::format("{}.{}.{}", dcn::MAJOR_VERSION, dcn::MINOR_VERSION, dcn::PATCH_VERSION)}, 
                    {"build_timestamp", build_timestamp}
                }.dump());

        co_return response;
    }
}