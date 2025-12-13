#include "api.hpp"

namespace dcn
{
    // TODO VERIFY WHETHER FILE EXISTS
    asio::awaitable<http::Response> HEAD_serveFile(const http::Request & request, std::vector<server::RouteArg>, server::QueryArgsList)
    {
        http::Response response;
        response.setCode(dcn::http::Code::NoContent)
                .setVersion("HTTP/1.1")
                .setHeader(http::Header::AccessControlAllowOrigin, "*")
                .setHeader(http::Header::Connection, "close");

        co_return response;
    }

    asio::awaitable<http::Response> OPTIONS_serveFile(const http::Request & request, std::vector<server::RouteArg>, server::QueryArgsList)
    {
        http::Response response;
        response.setCode(dcn::http::Code::NoContent)
                .setVersion("HTTP/1.1")
                .setHeader(http::Header::AccessControlAllowOrigin, "*")
                .setHeader(http::Header::AccessControlAllowMethods, "HEAD, GET, OPTIONS")
                .setHeader(http::Header::AccessControlAllowHeaders, "Content-Type")
                .setHeader(http::Header::AccessControlMaxAge, "600")
                .setHeader(http::Header::Connection, "close");

        co_return response;
    }

    asio::awaitable<http::Response> GET_serveFile(const http::Request & request, std::vector<server::RouteArg>, server::QueryArgsList, const std::string mime_type, const std::string & file_content)
    {
        http::Response response;
        response.setCode(dcn::http::Code::OK)
                .setVersion("HTTP/1.1")
                .setHeader(http::Header::AccessControlAllowOrigin, "*")
                .setHeader(http::Header::ContentType, mime_type)
                .setHeader(http::Header::Connection, "close")
                .setBodyWithContentLength(file_content);

        co_return response;
    }

    asio::awaitable<http::Response> GET_serveBinaryFile(const http::Request & request, std::vector<server::RouteArg>, server::QueryArgsList, const std::string mime_type, const std::vector<std::byte> & file_content)
    {
        http::Response response;
        response.setCode(dcn::http::Code::OK)
                .setVersion("HTTP/1.1")
                .setHeader(http::Header::AccessControlAllowOrigin, "*")
                .setHeader(http::Header::ContentType, mime_type)
                .setHeader(http::Header::Connection, "close")
                .setBodyWithContentLength( std::string(reinterpret_cast<const char*>(file_content.data()), file_content.size()));
        
        co_return response;
    }
}