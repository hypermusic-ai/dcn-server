#include "api.hpp"

namespace dcn
{
    static const std::string NONCE_PREFIX = "Login nonce: ";
    static const std::string ACCESS_TOKEN_PREFIX = "access_token=";

    static parse::Result<std::string> _getAccessTokenFromHeader(const http::Request & request) 
    {
        const auto auth_res = request.getHeader(http::Header::Authorization);
        if(auth_res.empty())
        {
            return std::unexpected(parse::ParseError{parse::ParseError::Kind::INVALID_VALUE, "Authorization header not found"});
        }

        const std::string auth_header = std::accumulate(auth_res.begin(), auth_res.end(), std::string(""));

        static const std::regex token_regex(R"(Bearer\s+([^\s]+))");

        if (auth_header.empty()) {
            return std::unexpected(parse::ParseError{parse::ParseError::Kind::INVALID_VALUE, "Header string is empty"});
        }

        std::smatch match;
        if (std::regex_search(auth_header, match, token_regex) == false) {
            return std::unexpected(parse::ParseError{parse::ParseError::Kind::INVALID_VALUE, "Token not found in header"});
        }

        return match[1].str();
    }

    asio::awaitable<std::expected<evm::Address, auth::AuthError>> authenticate(const http::Request & request, const auth::AuthManager & auth_manager)
    {
        // try to obtain token from authorization header
        const parse::Result<std::string> token_res = _getAccessTokenFromHeader(request);

        if (!token_res) 
        {
            spdlog::error("Failed to parse token");
            co_return std::unexpected(auth::AuthError{auth::AuthError::Kind::INVALID_TOKEN});
        }
        const std::string & token = token_res.value();

        const auto verification_res = co_await auth_manager.verifyAccessToken(token);

        if(!verification_res)
        {
            spdlog::error("Failed to verify token");
            co_return std::unexpected(verification_res.error());
        }

        co_return verification_res.value();
    }

    asio::awaitable<http::Response> GET_nonce(const http::Request & request, std::vector<server::RouteArg> args, server::QueryArgsList, auth::AuthManager & auth_manager)
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
                    {"message", "Invalid number of arguments. Expected 1 argument."}
                }.dump());

            co_return response;
        }
        const auto address_arg = parse::parseRouteArgAs<std::string>(args.at(0));

        if(!address_arg)
        {
            response.setCode(http::Code::BadRequest)
                .setBodyWithContentLength(json {
                    {"message", "Invalid address argument"}
                }.dump());

            co_return response;
        }

        const std::optional<evm::Address> address_res = evmc::from_hex<evm::Address>(address_arg.value());
        if(!address_res)
        {
            response.setCode(http::Code::BadRequest)
                .setBodyWithContentLength(json{
                    {"message", "Invalid address value"}
                }.dump());

            co_return response;
        }
        
        const auto generated_nonce = co_await auth_manager.generateNonce(*address_res);

        response.setCode(http::Code::OK)
            .setBodyWithContentLength(json{
                {"nonce", generated_nonce}
            }.dump());
        
        co_return response;
    }

    asio::awaitable<http::Response> OPTIONS_auth(const http::Request & request, std::vector<server::RouteArg>, server::QueryArgsList)
    {
        http::Response response;
        response.setCode(http::Code::NoContent)
                .setVersion("HTTP/1.1")
                .setHeader(http::Header::AccessControlAllowOrigin, "*")
                .setHeader(http::Header::AccessControlAllowMethods, "POST, OPTIONS")
                .setHeader(http::Header::AccessControlAllowHeaders, "Authorization, Content-Type")
                .setHeader(http::Header::AccessControlMaxAge, "600")
                .setHeader(http::Header::Connection, "close");

        co_return response;
    }

    asio::awaitable<http::Response> POST_auth(const http::Request & request, std::vector<server::RouteArg> args, server::QueryArgsList, auth::AuthManager & auth_manager)
    {
        http::Response response;
        response.setCode(http::Code::Unknown)
                .setVersion("HTTP/1.1")
                .setHeader(http::Header::AccessControlAllowOrigin, "*")
                .setHeader(http::Header::Connection, "close")
                .setHeader(http::Header::ContentType, "application/json");

        if(args.size() != 0)
        {
            response.setCode(http::Code::BadRequest)
                .setBodyWithContentLength(json{
                    {"message", "Invalid number of arguments. Expected 1 argument."}
                }.dump());

            co_return response;
        }

        const json auth_request = json::parse(request.getBody());

        if(auth_request.contains("address") == false)
        {
            response.setCode(http::Code::BadRequest)
                .setBodyWithContentLength(json {
                    {"message", "Missing address"}
                }.dump());

            co_return response;
        }

        if(auth_request.contains("signature") == false)
        {
            response.setCode(http::Code::BadRequest)
                .setBodyWithContentLength(json {
                    {"message", "Missing signature"}
                }.dump());

            co_return response;
        }

        if(auth_request.contains("message") == false)
        {
            response.setCode(http::Code::BadRequest)
                .setBodyWithContentLength(json {
                    {"message", "Missing message"}
                }.dump());

            co_return response;
        }

        const std::string & address_str = auth_request["address"].get<std::string>();
        const std::string & signature = auth_request["signature"].get<std::string>();
        const std::string & message = auth_request["message"].get<std::string>();

        // Check that message is at least as long as the prefix and
        // does it start with prefix?
        if (message.size() <= NONCE_PREFIX.size() ||
            message.compare(0, NONCE_PREFIX.size(), NONCE_PREFIX) != 0) 
        {
            spdlog::error("Nonce too short or does not start with prefix");

            response.setCode(http::Code::BadRequest)
                .setBodyWithContentLength(json {
                    {"message", "Invalid nonce"}
                }.dump());

            co_return response;
        }

        // Extract everything after prefix
        const std::string request_nonce = message.substr(NONCE_PREFIX.size());

        auto address_res = evmc::from_hex<evm::Address>(address_str);
        if(!address_res)
        {
            response.setCode(http::Code::BadRequest)
                .setBodyWithContentLength(json {
                    {"message", "Invalid address"}
                }.dump());

            co_return response;
        }
        const evm::Address & address = address_res.value();

        if(co_await auth_manager.verifyNonce(address, request_nonce) == false)
        {
            response.setCode(http::Code::BadRequest)
                .setBodyWithContentLength(json {
                    {"message", "Invalid nonce"}
                }.dump());

            co_return response;
        }

        if(co_await auth_manager.verifySignature(address, signature, message) == false)
        {
            response.setCode(http::Code::BadRequest)
                .setBodyWithContentLength(json {
                    {"message", "Invalid signature"}
                }.dump());
                
            co_return response;
        }

        const std::string access_token = co_await auth_manager.generateAccessToken(address);
        
        // set access token in authorization header
        // Authorization: Bearer <token>
        // also include access token in response
        response.setCode(http::Code::OK)
            .setHeader(http::Header::Authorization, std::format("Bearer {}", access_token))
            .setBodyWithContentLength(json {
                {"access_token", access_token}
            }.dump());

        co_return response;
    }
}