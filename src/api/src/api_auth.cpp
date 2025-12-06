#include "api.hpp"

namespace dcn
{
    asio::awaitable<std::expected<evm::Address, AuthError>> authenticate(const http::Request & request, const AuthManager & auth_manager)
    {
        parse::Result<std::string> token_res;

        // firstly try to obtain token from authorization header
        const auto auth_res = request.getHeader(http::Header::Authorization);
        if(auth_res.empty() == false)
        {
            const std::string auth_header = std::accumulate(auth_res.begin(), auth_res.end(), std::string(""));
            token_res = parse::parseAccessTokenFrom<http::Header::Authorization>(auth_header);
        }
        else 
        {
            // if authorization header is empty, try to obtain token from cookie header
            auto cookie_res = request.getHeader(http::Header::Cookie);
            if(cookie_res.empty() == false)
            {
                const std::string cookie_header = std::accumulate(cookie_res.begin(), cookie_res.end(), std::string(""));
                token_res = parse::parseAccessTokenFrom<http::Header::Cookie>(cookie_header);
            }
        }

        if (!token_res) 
        {
            spdlog::error("Failed to parse token");
            co_return std::unexpected(AuthError{AuthError::Kind::INVALID_TOKEN});
        }

        const std::string & token = token_res.value();

        auto verification_res = co_await auth_manager.verifyAccessToken(token);

        if(!verification_res)
        {
            spdlog::error("Failed to verify token");
            co_return std::unexpected(verification_res.error());
        }

        co_return verification_res.value();
    }

    asio::awaitable<http::Response> GET_nonce(const http::Request & request, std::vector<server::RouteArg> args, server::QueryArgsList, AuthManager & auth_manager)
    {
        http::Response response;
        response.setVersion("HTTP/1.1")
                .setHeader(http::Header::Connection, "close");
        
        setCORSHeaders(request, response);

        if(args.size() != 1)
        {
            response.setCode(http::Code::BadRequest);
            response.setHeader(http::Header::ContentType, "application/json");

            json error_output;
            error_output["error"] = std::format("{}", response.getCode());
            error_output["message"] = "Invalid number of arguments. Expected 1 argument.";
            response.setBodyWithContentLength(error_output.dump());

            co_return response;
        }
        auto address_arg = parse::parseRouteArgAs<std::string>(args.at(0));

        if(!address_arg)
        {
            response.setCode(http::Code::BadRequest);
            response.setHeader(http::Header::ContentType, "application/json");

            json error_output;
            error_output["error"] = std::format("{}", response.getCode());
            error_output["message"] = "Invalid address argument";
            response.setBodyWithContentLength(error_output.dump());

            co_return response;
        }

        std::optional<evm::Address> address_res = evmc::from_hex<evm::Address>(address_arg.value());
        if(!address_res)
        {
            response.setCode(http::Code::BadRequest);
            response.setHeader(http::Header::ContentType, "application/json");

            json error_output;
            error_output["error"] = std::format("{}", response.getCode());
            error_output["message"] = "Invalid address argument";
            response.setBodyWithContentLength(error_output.dump());

            co_return response;
        }
        
        auto generated_nounce = co_await auth_manager.generateNonce(*address_res);
        json nonce_json({{"nonce", generated_nounce}});

        response.setCode(http::Code::OK)
                .setHeader(http::Header::ContentType, "application/json")
                .setBodyWithContentLength(nonce_json.dump());
        co_return response;
    }

    asio::awaitable<http::Response> OPTIONS_auth(const http::Request & request, std::vector<server::RouteArg>, server::QueryArgsList)
    {
        http::Response response;
        response.setVersion("HTTP/1.1");

        setCORSHeaders(request, response);

        response.setHeader(http::Header::AccessControlAllowMethods, "POST, OPTIONS");
        response.setHeader(http::Header::AccessControlAllowHeaders, "authorization, content-type");
        response.setHeader(http::Header::Connection, "close");
        response.setHeader(http::Header::ContentType, "text/plain");
        response.setHeader(http::Header::AccessControlAllowCredentials, "true");
        response.setCode(http::Code::OK);
        response.setBodyWithContentLength("OK");
        co_return response;
    }

    asio::awaitable<http::Response> POST_auth(const http::Request & request, std::vector<server::RouteArg> args, server::QueryArgsList, AuthManager & auth_manager)
    {
        http::Response response;
        response.setVersion("HTTP/1.1")
                .setHeader(http::Header::Connection, "close");

        setCORSHeaders(request, response);

        if(args.size() != 0)
        {
            response.setCode(http::Code::BadRequest);
            response.setHeader(http::Header::ContentType, "application/json");

            json error_output;
            error_output["error"] = std::format("{}", response.getCode());
            error_output["message"] = "Invalid number of arguments. Expected 1 argument.";
            response.setBodyWithContentLength(error_output.dump());

            co_return response;
        }

        json auth_request = json::parse(request.getBody());

        if(auth_request.contains("address") == false)
        {
            response.setCode(http::Code::BadRequest);
            response.setHeader(http::Header::ContentType, "application/json");

            json error_output;
            error_output["error"] = std::format("{}", response.getCode());
            error_output["message"] = "Missing address";
            response.setBodyWithContentLength(error_output.dump());

            co_return response;
        }

        if(auth_request.contains("signature") == false)
        {
            response.setCode(http::Code::BadRequest);
            response.setHeader(http::Header::ContentType, "application/json");

            json error_output;
            error_output["error"] = std::format("{}", response.getCode());
            error_output["message"] = "Missing signature";
            response.setBodyWithContentLength(error_output.dump());

            co_return response;
        }

        if(auth_request.contains("message") == false)
        {
            response.setCode(http::Code::BadRequest);
            response.setHeader(http::Header::ContentType, "application/json");

            json error_output;
            error_output["error"] = std::format("{}", response.getCode());
            error_output["message"] = "Missing message";
            response.setBodyWithContentLength(error_output.dump());

            co_return response;
        }

        const std::string & address_str = auth_request["address"].get<std::string>();
        const std::string & signature = auth_request["signature"].get<std::string>();
        const std::string & message = auth_request["message"].get<std::string>();
        const auto request_nonce = parse::parseNonceFromMessage(message);

        if(!request_nonce)
        {
            response.setCode(http::Code::BadRequest);
            response.setHeader(http::Header::ContentType, "application/json");

            json error_output;
            error_output["error"] = std::format("{}", response.getCode());
            error_output["message"] = "Invalid message";
            response.setBodyWithContentLength(error_output.dump());

            co_return response;
        }

        auto address_res = evmc::from_hex<evm::Address>(address_str);
        if(!address_res)
        {
            response.setCode(http::Code::BadRequest);
            response.setHeader(http::Header::ContentType, "application/json");

            json error_output;
            error_output["error"] = std::format("{}", response.getCode());
            error_output["message"] = "Invalid address";
            response.setBodyWithContentLength(error_output.dump());

            co_return response;
        }
        const evm::Address & address = address_res.value();

        if(co_await auth_manager.verifyNonce(address, *request_nonce) == false)
        {
            response.setCode(http::Code::BadRequest);
            response.setHeader(http::Header::ContentType, "application/json");

            json error_output;
            error_output["error"] = std::format("{}", response.getCode());
            error_output["message"] = "Invalid nonce";
            response.setBodyWithContentLength(error_output.dump());

            co_return response;
        }

        if(co_await auth_manager.verifySignature(address, signature, message) == false)
        {
            response.setCode(http::Code::BadRequest);
            response.setHeader(http::Header::ContentType, "application/json");

            json error_output;
            error_output["error"] = std::format("{}", response.getCode());
            error_output["message"] = "Invalid signature";
            response.setBodyWithContentLength(error_output.dump());


            co_return response;
        }

        const std::string access_token = co_await auth_manager.generateAccessToken(address);
        const std::string refresh_token = co_await auth_manager.generateRefreshToken(address);
        
        response.setHeader(http::Header::ContentType, "application/json");

        json auth_response;
        auth_response["access_token"] = access_token;
        auth_response["refresh_token"] = refresh_token;

        response.addHeader(http::Header::SetCookie, parse::parseAccessTokenTo<http::Header::SetCookie>(access_token));
        response.addHeader(http::Header::SetCookie, parse::parseRefreshTokenTo<http::Header::SetCookie>(refresh_token));

        response.setCode(http::Code::OK).setBodyWithContentLength(auth_response.dump());

        co_return response;
    }

    asio::awaitable<http::Response> OPTIONS_refresh(const http::Request & request, std::vector<server::RouteArg>, server::QueryArgsList)
    {
        http::Response response;
        response.setVersion("HTTP/1.1");

        setCORSHeaders(request, response);

        response.setHeader(http::Header::AccessControlAllowMethods, "POST, OPTIONS");
        response.setHeader(http::Header::AccessControlAllowHeaders, "authorization, content-type, x-refresh-token");
        response.setHeader(http::Header::Connection, "close");
        response.setHeader(http::Header::ContentType, "text/plain");
        response.setHeader(http::Header::AccessControlAllowCredentials, "true");
        response.setCode(http::Code::OK);
        response.setBodyWithContentLength("OK");
        co_return response;
    }

    asio::awaitable<http::Response> POST_refresh(const http::Request & request, std::vector<server::RouteArg> args, server::QueryArgsList, AuthManager & auth_manager)
    {
        http::Response response;
        response.setVersion("HTTP/1.1");
    
        setCORSHeaders(request, response);

        parse::Result<std::string> refresh_token_res;
        // firstly try to obtain refresh token from XRefreshToken header
        const auto xRefreshToken_res = request.getHeader(http::Header::XRefreshToken);
        if(xRefreshToken_res.empty() == false)
        {
            const std::string xRefreshToken_header = std::accumulate(xRefreshToken_res.begin(), xRefreshToken_res.end(), std::string(""));
            refresh_token_res = parse::parseRefreshTokenFrom<http::Header::XRefreshToken>(xRefreshToken_header);
        }
        else 
        {
            // if XRefreshToken header is empty, try to obtain token from cookie header
            auto cookie_res = request.getHeader(http::Header::Cookie);
            if(cookie_res.empty() == false)
            {
                const std::string cookie_header = std::accumulate(cookie_res.begin(), cookie_res.end(), std::string(""));
                refresh_token_res = parse::parseRefreshTokenFrom<http::Header::Cookie>(cookie_header);
            }
        }       
        
        if (!refresh_token_res) {
            response.setCode(dcn::http::Code::Unauthorized)
                    .setHeader(http::Header::Connection, "close")
                    .setHeader(http::Header::ContentType, "application/json");

            json error_output;
            error_output["error"] = std::format("{}", response.getCode());
            error_output["message"] = "Missing refresh token";
            response.setBodyWithContentLength(error_output.dump());

            co_return std::move(response);
        }
        const std::string & refresh_token = refresh_token_res.value();

        auto refresh_verification_res = co_await auth_manager.verifyRefreshToken(refresh_token);
        if(!refresh_verification_res)
        {
            response.setCode(dcn::http::Code::Unauthorized)
                    .setHeader(http::Header::Connection, "close")
                    .setHeader(http::Header::ContentType, "text/plain");

            json error_output;
            error_output["error"] = std::format("{}", response.getCode());
            error_output["message"] = std::format("Error: {}", refresh_verification_res.error().kind);
            response.setBodyWithContentLength(error_output.dump());

            co_return std::move(response);
        }
        spdlog::debug(std::format("refresh token verified: {}", refresh_verification_res.value()));


        // Verify if access token matches old one for that address
        // since it probably expired we does not strictly verify it
        // only compare it
        dcn::parse::Result<std::string> access_token_res;

        // firstly try to obtain token from authorization header
        const auto auth_res = request.getHeader(http::Header::Authorization);
        if(auth_res.empty() == false)
        {
            const std::string auth_header = std::accumulate(auth_res.begin(), auth_res.end(), std::string(""));
            access_token_res = parse::parseAccessTokenFrom<http::Header::Authorization>(auth_header);
        }
        else 
        {
            // if authorization header is empty, try to obtain token from cookie header
            auto cookie_res = request.getHeader(http::Header::Cookie);
            if(cookie_res.empty() == false)
            {
                const std::string cookie_header = std::accumulate(cookie_res.begin(), cookie_res.end(), std::string(""));
                access_token_res = parse::parseAccessTokenFrom<http::Header::Cookie>(cookie_header);
            }
        }

        if (!access_token_res) {
            response.setCode(dcn::http::Code::Unauthorized)
                    .setHeader(http::Header::Connection, "close")
                    .setHeader(http::Header::ContentType, "text/plain");

            json error_output;
            error_output["error"] = std::format("{}", response.getCode());
            error_output["message"] = "Missing access token";
            response.setBodyWithContentLength(error_output.dump());

            co_return std::move(response);
        }
        const std::string & access_token = access_token_res.value();

        const evm::Address & address = refresh_verification_res.value();

        if(co_await auth_manager.compareAccessToken(address, access_token) == false)
        {
            response.setCode(dcn::http::Code::Unauthorized)
                    .setHeader(http::Header::Connection, "close")
                    .setHeader(http::Header::ContentType, "text/plain");

            json error_output;
            error_output["error"] = std::format("{}", response.getCode());
            error_output["message"] = "Invalid access token";
            response.setBodyWithContentLength(error_output.dump());

            co_return std::move(response);
        }
        spdlog::debug("access token verified");

        // generate new access and refresh tokens

        const std::string new_access_token = co_await auth_manager.generateAccessToken(address);
        const std::string new_refresh_token = co_await auth_manager.generateRefreshToken(address);

        response.setHeader(http::Header::ContentType, "application/json");

        json refresh_response;

        refresh_response["access_token"] = new_access_token;
        refresh_response["refresh_token"] = new_refresh_token;

        response.addHeader(http::Header::SetCookie, parse::parseAccessTokenTo<http::Header::SetCookie>(new_access_token));
        response.addHeader(http::Header::SetCookie, parse::parseRefreshTokenTo<http::Header::SetCookie>(new_refresh_token));

        response.setCode(http::Code::OK).setBodyWithContentLength(refresh_response.dump());

        co_return response;
    }
}