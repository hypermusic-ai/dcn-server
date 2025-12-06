#pragma once

#include <random>
#include <string>
#include <expected>
#include <format>
#include <regex>

#include "native.h"
#include <asio.hpp>
#include <asio/experimental/awaitable_operators.hpp>
using namespace asio::experimental::awaitable_operators;

#include <absl/container/flat_hash_map.h>
#include <spdlog/spdlog.h>
#include <secp256k1.h>
#include <secp256k1_recovery.h>
#include <jwt-cpp/jwt.h>

// Undefine the conflicting macro
#ifdef interface
    #undef interface
#endif
#include <evmc/evmc.hpp>
#ifndef interface
    #define interface __STRUCT__
#endif

#include "utils.hpp"
#include "keccak256.hpp"
#include "http.hpp"
#include "parse_error.hpp"

namespace dcn::parse
{
    Result<std::string> parseNonceFromMessage(const std::string & msg);

    // access token

    // from header
    template<http::Header HeaderType>
    Result<std::string> parseAccessTokenFrom(const std::string & header_str);
    
    template<>
    Result<std::string> parseAccessTokenFrom<http::Header::Cookie>(const std::string& header_str);

    template<>
    Result<std::string> parseAccessTokenFrom<http::Header::Authorization>(const std::string& header_str);

    // to header
    template<http::Header HeaderType>
    std::string parseAccessTokenTo(const std::string & token_str);

    template<>
    std::string parseAccessTokenTo<http::Header::SetCookie>(const std::string & token_str);

    template<>
    std::string parseAccessTokenTo<http::Header::Authorization>(const std::string & token_str);

    // refresh token

    // from header
    template<http::Header HeaderType>
    Result<std::string> parseRefreshTokenFrom(const std::string & header_str);

    template<>
    Result<std::string> parseRefreshTokenFrom<http::Header::Cookie>(const std::string & header_str);

    template<>
    Result<std::string> parseRefreshTokenFrom<http::Header::XRefreshToken>(const std::string & header_str);

    // to header
    template<http::Header HeaderType>
    std::string parseRefreshTokenTo(const std::string & token_str);

    template<>
    std::string parseRefreshTokenTo<http::Header::SetCookie>(const std::string & token_str);

    template<>
    std::string parseRefreshTokenTo<http::Header::XRefreshToken>(const std::string & token_str);
}

namespace dcn
{
    struct AuthError
    {
        enum class Kind : std::uint8_t
        {
            UNKNOWN = 0,

            MISSING_COOKIE,
            INVALID_COOKIE,

            MISSING_TOKEN,
            INVALID_TOKEN,

            INVALID_SIGNATURE,

            INVALID_NONCE,

            INVALID_ADDRESS
        }
        kind = Kind::UNKNOWN;

        std::string message = "";
    };

    class AuthManager
    {
        public:
            AuthManager() = delete;
            AuthManager(asio::io_context & io_context);

            AuthManager(const AuthManager&) = delete;
            AuthManager& operator=(const AuthManager&) = delete;
            
            ~AuthManager() = default;

            asio::awaitable<std::string> generateNonce(const evmc::address & address);

            asio::awaitable<bool> verifyNonce(const evmc::address & address, const std::string & nonce);

            asio::awaitable<bool> verifySignature(const evmc::address & address, const std::string& signature, const std::string& message);

            asio::awaitable<std::string> generateAccessToken(const evmc::address & address);

            asio::awaitable<std::expected<evmc::address, AuthError>> verifyAccessToken(std::string token) const;

            asio::awaitable<bool> compareAccessToken(const evmc::address & address, std::string token) const;

            asio::awaitable<std::string> generateRefreshToken(const evmc::address & address);

            asio::awaitable<std::expected<evmc::address, AuthError>> verifyRefreshToken(std::string token) const;

        private:
            asio::strand<asio::io_context::executor_type> _strand;

            const std::string _SECRET; // !!! TODO !!! use secure secret in production

            std::mt19937 _rng;
            std::uniform_int_distribution<int> _dist;
            absl::flat_hash_map<evmc::address, std::string> _nonces;

            absl::flat_hash_map<evmc::address, std::string> _refresh_tokens;
            absl::flat_hash_map<evmc::address, std::string> _access_tokens;
    };
}

template <>
struct std::formatter<dcn::AuthError::Kind> : std::formatter<std::string> {
    auto format(const dcn::AuthError::Kind & err, format_context& ctx) const {
        switch(err)
        {
            case dcn::AuthError::Kind::MISSING_COOKIE : return formatter<string>::format("Missing cookie", ctx);
            case dcn::AuthError::Kind::INVALID_COOKIE : return formatter<string>::format("Invalid cookie", ctx);
            case dcn::AuthError::Kind::MISSING_TOKEN : return formatter<string>::format("Missing token", ctx);
            case dcn::AuthError::Kind::INVALID_TOKEN : return formatter<string>::format("Invalid token", ctx);
            case dcn::AuthError::Kind::INVALID_SIGNATURE : return formatter<string>::format("Invalid signature", ctx);
            case dcn::AuthError::Kind::INVALID_NONCE : return formatter<string>::format("Invalid nonce", ctx);
            case dcn::AuthError::Kind::INVALID_ADDRESS : return formatter<string>::format("Invalid address", ctx);

            default:  return formatter<string>::format("Unknown", ctx);
        }
        return formatter<string>::format("", ctx);
    }
};