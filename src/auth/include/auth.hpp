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

#include "utils.hpp"
#include "keccak256.hpp"
#include "http.hpp"
#include "parse_error.hpp"
#include "evm.hpp"

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

namespace dcn::auth
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

            asio::awaitable<std::string> generateNonce(const evm::Address & address);

            asio::awaitable<bool> verifyNonce(const evm::Address & address, const std::string & nonce);

            asio::awaitable<bool> verifySignature(const evm::Address & address, const std::string& signature, const std::string& message);

            asio::awaitable<std::string> generateAccessToken(const evm::Address & address);

            asio::awaitable<std::expected<evm::Address, AuthError>> verifyAccessToken(std::string token) const;

            asio::awaitable<bool> compareAccessToken(const evm::Address & address, std::string token) const;

            asio::awaitable<std::string> generateRefreshToken(const evm::Address & address);

            asio::awaitable<std::expected<evm::Address, AuthError>> verifyRefreshToken(std::string token) const;

        private:
            asio::strand<asio::io_context::executor_type> _strand;

            const std::string _SECRET; // !!! TODO !!! use secure secret in production

            std::mt19937 _rng;
            std::uniform_int_distribution<int> _dist;
            absl::flat_hash_map<evm::Address, std::string> _nonces;

            absl::flat_hash_map<evm::Address, std::string> _refresh_tokens;
            absl::flat_hash_map<evm::Address, std::string> _access_tokens;
    };
}

template <>
struct std::formatter<dcn::auth::AuthError::Kind> : std::formatter<std::string> {
    auto format(const dcn::auth::AuthError::Kind & err, format_context& ctx) const {
        switch(err)
        {
            case dcn::auth::AuthError::Kind::MISSING_COOKIE : return formatter<string>::format("Missing cookie", ctx);
            case dcn::auth::AuthError::Kind::INVALID_COOKIE : return formatter<string>::format("Invalid cookie", ctx);
            case dcn::auth::AuthError::Kind::MISSING_TOKEN : return formatter<string>::format("Missing token", ctx);
            case dcn::auth::AuthError::Kind::INVALID_TOKEN : return formatter<string>::format("Invalid token", ctx);
            case dcn::auth::AuthError::Kind::INVALID_SIGNATURE : return formatter<string>::format("Invalid signature", ctx);
            case dcn::auth::AuthError::Kind::INVALID_NONCE : return formatter<string>::format("Invalid nonce", ctx);
            case dcn::auth::AuthError::Kind::INVALID_ADDRESS : return formatter<string>::format("Invalid address", ctx);

            default:  return formatter<string>::format("Unknown", ctx);
        }
        return formatter<string>::format("", ctx);
    }
};