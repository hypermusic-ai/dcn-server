#pragma once

#include <cstdint>
#include <expected>
#include <format>
#include <optional>
#include <string>
#include <vector>

#ifdef interface
    #undef interface
#endif
#include <evmc/evmc.hpp>
#ifndef interface
    #define interface __STRUCT__
#endif

namespace dcn::chain
{
    struct DeployError
    {
        enum class Kind : std::uint8_t
        {
            UNKNOWN = 0,
            INVALID_CONFIG,
            INVALID_INPUT,
            RPC_ERROR,
            RPC_MALFORMED,
            SIGNING_ERROR,
            TRANSACTION_REVERTED,
            TIMEOUT
        } kind = Kind::UNKNOWN;

        std::string message;
        std::vector<std::uint8_t> result_bytes;
    };

    struct DeployReceipt
    {
        std::string tx_hash;
        evmc::address signer_address{};
        evmc::address contract_address{};
        std::string block_number_hex;
        std::string gas_used_hex;
    };
}

template <>
struct std::formatter<dcn::chain::DeployError::Kind> : std::formatter<std::string>
{
    auto format(const dcn::chain::DeployError::Kind & err, format_context & ctx) const
    {
        switch(err)
        {
            case dcn::chain::DeployError::Kind::INVALID_CONFIG:
                return formatter<string>::format("Invalid config", ctx);
            case dcn::chain::DeployError::Kind::INVALID_INPUT:
                return formatter<string>::format("Invalid input", ctx);
            case dcn::chain::DeployError::Kind::RPC_ERROR:
                return formatter<string>::format("RPC error", ctx);
            case dcn::chain::DeployError::Kind::RPC_MALFORMED:
                return formatter<string>::format("Malformed RPC response", ctx);
            case dcn::chain::DeployError::Kind::SIGNING_ERROR:
                return formatter<string>::format("Signing error", ctx);
            case dcn::chain::DeployError::Kind::TRANSACTION_REVERTED:
                return formatter<string>::format("Transaction reverted", ctx);
            case dcn::chain::DeployError::Kind::TIMEOUT:
                return formatter<string>::format("Timeout", ctx);
            default:
                return formatter<string>::format("Unknown", ctx);
        }
    }
};
