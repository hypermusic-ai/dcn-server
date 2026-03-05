#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <expected>
#include <optional>
#include <string>

#include <nlohmann/json_fwd.hpp>

#include "chain_interface.hpp"

namespace dcn::sepolia
{
    struct BackendConfig
    {
        std::string rpc_url;
        std::string private_key_hex;

        std::uint64_t chain_id = 11155111; // Sepolia
        std::uint64_t gas_limit_fallback = 6'000'000;
        std::uint64_t fallback_max_priority_fee_wei = 2'000'000'000; // 2 gwei
        std::uint64_t receipt_poll_interval_ms = 1500;
        std::size_t max_receipt_polls = 120;
    };

    class SepoliaBackend final : public chain::IChain
    {
    public:
        explicit SepoliaBackend(BackendConfig cfg);

        const BackendConfig & config() const noexcept;

        std::expected<evmc::address, chain::DeployError> signerAddress() const override;

        std::expected<std::string, chain::DeployError> sendCreateTransaction(
            const std::vector<std::uint8_t> & init_code,
            std::optional<std::uint64_t> gas_limit = std::nullopt,
            std::uint64_t value_wei = 0) const override;

        std::expected<chain::DeployReceipt, chain::DeployError> deployContract(
            const std::vector<std::uint8_t> & init_code,
            std::optional<std::uint64_t> gas_limit = std::nullopt,
            std::uint64_t value_wei = 0) const override;

    private:
        std::expected<nlohmann::json, chain::DeployError> rpc(const std::string & method, nlohmann::json params) const;

        BackendConfig _cfg;
        std::array<std::uint8_t, 32> _private_key{};
        evmc::address _signer_address{};
        std::optional<chain::DeployError> _init_error;
    };
}
