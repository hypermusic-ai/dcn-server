#pragma once

#include <optional>
#include <expected>
#include <string>
#include <vector>

#include "deploy.hpp"
#include "address.hpp"

namespace dcn::chain
{
    class IChain
    {
    public:
        virtual ~IChain() = default;

        virtual std::expected<Address, DeployError> signerAddress() const = 0;

        virtual std::expected<std::string, DeployError> sendCreateTransaction(
            const std::vector<std::uint8_t> & init_code,
            std::optional<std::uint64_t> gas_limit = std::nullopt,
            std::uint64_t value_wei = 0) const = 0;

        virtual std::expected<DeployReceipt, DeployError> deployContract(
            const std::vector<std::uint8_t> & init_code,
            std::optional<std::uint64_t> gas_limit = std::nullopt,
            std::uint64_t value_wei = 0) const = 0;
    };
}