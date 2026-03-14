#pragma once

#include <fstream>

#include "native.h"
#include <asio.hpp>
#include <spdlog/spdlog.h>

#include "pt.hpp"
#include "registry.hpp"
#include "parser.hpp"
#include "evm.hpp"
#include "chain.hpp"

namespace dcn::loader
{
    bool ensurePTBuildVersion(const std::filesystem::path & storage_path);

    asio::awaitable<std::expected<chain::Address, pt::PTDeployError>> deployConnector(evm::EVM & evm, registry::Registry & registry, ConnectorRecord connector, const std::filesystem::path & storage_path);
    asio::awaitable<std::expected<chain::Address, pt::PTDeployError>> deployTransformation(evm::EVM & evm, registry::Registry & registry, TransformationRecord transformation, const std::filesystem::path & storage_path);
    asio::awaitable<std::expected<chain::Address, pt::PTDeployError>> deployCondition(evm::EVM & evm, registry::Registry & registry, ConditionRecord condition, const std::filesystem::path & storage_path);


    asio::awaitable<bool> loadStoredConnectors(evm::EVM & evm, registry::Registry & registry, const std::filesystem::path & storage_path);
    asio::awaitable<bool> loadStoredConditions(evm::EVM & evm, registry::Registry & registry, const std::filesystem::path & storage_path);
    asio::awaitable<bool> loadStoredTransformations(evm::EVM & evm, registry::Registry & registry, const std::filesystem::path & storage_path);
}
