#pragma once

#include <fstream>

#include "native.h"
#include <asio.hpp>
#include <spdlog/spdlog.h>

#include "pt.hpp"
#include "registry.hpp"
#include "parser.hpp"
#include "file.hpp"
#include "evm.hpp"

namespace dcn::loader
{
    asio::awaitable<std::expected<evm::Address, evm::DeployError>> deployParticle(evm::EVM & evm, registry::Registry & registry, ParticleRecord particle, const std::filesystem::path & storage_path);
    asio::awaitable<std::expected<evm::Address, evm::DeployError>> deployFeature(evm::EVM & evm, registry::Registry & registry, FeatureRecord feature, const std::filesystem::path & storage_path);
    asio::awaitable<std::expected<evm::Address, evm::DeployError>> deployTransformation(evm::EVM & evm, registry::Registry & registry, TransformationRecord transformation, const std::filesystem::path & storage_path);
    asio::awaitable<std::expected<evm::Address, evm::DeployError>> deployCondition(evm::EVM & evm, registry::Registry & registry, ConditionRecord condition, const std::filesystem::path & storage_path);


    asio::awaitable<bool> loadStoredParticles(evm::EVM & evm, registry::Registry & registry, const std::filesystem::path & storage_path);
    asio::awaitable<bool> loadStoredConditions(evm::EVM & evm, registry::Registry & registry, const std::filesystem::path & storage_path);
    asio::awaitable<bool> loadStoredTransformations(evm::EVM & evm, registry::Registry & registry, const std::filesystem::path & storage_path);
    asio::awaitable<bool> loadStoredFeatures(evm::EVM & evm, registry::Registry & registry, const std::filesystem::path & storage_path);
}