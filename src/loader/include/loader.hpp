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

namespace dcn
{
    asio::awaitable<std::expected<evm::Address, evm::DeployError>> deployFeature(evm::EVM & evm, registry::Registry & registry, FeatureRecord feature);
    asio::awaitable<std::expected<evm::Address, evm::DeployError>> deployTransformation(evm::EVM & evm, registry::Registry & registry, TransformationRecord transformation);

    asio::awaitable<bool> loadStoredTransformations(evm::EVM & evm, registry::Registry & registry);

    asio::awaitable<bool> loadStoredFeatures(evm::EVM & evm, registry::Registry & registry);
}