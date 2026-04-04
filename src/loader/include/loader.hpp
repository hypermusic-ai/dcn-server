#pragma once

#include <cstddef>
#include <fstream>

#include "native.h"
#include <asio.hpp>
#include <spdlog/spdlog.h>

#include "pt.hpp"
#include "storage.hpp"
#include "parser.hpp"
#include "evm.hpp"
#include "chain.hpp"

namespace dcn::loader
{
    struct LoaderBatchConfig
    {
        std::size_t connectors = 1000;
        std::size_t transformations = 5000;
        std::size_t conditions = 5000;
    };

    bool ensurePTBuildVersion(const std::filesystem::path & storage_path);

    asio::awaitable<std::expected<chain::Address, pt::PTDeployError>> deployConnector(
        evm::EVM & evm,
        storage::Registry & registry,
        ConnectorRecord connector,
        const std::filesystem::path & storage_path,
        bool register_in_registry = true,
        bool persist_json = true);
    asio::awaitable<std::expected<chain::Address, pt::PTDeployError>> deployTransformation(
        evm::EVM & evm,
        storage::Registry & registry,
        TransformationRecord transformation,
        const std::filesystem::path & storage_path,
        bool register_in_registry = true,
        bool persist_json = true);
    asio::awaitable<std::expected<chain::Address, pt::PTDeployError>> deployCondition(
        evm::EVM & evm,
        storage::Registry & registry,
        ConditionRecord condition,
        const std::filesystem::path & storage_path,
        bool register_in_registry = true,
        bool persist_json = true);

    asio::awaitable<bool> importJsonStorageToDatabase(
        evm::EVM & evm,
        storage::Registry & registry,
        const std::filesystem::path & storage_path,
        LoaderBatchConfig batch_config = {});

    asio::awaitable<bool> ensureTransformationDeployed(
        evm::EVM & evm,
        storage::Registry & registry,
        const std::string & name,
        const std::filesystem::path & storage_path);
    asio::awaitable<bool> ensureConditionDeployed(
        evm::EVM & evm,
        storage::Registry & registry,
        const std::string & name,
        const std::filesystem::path & storage_path);
    asio::awaitable<bool> ensureConnectorDeployed(
        evm::EVM & evm,
        storage::Registry & registry,
        const std::string & name,
        const std::filesystem::path & storage_path);


    asio::awaitable<bool> loadStoredConnectors(
        evm::EVM & evm,
        storage::Registry & registry,
        const std::filesystem::path & storage_path,
        std::size_t registry_batch_size = 1000);
    asio::awaitable<bool> loadStoredConditions(
        evm::EVM & evm,
        storage::Registry & registry,
        const std::filesystem::path & storage_path,
        std::size_t registry_batch_size = 5000);
    asio::awaitable<bool> loadStoredTransformations(
        evm::EVM & evm,
        storage::Registry & registry,
        const std::filesystem::path & storage_path,
        std::size_t registry_batch_size = 5000);
}
