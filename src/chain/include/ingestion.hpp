#pragma once

#include <cstddef>
#include <cstdint>
#include <functional>
#include <filesystem>
#include <optional>
#include <string>

#include <asio.hpp>
#include <nlohmann/json_fwd.hpp>

#include "address.hpp"
#include "crypto.hpp"

namespace dcn::chain
{
    using RpcCall = std::function<std::optional<nlohmann::json>(const std::string & rpc_url, const nlohmann::json & request)>;

    struct IngestionConfig
    {
        bool enabled = false;

        std::string rpc_url;
        chain::Address registry_address{};

        std::optional<std::uint64_t> start_block;
        std::uint64_t poll_interval_ms = 5000;
        std::uint64_t confirmations = 12;
        std::uint64_t block_batch_size = 500;

        std::filesystem::path storage_path;
    };

    struct IngestionRuntimeOptions
    {
        RpcCall rpc_call = {};
        std::optional<std::size_t> max_polls = std::nullopt;
        bool skip_sleep = false;
    };

    //asio::awaitable<void> runEventIngestion(IngestionConfig cfg, registry::Registry & registry, IngestionRuntimeOptions runtime_options);
    //asio::awaitable<void> runEventIngestion(IngestionConfig cfg, registry::Registry & registry);
}
