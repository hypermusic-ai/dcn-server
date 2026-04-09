#pragma once

#include <cstddef>
#include <cstdint>
#include <functional>
#include <filesystem>
#include <optional>
#include <string>

#include "native.h"
#include <asio.hpp>
#include <nlohmann/json_fwd.hpp>

#include "address.hpp"
#include "crypto.hpp"

namespace dcn::chain
{
    using RpcCall = std::function<std::optional<nlohmann::json>(const std::string & rpc_url, const nlohmann::json & request)>;

    struct IngestionRuntimeOptions
    {
        RpcCall rpc_call = {};
        std::optional<std::size_t> max_polls = std::nullopt;
        bool skip_sleep = false;
    };

    //asio::awaitable<void> runEventIngestion(IngestionConfig cfg, storage::Registry & registry, IngestionRuntimeOptions runtime_options);
    //asio::awaitable<void> runEventIngestion(IngestionConfig cfg, storage::Registry & registry);
}
