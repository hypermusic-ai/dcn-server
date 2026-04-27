#include "rpc_client.hpp"

#include <exception>
#include <vector>

#include <spdlog/spdlog.h>

#include "native.h"

namespace dcn::events
{
    RpcClient::RpcClient(
        std::string rpc_url,
        const unsigned int rpc_timeout_ms)
        : _rpc_url(std::move(rpc_url))
        , _rpc_timeout_ms(rpc_timeout_ms)
    {
    }

    RpcClient::~RpcClient()
    {
        stop();
    }

    void RpcClient::stop()
    {
        // Synchronous transport currently has no background workers to stop.
    }

    std::uint64_t RpcClient::callCount() const
    {
        return _call_count.load(std::memory_order_acquire);
    }

    nlohmann::json RpcClient::callSync(
        const std::string & method,
        nlohmann::json params) const
    {
        _call_count.fetch_add(1, std::memory_order_acq_rel);
        if(_rpc_url.empty())
        {
            return nlohmann::json{};
        }

        nlohmann::json request{
            {"jsonrpc", "2.0"},
            {"id", 1},
            {"method", method},
            {"params", std::move(params)}
        };

        const auto rpc_url = _rpc_url;
        const auto method_name = std::string(method);
        const unsigned int timeout_seconds =
            std::max<unsigned int>(1u, (_rpc_timeout_ms + 999u) / 1000u);

        return _callBlocking(rpc_url, method_name, timeout_seconds, std::move(request));
    }

    nlohmann::json RpcClient::_callBlocking(
        const std::string & rpc_url,
        const std::string & method_name,
        const unsigned int timeout_seconds,
        nlohmann::json request)
    {
        try
        {
            std::vector<std::string> args{
                "-sS",
                "--max-time",
                std::to_string(timeout_seconds),
                "-X",
                "POST",
                rpc_url,
                "-H",
                "Content-Type: application/json",
                "--data",
                request.dump()
            };

            const auto [exit_code, output] = native::runProcess("curl", std::move(args));
            if(exit_code != 0)
            {
                spdlog::warn(
                    "Events RPC call failed for method='{}' (exit={}): {}",
                    method_name,
                    exit_code,
                    output);
                return nlohmann::json{};
            }

            const nlohmann::json response = nlohmann::json::parse(output, nullptr, false);
            if(response.is_discarded())
            {
                spdlog::warn("Events RPC response parse failed for method='{}'", method_name);
                return nlohmann::json{};
            }

            if(response.contains("error"))
            {
                spdlog::warn(
                    "Events RPC response error for method='{}': {}",
                    method_name,
                    response.at("error").dump());
                return nlohmann::json{};
            }

            if(!response.contains("result"))
            {
                spdlog::warn(
                    "Events RPC response missing result for method='{}'",
                    method_name);
                return nlohmann::json{};
            }

            return response.at("result");
        }
        catch(const std::exception & e)
        {
            spdlog::warn("Events RPC transport failed for method='{}': {}", method_name, e.what());
            return nlohmann::json{};
        }
    }
}
