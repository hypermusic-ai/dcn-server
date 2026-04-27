#pragma once

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <string>
#include <utility>

#include <nlohmann/json.hpp>

namespace dcn::events
{
    class RpcClient final
    {
        public:
            RpcClient(
                std::string rpc_url,
                unsigned int rpc_timeout_ms);
            ~RpcClient();

            RpcClient(const RpcClient &) = delete;
            RpcClient & operator=(const RpcClient &) = delete;
            RpcClient(RpcClient &&) = delete;
            RpcClient & operator=(RpcClient &&) = delete;

            void stop();
            std::uint64_t callCount() const;
            nlohmann::json callSync(
                const std::string & method,
                nlohmann::json params) const;

        private:
            static nlohmann::json _callBlocking(
                const std::string & rpc_url,
                const std::string & method_name,
                unsigned int timeout_seconds,
                nlohmann::json request);

        private:
            const std::string _rpc_url;
            const unsigned int _rpc_timeout_ms;

            mutable std::atomic<std::uint64_t> _call_count{0};
    };
}
