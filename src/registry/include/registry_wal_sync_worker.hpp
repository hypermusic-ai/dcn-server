#pragma once

#include <atomic>
#include <chrono>

#include "native.h"
#include <asio.hpp>

namespace dcn::registry
{
    class Registry;

    class RegistryWalSyncWorker
    {
        public:
            RegistryWalSyncWorker(
                asio::io_context & io_context,
                Registry & registry,
                std::chrono::milliseconds interval);

            asio::awaitable<void> run();
            void requestStop();

        private:
            asio::steady_timer _timer;
            Registry & _registry;
            std::chrono::milliseconds _interval;
            std::atomic<bool> _stop_requested = false;
    };
}
