#pragma once

#include <atomic>
#include <chrono>

#include "async.hpp"

#include "wal_store.hpp"

namespace dcn::storage::sqlite
{
    class WalSyncWorker
    {
        public:
            WalSyncWorker(
                asio::io_context & io_context,
                IWalStore & store,
                std::chrono::milliseconds interval);

            asio::awaitable<void> run();
            void requestStop();

        private:
            asio::steady_timer _timer;
            IWalStore & _store;
            std::chrono::milliseconds _interval;
            std::atomic<bool> _stop_requested = false;
    };
}
