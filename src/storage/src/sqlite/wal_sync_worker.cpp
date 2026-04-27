#include <system_error>

#include <spdlog/spdlog.h>

#include "sqlite/wal_sync_worker.hpp"

namespace dcn::storage::sqlite
{
    WalSyncWorker::WalSyncWorker(
        asio::io_context & io_context,
        IWalStore & store,
        const std::chrono::milliseconds interval)
        : _timer(io_context)
        , _store(store)
        , _interval(interval)
    {
    }

    asio::awaitable<void> WalSyncWorker::run()
    {
        while(!_stop_requested.load(std::memory_order_acquire))
        {
            _timer.expires_after(_interval);
            std::error_code wait_ec;
            co_await _timer.async_wait(asio::redirect_error(asio::use_awaitable, wait_ec));

            if(wait_ec == asio::error::operation_aborted)
            {
                continue;
            }

            if(wait_ec)
            {
                spdlog::warn("WAL sync worker timer failed: {}", wait_ec.message());
                continue;
            }

            const auto stats = co_await _store.checkpointWal(storage::sqlite::WalCheckpointMode::PASSIVE);
            if(!stats.ok)
            {
                spdlog::warn("WAL passive checkpoint failed");
            }
            else
            {
                spdlog::debug("WAL passive checkpoint ok");
            }
        }

        spdlog::debug("WAL sync worker stopped");
        co_return;
    }

    void WalSyncWorker::requestStop()
    {
        _stop_requested.store(true, std::memory_order_release);
        (void)_timer.cancel();
    }
}
