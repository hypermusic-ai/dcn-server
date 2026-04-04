#include "registry_wal_sync_worker.hpp"

#include <system_error>

#include <spdlog/spdlog.h>

#include "registry.hpp"

namespace dcn::storage
{
    RegistryWalSyncWorker::RegistryWalSyncWorker(
        asio::io_context & io_context,
        Registry & registry,
        const std::chrono::milliseconds interval)
        : _timer(io_context)
        , _registry(registry)
        , _interval(interval)
    {
    }

    asio::awaitable<void> RegistryWalSyncWorker::run()
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
                spdlog::warn("Registry WAL sync worker timer failed: {}", wait_ec.message());
                continue;
            }

            const bool checkpoint_ok = co_await _registry.checkpointWal(WalCheckpointMode::PASSIVE);
            if(!checkpoint_ok)
            {
                spdlog::warn("Registry WAL passive checkpoint failed");
            }
            else
            {
                spdlog::debug("Registry WAL passive checkpoint ok");
            }
        }

        spdlog::debug("Registry WAL sync worker stopped");
        co_return;
    }

    void RegistryWalSyncWorker::requestStop()
    {
        _stop_requested.store(true, std::memory_order_release);
        (void)_timer.cancel();
    }
}
