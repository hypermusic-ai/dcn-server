#include "async.hpp"

namespace dcn::async
{
    asio::awaitable<void> watchdog(std::chrono::steady_clock::time_point& deadline)
    {
        asio::steady_timer timer(co_await asio::this_coro::executor);
        auto now = std::chrono::steady_clock::now();
        while (deadline > now)
        {
            timer.expires_at(deadline);
            co_await timer.async_wait(asio::use_awaitable);
            now = std::chrono::steady_clock::now();
        }
        co_return;
    }

    asio::awaitable<void> ensureOnStrand(const asio::strand<asio::io_context::executor_type> & strand)
    {
        if (strand.running_in_this_thread())
        {
            co_return;
        }
        co_return co_await asio::dispatch(strand, asio::use_awaitable);
    }
}