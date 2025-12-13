#include "utils.hpp"

namespace dcn::utils
{
    std::string loadBuildTimestamp(const std::filesystem::path & path) 
    {
        std::ifstream file(path);
        if (!file.is_open()) return "Unknown";
        std::string timestamp;
        std::getline(file, timestamp);
        return timestamp;
    }

    std::string currentTimestamp()
    {
        const auto zt{ std::chrono::zoned_time{
            std::chrono::current_zone(),
            std::chrono::time_point_cast<std::chrono::seconds>(std::chrono::system_clock::now())}
            };
        std::string ts = std::format("{:%F-%H_%M_%S}", zt);
        return ts;
    }

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

    bool equalsIgnoreCase(const std::string& a, const std::string& b) {
    return a.size() == b.size() &&
           std::equal(a.begin(), a.end(), b.begin(), [](char a, char b) {
               return std::tolower(static_cast<unsigned char>(a)) == std::tolower(static_cast<unsigned char>(b));
           });
    }

}