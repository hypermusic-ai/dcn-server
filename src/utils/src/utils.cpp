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
        const auto now = std::chrono::system_clock::now();
        const auto now_time_t = std::chrono::system_clock::to_time_t(now);

        std::tm local_tm{};
        static std::mutex localtime_mutex;
        {
            std::lock_guard<std::mutex> lock(localtime_mutex);
            const auto* local_tm_ptr = std::localtime(&now_time_t);
            if (local_tm_ptr == nullptr)
            {
                throw std::runtime_error("Failed to convert current time");
            }
            local_tm = *local_tm_ptr;
        }

        return std::format("{:04d}-{:02d}-{:02d}-{:02d}_{:02d}_{:02d}",
            local_tm.tm_year + 1900,
            local_tm.tm_mon + 1,
            local_tm.tm_mday,
            local_tm.tm_hour,
            local_tm.tm_min,
            local_tm.tm_sec);
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
