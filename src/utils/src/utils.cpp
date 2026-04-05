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

    bool equalsIgnoreCase(const std::string& a, const std::string& b) {
    return a.size() == b.size() &&
           std::equal(a.begin(), a.end(), b.begin(), [](char a, char b) {
               return std::tolower(static_cast<unsigned char>(a)) == std::tolower(static_cast<unsigned char>(b));
           });
    }


    void logException(const std::exception_ptr & exception_ptr, const std::string_view context)
    {
        if(!exception_ptr)
        {
            return;
        }

        try
        {
            std::rethrow_exception(exception_ptr);
        }
        catch(const std::exception & e)
        {
            spdlog::error("{}: {}", context, e.what());
        }
        catch(...)
        {
            spdlog::error("{}: unknown exception", context);
        }
    }


    bool isImportTraceEnabled()
    {
        const char * env_value = std::getenv("DECENTRALISED_ART_IMPORT_TRACE");
        if(env_value == nullptr)
        {
            return false;
        }

        const std::string_view flag(env_value);
        return !(flag.empty() ||
            flag == "0" ||
            flag == "false" ||
            flag == "FALSE" ||
            flag == "off" ||
            flag == "OFF");
    }
}
