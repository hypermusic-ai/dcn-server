#pragma once

#include <sstream>
#include <cstdlib>
#include <cstdint>
#include <ctime>
#include <iomanip>
#include <chrono>
#include <format>
#include <stdexcept>
#include <string>
#include <fstream>
#include <filesystem>
#include <functional>
#include <mutex>

#include "native.h"
#include <asio.hpp>

#include "logo.hpp"
#include "math.hpp"
#include "decode_abi.hpp"

#include <spdlog/spdlog.h>
#include <absl/container/flat_hash_map.h>
#include <absl/container/flat_hash_set.h>

#define STRINGIFY(x) #x
#define TOSTRING(x) STRINGIFY(x)

namespace dcn::utils
{
    std::string loadBuildTimestamp(const std::filesystem::path & path);
    
    std::string currentTimestamp();

    /**
     * @brief Suspends the coroutine until the given deadline is reached
     * 
     * @param deadline The point in time when the coroutine should be resumed
     * 
     * This function is used to implement a watchdog like behavior in the
     * server. It is used for example in the listening function to periodically check if
     * the server should be shut down.
     */
    asio::awaitable<void> watchdog(std::chrono::steady_clock::time_point& deadline);

    asio::awaitable<void> ensureOnStrand(const asio::strand<asio::io_context::executor_type> & strand);

    template<class DataT, class ChildT, template<typename...> class ChildListT>
    std::vector<std::string> topologicalSort(   
        const absl::flat_hash_map<std::string, DataT> & data,
        std::function<ChildListT<ChildT>(const DataT &)> get_childreen,
        std::function<std::string(const ChildT &)> get_child_name)
    {
        std::vector<std::string> sorted;
        absl::flat_hash_set<std::string> visited;
        absl::flat_hash_set<std::string> on_stack;

        std::function<void(const std::string&)> visit = [&](const std::string& name)  {
            if (visited.contains(name)) return;
            if (on_stack.contains(name)) {
                spdlog::error(std::format("Cycle detected involving : {}", name));
                throw std::runtime_error("Cyclic dependency");
            }

            on_stack.insert(name);

            auto it = data.find(name);
            if (it == data.end()) {
                spdlog::warn(std::format("Missing dependency: {}", name));
                on_stack.erase(name);
                return;
            }

            std::string child_name;
            for (const auto& child : get_childreen(it->second)) 
            {
                child_name = get_child_name(child);
                if(child_name.empty()) continue;
                visit(child_name);
            }

            visited.insert(name);
            on_stack.erase(name);
            sorted.push_back(name);
        };

        // perform the sort
        for (const auto& [name, _] : data) {
            visit(name);
        }

        return sorted;
    }

    bool equalsIgnoreCase(const std::string& a, const std::string& b);
}
