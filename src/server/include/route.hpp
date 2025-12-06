#pragma once
#include <string>
#include <functional>
using namespace std::placeholders;

#include <utility>
#include <regex>
#include <vector>
#include <cassert>

#include "native.h"
#include <asio.hpp>
#include <absl/hash/hash.h>
#include <absl/container/flat_hash_map.h>

#include "http.hpp"

#include "route_arg.hpp"
#include "route_key.hpp"

namespace dcn::server
{
    using QueryArgsList = absl::flat_hash_map<std::string, RouteArg>;

    /**
     * @brief A class representing a route handler function.
     * 
     * This class is used to store and execute route handler function.
     */
    class RouteHandlerFunc
    {
        private:
            class Base
            {
                public:
                    virtual ~Base() = default;
            };

            template<typename... Args>
            struct Wrapper : public Base
            {
                Wrapper(std::function<asio::awaitable<http::Response>(Args...)> func) 
                : function(std::move(func)) 
                {}

                std::function<asio::awaitable<http::Response>(Args...)> function; 
            };

        public:

            template<typename... Args>
            RouteHandlerFunc(std::function<asio::awaitable<http::Response>(Args...)> func) 
            : _base(std::make_unique<Wrapper<Args...>>(std::move(func))) 
            {}

            RouteHandlerFunc(RouteHandlerFunc && other) = default;

            template<typename... Args>
            asio::awaitable<http::Response> operator()(Args && ... args) const
            {
                Wrapper<Args...>* wrapper_ptr = dynamic_cast<Wrapper<Args...>*>(_base.get());

                if(wrapper_ptr)
                {
                    co_return co_await wrapper_ptr->function(std::forward<Args>(args)...);
                }
                else 
                {
                    throw std::runtime_error("Invalid arguments to function object call!");
                }
            }

        private:
            std::unique_ptr<Base> _base;
    };

    /**
     * @brief A class representing a router for handling HTTP requests.
     * 
     * This class is used to store and execute set of route handler functions.
     */
    class Router
    {
        public:
            Router() = default;
            ~Router() = default;
            
            void addRoute(RouteKey route, RouteHandlerFunc handler);

            std::tuple<const RouteHandlerFunc *, std::vector<RouteArg>,  QueryArgsList> findRoute(const http::Request & request) const;
        protected:
            std::tuple<bool, std::vector<RouteArg>, QueryArgsList> doesRouteMatch(
                    const RouteKey & route,
                    const http::Method & request_method,
                    const std::string & module_path,
                    const std::vector<std::string> & request_path_info_segments,
                    const absl::flat_hash_map<std::string, std::string> request_query_segments) const;

        private:
            absl::flat_hash_map<RouteKey, RouteHandlerFunc> _routes;
    };
}
