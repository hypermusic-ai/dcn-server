#pragma once

#include <vector>
#include <string>
#include <variant>

#include <absl/container/flat_hash_map.h>

#include "http.hpp"
#include "route_arg.hpp"

namespace dcn::server
{
    /**
     * @brief A class representing a route key, which is a combination of a HTTP method and a URL.
     * 
     * This class is used to identify a specific route in the server's routing table.
     */
    class RouteKey 
    {
        public:
            RouteKey(http::Method method, http::URL path);
        
            bool operator==(const RouteKey& other) const;
            
            /**
             * @brief Get the HTTP method of the route key.
             * 
             * @return The HTTP method.
             */
            http::Method getMethod() const;

            /**
             * @brief Get the URL of the route key.
             * 
             * @return The URL.
             */
            const http::URL & getPath() const;

            /**
             * @brief Get the path info segments of the route key.
             * 
             * @return The path info segments.
             */
            const std::vector<std::variant<std::string, RouteArgDef>> & getPathInfoDef() const;

            const absl::flat_hash_map<std::string, std::variant<std::string, RouteArgDef>> & getQueryDef() const;

        private:
            http::Method _method;
            http::URL _url;

            std::vector<std::variant<std::string, RouteArgDef>> _path_info_segments;
            absl::flat_hash_map<std::string, std::variant<std::string, RouteArgDef>> _query_segments;
    };

    /**
     * @brief Combines hash values for a RouteKey object.
     *
     * @tparam H The hash state type.
     * @param h The initial hash state.
     * @param route_key The RouteKey object whose attributes will be hashed.
     * @return A combined hash state incorporating the HTTP method and path of the RouteKey.
     */
    template <typename H>
    inline H AbslHashValue(H h, const RouteKey& route_key) {
        return H::combine(std::move(h), route_key.getMethod(), route_key.getPath());
    }

}