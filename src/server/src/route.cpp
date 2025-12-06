#include "route.hpp"

namespace dcn::server
{
    std::tuple<bool, std::vector<RouteArg>, QueryArgsList> Router::doesRouteMatch(
            const RouteKey & route,
            const http::Method & request_method,
            const std::string & request_module_path,
            const std::vector<std::string> & request_path_info_segments,
            const absl::flat_hash_map<std::string, std::string> request_query_segments) const
    {
        // if method does not match
        if(route.getMethod() != request_method)
        {
            return {false, std::vector<RouteArg>(), QueryArgsList()};
        }

        // if module path does not match
        if(route.getPath().getPathModule() != request_module_path)
        {
            return {false, std::vector<RouteArg>(), QueryArgsList()};
        }

        const auto & path_def = route.getPathInfoDef();
        std::size_t path_segments_index = 0;
        std::vector<RouteArg> found_path_args;

        for(const auto & path_def_segment : path_def)
        {
            if(std::holds_alternative<std::string>(path_def_segment))
            {
                // if request path info segments does not have enough segments
                if(path_segments_index >= request_path_info_segments.size())
                {
                    return {false, std::vector<RouteArg>(), QueryArgsList()};
                }

                // if literal value does not match
                if(std::get<std::string>(path_def_segment) != request_path_info_segments.at(path_segments_index))
                {
                    return {false, std::vector<RouteArg>(), QueryArgsList()};
                }
                
                // consume path info segment
                path_segments_index++;

            }else if(std::holds_alternative<RouteArgDef>(path_def_segment))
            {
                const auto & arg_def = std::get<RouteArgDef>(path_def_segment);
                
                // if argument is required but we cannot consume more path segments
                // then the path does not match
                if(arg_def.requirement == RouteArgRequirement::required && path_segments_index >= request_path_info_segments.size())
                {
                    return {false, std::vector<RouteArg>(), QueryArgsList()};
                }

                // if argument is optional but we cannot consume more path segments
                // then skip the argument
                if(arg_def.requirement == RouteArgRequirement::optional && path_segments_index >= request_path_info_segments.size())
                {
                    continue;
                }
                
                // consume path info segment
                found_path_args.emplace_back(arg_def, request_path_info_segments.at(path_segments_index));
                path_segments_index++;
            }else 
            {
                spdlog::error("RouteKey definition error - path segment is not a literal or an argument definition");
                return {false, std::vector<RouteArg>(), QueryArgsList()};
            }
        }

        // if we have more path segments than the route definition
        if(path_segments_index < request_path_info_segments.size())
        {
            return {false, std::vector<RouteArg>(), QueryArgsList()};
        }

        // --- QUERY ARG MATCHING ---
        QueryArgsList found_query_args;

        for (const auto & def_pair : route.getQueryDef()) {
            const std::string & key = def_pair.first;
            const auto & value_def = def_pair.second;

            auto it = request_query_segments.find(key);
            bool found_in_request = (it != request_query_segments.end());

            if (std::holds_alternative<std::string>(value_def)) {
                const std::string & expected_literal = std::get<std::string>(value_def);

                if (!found_in_request || it->second != expected_literal) {
                    return {false, {}, {}};
                }

                found_query_args.emplace(key, RouteArg(RouteArgDef(RouteArgType::string, RouteArgRequirement::required), expected_literal));
            }
            else if (std::holds_alternative<RouteArgDef>(value_def)) {
                const RouteArgDef & arg_def = std::get<RouteArgDef>(value_def);

                if (arg_def.requirement == RouteArgRequirement::required) {
                    if (!found_in_request) {
                        return {false, {}, {}};
                    }
                    found_query_args.emplace(key, RouteArg(arg_def, it->second));
                }
                else if (arg_def.requirement == RouteArgRequirement::optional) {
                    if (found_in_request) {
                        found_query_args.emplace(key, RouteArg(arg_def, it->second));
                    }
                    // If not found, just skip — it's optional
                }
            }
        }

        return {true, found_path_args, found_query_args};
    }


    std::tuple<const RouteHandlerFunc *, std::vector<RouteArg>, QueryArgsList> Router::findRoute(const http::Request & request) const
    {
        const http::Method request_method = request.getMethod();
        const std::string request_module_path = request.getPath().getPathModule();

        const std::vector<std::string> request_path_info_segments = http::splitPathSegments(request.getPath().getPathInfo());
        const absl::flat_hash_map<std::string, std::string> request_query_segments = http::splitQuerySegments(request.getPath().getQuery());


        for(const auto & [route_key, route_handler] : _routes)
        {
            // check if route matches
            const auto [match, args, query_args] = doesRouteMatch(  
                route_key,
                request_method,
                request_module_path,
                request_path_info_segments,
                request_query_segments);
            
            if(!match)
            {
                continue;
            }

            spdlog::debug(std::format("Route for request path {} found: {} {}", request.getPath(), route_key.getMethod(), route_key.getPath()));
            return {&route_handler, args, query_args};
        }

        return std::make_tuple(nullptr, std::vector<RouteArg>(), QueryArgsList());
    }

    void Router::addRoute(RouteKey route, RouteHandlerFunc handler)
    {
        _routes.try_emplace(std::move(route), std::move(handler));
    }
}