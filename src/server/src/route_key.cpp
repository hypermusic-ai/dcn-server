#include "route_key.hpp"

namespace dcn::server
{
    RouteKey::RouteKey(http::Method method, http::URL path)
    :   _method(method), 
        _url(std::move(path))
    {
        const std::vector<std::string> path_info_segments = http::splitPathSegments(_url.getPathInfo());

        bool optional_at_end = false;

        for(const auto & segment : path_info_segments)
        {
            auto arg_parse_result = parse::parseRouteArgDefFromString(segment);

            if(arg_parse_result)
            {
                if(optional_at_end == true && arg_parse_result->requirement == RouteArgRequirement::required)
                {
                    spdlog::error("RouteKey definition error - optional values MUST appear at the end of the path");
                    throw std::runtime_error("RouteKey definition error - optional values MUST appear at the end of the path");
                } 

                if(arg_parse_result->requirement == RouteArgRequirement::optional)optional_at_end = true;
                
                // argument
                _path_info_segments.emplace_back(std::move(*arg_parse_result));
            }
            else
            {
                if(optional_at_end == true)
                {
                    spdlog::error("RouteKey definition error - optional values MUST appear at the end of the path");
                    throw std::runtime_error("RouteKey definition error - optional values MUST appear at the end of the path");
                }
                
                // literal value
                _path_info_segments.emplace_back(segment);
            }
        }

        const auto query_segments = http::splitQuerySegments(_url.getQuery());

        for(const auto & [key, value] : query_segments)
        {
            auto arg_parse_result = parse::parseRouteArgDefFromString(value);

            if(arg_parse_result)
            {
                std::variant<std::string, dcn::server::RouteArgDef> value_var = std::move(arg_parse_result.value());
                _query_segments.try_emplace(std::move(key), std::move(value_var));
            }
            else
            {
                std::variant<std::string, dcn::server::RouteArgDef> value_var = std::move(value);
                _query_segments.try_emplace(std::move(key), std::move(value_var));
            }
        }
    }

    bool RouteKey::operator==(const RouteKey& other) const 
    {
        return _method == other._method && _url == other._url;
    }

    http::Method RouteKey::getMethod() const 
    {
         return _method;
    }

    const std::vector<std::variant<std::string, RouteArgDef>> & RouteKey::getPathInfoDef() const
    {
        return _path_info_segments;
    }

    const absl::flat_hash_map<std::string, std::variant<std::string, RouteArgDef>> & RouteKey::getQueryDef() const
    {
        return _query_segments;
    }

    const http::URL & RouteKey::getPath() const 
    {
         return _url; 
    }
}