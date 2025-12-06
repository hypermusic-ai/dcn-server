#include "route_arg.hpp"

namespace dcn
{
    RouteArg::RouteArg(RouteArgDef def, std::string data)
    :   _def(std::move(def)),
        _data(std::move(data))
    {
        
    }

    RouteArgType RouteArg::getType() const
    {
        return _def.type;
    }

    const std::string & RouteArg::getData() const
    {
        return _data;
    }

    RouteArgRequirement RouteArg::getRequirement() const
    {
        return _def.requirement;
    }

    const std::vector<std::unique_ptr<RouteArgDef>> & RouteArg::getChildren() const
    {
        return _def.children;
    }
}

namespace dcn::parse
{
    RouteArgType parseRouteArgTypeFromString(const std::string & str)
    {
        if(str == std::format("{}", RouteArgType::character)) return RouteArgType::character;
        if(str == std::format("{}", RouteArgType::unsigned_integer)) return RouteArgType::unsigned_integer;
        if(str == std::format("{}", RouteArgType::base58)) return RouteArgType::base58;
        if(str == std::format("{}", RouteArgType::string)) return RouteArgType::string;
        if(str == std::format("{}", RouteArgType::array)) return RouteArgType::array;
        if(str == std::format("{}", RouteArgType::object)) return RouteArgType::object;

        return RouteArgType::Unknown;
    }

    std::optional<RouteArgDef> parseRouteArgDefFromString(const std::string str)
    {   
        constexpr static const char start_delimeter = '<';
        constexpr static const char end_delimeter = '>';
        constexpr static const char optional_identifier = '~';
        
        const auto it_start = str.find(start_delimeter);
        if (it_start == std::string::npos) return std::nullopt;
         
        const auto it_end = str.rfind(end_delimeter);
        if (it_end == std::string::npos)return std::nullopt;
        
        assert(it_start < it_end);
        std::string arg = str.substr(it_start + 1, it_end - it_start - 1);
         
        if(arg.size() == 0)return std::nullopt;
            
        RouteArgRequirement requirement = RouteArgRequirement::required;
        RouteArgType type;
        
        if(arg.front() == optional_identifier)
        {
            // optional value
            requirement = RouteArgRequirement::optional;
            // remove optional identifier
            arg.erase(0, 1);
        }
        else
        {
            // required value
            requirement = RouteArgRequirement::required;
        }
        
        std::vector<std::unique_ptr<RouteArgDef>> additional_fields{};

        const auto it_array_start = arg.find(ARRAY_START_IDENTIFIER);
        const auto it_object_start = arg.find(OBJECT_START_IDENTIFIER);
        
        if(it_array_start != std::string::npos)
        {
            const auto it_array_end = arg.rfind(ARRAY_END_IDENTIFIER);
            if(it_array_end == std::string::npos)
            {
                spdlog::error("parseRouteArg - cannot find array end identifier : {}", arg);
                return std::nullopt;
            }

            // array type
            type = RouteArgType::array;
            // remove array identifiers
            arg = arg.substr(it_array_start + 1, it_array_end - it_array_start - 1);
            
            // if its an array we need to fetch its type <[...]>
            const auto array_type = parseRouteArgDefFromString(arg);
            if(array_type == std::nullopt)
            {
                spdlog::error("parseRouteArg - cannot find array type definition : {}", arg);
                return std::nullopt;
            }

            if(array_type->requirement == RouteArgRequirement::optional)
            {
                spdlog::error("parseRouteArg - array type cannot be optional : {}", arg);
                return std::nullopt;
            }

            additional_fields.emplace_back(std::make_unique<RouteArgDef>(std::move(*array_type)));
        }
        else if(it_object_start != std::string::npos)
        {
            const auto it_object_end = arg.rfind(OBJECT_END_IDENTIFIER);
            if(it_object_end == std::string::npos)
            {
                spdlog::error("parseRouteArg - cannot find object end identifier : {}", arg);
                return std::nullopt;
            }

            // object type
            type = RouteArgType::object;

            // remove object identifiers
            arg = arg.substr(it_object_start + 1, it_object_end - it_object_start - 1);

            // if its an object we need to fetch its fields <(...)>
            std::string & object_fields = arg;
            if(object_fields.empty())
            {
                spdlog::error("parseRouteArg - cannot find object fields : {}", arg);
                return std::nullopt;
            }

            // we need to split the object fields by comma
            std::vector<std::string> fields;
            std::size_t pos = 0;
            while ((pos = object_fields.find(OBJECT_FIELDS_DELIMETER)) != std::string::npos) {
                fields.push_back(object_fields.substr(0, pos));
                object_fields.erase(0, pos + 1);
            }
            fields.push_back(object_fields); // add the last field
            
            if(fields.size() == 0)
            {
                spdlog::error("parseRouteArg - cannot find object fields : {}", arg);
                return std::nullopt;
            }

            // for every field we need to parse it
            for(const auto & field : fields)
            {
                const auto field_type = parseRouteArgDefFromString(field);
                if(field_type == std::nullopt)
                {
                    spdlog::error("parseRouteArg - cannot find object field type definition : {}", field);
                    return std::nullopt;
                }

                if(field_type->requirement == RouteArgRequirement::optional)
                {
                    spdlog::error("parseRouteArg - object field type cannot be optional : {}", arg);
                    return std::nullopt;
                }

                additional_fields.emplace_back(std::make_unique<RouteArgDef>(std::move(*field_type)));
            }
        }
        else 
        {
            // normal type
            type = parseRouteArgTypeFromString(arg);
        }

        if(type == RouteArgType::Unknown)
        {
            spdlog::error("parseRouteArg - cannot find type definition : {}", arg);
            return std::nullopt;
        }

        return std::make_optional(RouteArgDef(type, requirement, std::move(additional_fields))); 
    }

    template<>
    std::optional<std::size_t> parseRouteArgAs<std::size_t>(const RouteArg & arg) 
    {
        if(arg.getType() != RouteArgType::unsigned_integer)return std::nullopt;
        std::size_t val;
        try{
            val = std::stoull(arg.getData());
        }
        catch(const std::out_of_range & e)
        {
            return std::nullopt;
        }
        catch(const std::invalid_argument & e)
        {
            return std::nullopt;
        }
        catch(...)
        {
            return std::nullopt;
        }
        return val;
    }


    template<>
    std::optional<std::uint32_t> parseRouteArgAs<std::uint32_t>(const RouteArg & arg) 
    {
        if(arg.getType() != RouteArgType::unsigned_integer)return std::nullopt;

        try {
            std::size_t pos = 0;
            unsigned long long parsed = std::stoull(arg.getData(), &pos);

            // Ensure the whole string was parsed and the value fits in uint32_t
            if (pos != arg.getData().size() || parsed > std::numeric_limits<uint32_t>::max())
                return std::nullopt;

            return static_cast<uint32_t>(parsed);
        } catch (...) {
            return std::nullopt;
        }
    }

    template<>
    std::optional<std::string> parseRouteArgAs<std::string>(const RouteArg & arg) 
    {
        if(arg.getType() != RouteArgType::string)return std::nullopt;
        return arg.getData();
    }

    // template<>
    // std::optional<std::vector<std::uint32_t>> parseRouteArgAs<std::vector<std::uint32_t>>(const RouteArg& arg)
    // {
    //     if (arg.getType() != RouteArgType::unsigned_integer)
    //         return std::nullopt;
    
    //     const std::string& data = arg.getData();
    //     std::vector<std::uint32_t> result;
    //     std::size_t start = 0;
    
    //     try {
    //         while (start < data.size()) {
    //             std::size_t end = data.find(',', start);
    //             std::string token = (end == std::string::npos)
    //                 ? data.substr(start)
    //                 : data.substr(start, end - start);
    
    //             // Skip empty tokens (e.g., double commas)
    //             if (!token.empty()) {
    //                 std::size_t pos = 0;
    //                 unsigned long long parsed = std::stoull(token, &pos);
    //                 if (pos != token.size() || parsed > std::numeric_limits<uint32_t>::max())
    //                     return std::nullopt;
    
    //                 result.push_back(static_cast<std::uint32_t>(parsed));
    //             }
    
    //             if (end == std::string::npos) break;
    //             start = end + 1;
    //         }
    //     } catch (...) {
    //         return std::nullopt;
    //     }
    
    //     return result;
    // }


}