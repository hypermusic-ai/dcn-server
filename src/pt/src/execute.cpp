#include "execute.hpp"

#include <algorithm>
#include <vector>

namespace dcn::parse
{
    template<>
    Result<json> parseToJson(RunningInstance running_instance, use_json_t)
    {
        json json_obj = json::object();
        json_obj["start_point"] = running_instance.start_point();
        json_obj["transformation_shift"] = running_instance.transformation_shift();
        return json_obj;
    }

    template<>
    Result<RunningInstance> parseFromJson(json json, use_json_t)
    {
        RunningInstance running_instance;
        running_instance.set_start_point(json["start_point"].get<std::uint32_t>());
        running_instance.set_transformation_shift(json["transformation_shift"].get<std::uint32_t>());
        return running_instance;
    }

    template<>
    Result<std::string> parseToJson(RunningInstance running_instance, use_protobuf_t)
    {
        google::protobuf::util::JsonPrintOptions options;
        options.add_whitespace = true; // Pretty print
        options.preserve_proto_field_names = true; // Use snake_case from proto
        options.always_print_fields_with_no_presence = true;

        std::string json_output;

        auto status = google::protobuf::util::MessageToJsonString(running_instance, &json_output, options);

        if(!status.ok()) return std::unexpected(ParseError{ParseError::Kind::INVALID_VALUE, "invalid running_instance"});

        return json_output;
    }

    template<>
    Result<RunningInstance> parseFromJson(std::string json_str, use_protobuf_t)
    {
        google::protobuf::util::JsonParseOptions options;

        RunningInstance running_instance;

        auto status = google::protobuf::util::JsonStringToMessage(json_str, &running_instance, options);

        if(!status.ok()) return std::unexpected(ParseError{ParseError::Kind::INVALID_VALUE, "invalid running_instance"});

        return running_instance;
    }



    template<>
    Result<json> parseToJson(ExecuteRequest execute_request, use_json_t)
    {
        json json_obj = json::object();
        json_obj["connector_name"] = execute_request.connector_name();
        json_obj["particles_count"] = execute_request.particles_count();
        json_obj["dynamic_ri"] = json::object();

        std::vector<std::pair<std::uint32_t, RunningInstance>> sorted_dynamic_ri;
        sorted_dynamic_ri.reserve(static_cast<std::size_t>(execute_request.dynamic_ri_size()));
        for(const auto & [position, running_instance] : execute_request.dynamic_ri())
        {
            sorted_dynamic_ri.emplace_back(position, running_instance);
        }
        std::ranges::sort(sorted_dynamic_ri, [](const auto & lhs, const auto & rhs)
        {
            return lhs.first < rhs.first;
        });

        for(const auto & [position, running_instance] : sorted_dynamic_ri)
        {
            auto running_instance_result = parseToJson(running_instance, use_json);
            if(!running_instance_result) return std::unexpected(ParseError{ParseError::Kind::INVALID_VALUE, "invalid running_instance"});
            json_obj["dynamic_ri"][std::to_string(position)] = std::move(*running_instance_result);
        }

        return json_obj;
    }

    template<>
    Result<ExecuteRequest> parseFromJson(json json, use_json_t)
    {
        ExecuteRequest execute_request;
        execute_request.set_connector_name(json["connector_name"].get<std::string>()); 
        execute_request.set_particles_count(json["particles_count"].get<std::uint32_t>()); 

        if(json.contains("dynamic_ri") && !json["dynamic_ri"].is_object())
        {
            return std::unexpected(ParseError{ParseError::Kind::INVALID_VALUE, "invalid dynamic_ri"});
        }
        
        // Empty dynamic_ri
        if(!json.contains("dynamic_ri"))
        {
            json["dynamic_ri"] = json::object();
        }

        for(const auto & [position_key, running_instance] : json["dynamic_ri"].items())
        {
            const auto position_opt = parse::parseUint32Decimal(position_key);
            if(!position_opt || position_key != std::to_string(*position_opt))
            {
                return std::unexpected(ParseError{ParseError::Kind::INVALID_VALUE, "invalid dynamic_ri key"});
            }

            auto running_instance_result = parseFromJson<RunningInstance>(running_instance, use_json);
            if(!running_instance_result) return std::unexpected(ParseError{ParseError::Kind::INVALID_VALUE, "invalid running_instance"});

            (*execute_request.mutable_dynamic_ri())[*position_opt] = *running_instance_result;
        }

        return execute_request;
    }

    template<>
    Result<std::string> parseToJson(ExecuteRequest execute_request, use_protobuf_t)
    {
        google::protobuf::util::JsonPrintOptions options;
        options.add_whitespace = true; // Pretty print
        options.preserve_proto_field_names = true; // Use snakeCase from proto
        options.always_print_fields_with_no_presence = true;

        std::string json_output;

        auto status = google::protobuf::util::MessageToJsonString(execute_request, &json_output, options);

        if(!status.ok()) return std::unexpected(ParseError{ParseError::Kind::INVALID_VALUE, "invalid execute_request"});

        return json_output;
    }

    template<>
    Result<ExecuteRequest> parseFromJson(std::string json_str, use_protobuf_t)
    {
        google::protobuf::util::JsonParseOptions options;

        ExecuteRequest execute_request;

        auto status = google::protobuf::util::JsonStringToMessage(json_str, &execute_request, options);

        if(!status.ok()) return std::unexpected(ParseError{ParseError::Kind::INVALID_VALUE, "invalid execute_request"});

        return execute_request;
    }
}
