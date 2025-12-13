#pragma once
#include <absl/hash/hash.h>

#include "execute.pb.h"

#include "parser.hpp"

namespace dcn
{

}

namespace dcn::parse
{
    /**
     * @brief Converts a RunningInstance to a JSON object.
     * @param running_instance The RunningInstance to convert.
     */
    template<>
    Result<json> parseToJson(RunningInstance running_instance, use_json_t);

    /**
     * @brief Parses a JSON object into a RunningInstance.
     * @param json The JSON object to parse.
     */
    template<>
    Result<RunningInstance> parseFromJson(json json, use_json_t);

    /**
     * @brief Converts a RunningInstance to a protobuf JSON string.
     * @param running_instance The RunningInstance to convert.
     */
    template<>
    Result<std::string> parseToJson(RunningInstance running_instance, use_protobuf_t);

    /**
     * @brief Parses a protobuf JSON string into a RunningInstance.
     * @param json_str The JSON string to parse.
     */
    template<>
    Result<RunningInstance> parseFromJson(std::string json_str, use_protobuf_t);


    /**
     * @brief Converts an ExecuteRequest to a JSON object.
     * @param execute_request The ExecuteRequest to convert.
     */
    template<>
    Result<json> parseToJson(ExecuteRequest execute_request, use_json_t);

    /**
     * @brief Parses a JSON object into an ExecuteRequest.
     * @param json The JSON object to parse.
     */
    template<>
    Result<ExecuteRequest> parseFromJson(json json, use_json_t);

    /**
     * @brief Converts an ExecuteRequest to a protobuf JSON string.
     * @param execute_request The ExecuteRequest to convert.
     */
    template<>
    Result<std::string> parseToJson(ExecuteRequest execute_request, use_protobuf_t);

    /**
     * @brief Parses a protobuf JSON string into an ExecuteRequest.
     * @param json_str The JSON string to parse.
     */
    template<>
    Result<ExecuteRequest> parseFromJson(std::string json_str, use_protobuf_t);
}
