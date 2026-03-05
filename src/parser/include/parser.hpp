#pragma once

#include <nlohmann/json.hpp>
using json = nlohmann::json;

#include <google/protobuf/util/json_util.h>

#include "parse_error.hpp"

namespace dcn::parse
{   
    /**
     * @brief A tag type to indicate whether to use Protobuf or JSON for parsing.
     * 
     * This tag type is used to distinguish between Protobuf and JSON parsing.
     */
    struct use_protobuf_t{};

    /**
     * @brief A tag type to indicate whether to use Protobuf or JSON for parsing.
     * 
     * This tag type is used to distinguish between Protobuf and JSON parsing.
     */
    struct use_json_t{};

    /**
     * @brief A tag type to indicate whether to use Protobuf or JSON for parsing.
     * 
     * This tag type is used to distinguish between Protobuf and JSON parsing.
     */
    static constexpr use_protobuf_t use_protobuf{};

    /**
     * @brief A tag type to indicate whether to use Protobuf or JSON for parsing.
     * 
     * This tag type is used to distinguish between Protobuf and JSON parsing.
     */
    static constexpr use_json_t use_json{};

    /**
     * @brief Converts a JSON string to a T using JSON.
     * 
     * @tparam T The message type.
     * @param json The JSON string to convert.
     */
    template<class T>
    Result<T> parseFromJson(json json, use_json_t);

    /**
     * @brief Converts a JSON string to a T using Protobuf.
     * 
     * @tparam T The message type.
     * @param json The JSON string to convert.
     */
    template<class T>
    Result<T> parseFromJson(std::string json_str, use_protobuf_t);

    /**
     * @brief Converts a T to a JSON object using JSON.
     * 
     * @tparam T The message type.
     * @param message The message to convert.
     */
    template<class T>
    Result<json> parseToJson(T message, use_json_t);

    /**
     * @brief Converts a T to a JSON string using Protobuf.
     * 
     * @tparam T The message type.
     * @param message The message to convert.
     */
    template<class T>
    Result<std::string> parseToJson(T message, use_protobuf_t);


    template<class T>
    Result<T> decodeBytes(const std::vector<uint8_t> & bytes);
}