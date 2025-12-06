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


    template<class T>
    std::optional<T> parseFromJson(json json, use_json_t);

    template<class T>
    std::optional<T> parseFromJson(std::string json_str, use_protobuf_t);
}