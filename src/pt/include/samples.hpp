#pragma once
#include <string>
#include <vector>

#include "samples.pb.h"

#include "parser.hpp"

namespace dcn::parse
{
    /**
     * @brief Converts a Samples object to a JSON object using JSON.
     * @param samples The Samples object to convert.
     */
    template<>
    Result<json> parseToJson(std::vector<Samples> samples, use_json_t);

    /**
     * @brief Parses a JSON string to a Samples object using JSON.
     * @param json The JSON string to parse.
     */
    template<>
    Result<std::vector<Samples>> parseFromJson(json json, use_json_t);
}