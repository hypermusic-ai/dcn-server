#pragma once
#include <string>
#include <vector>

#include "particles.pb.h"

#include "parser.hpp"

namespace dcn::parse
{
    /**
     * @brief Converts a Particles object to a JSON object using JSON.
     * @param particles The Particles object to convert.
     */
    template<>
    Result<json> parseToJson(std::vector<Particles> particles, use_json_t);

    /**
     * @brief Parses a JSON string to a Particles object using JSON.
     * @param json The JSON string to parse.
     */
    template<>
    Result<std::vector<Particles>> parseFromJson(json json, use_json_t);

    template<>
    Result<std::vector<Particles>> decodeBytes(const std::vector<uint8_t> & bytes);
}
