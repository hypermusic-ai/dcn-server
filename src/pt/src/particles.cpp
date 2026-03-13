#include "particles.hpp"
#include "chain.hpp"

namespace dcn::parse
{
    template<>
    Result<json> parseToJson(std::vector<Particles> particles, use_json_t) 
    {
        json arr = json::array();

        for (const auto& particle : particles) {
            json obj;
            obj["path"] = particle.path();
            obj["data"] = particle.data();
            arr.push_back(obj);
        }

        return arr;
    }

    template<>
    Result<std::vector<Particles>> parseFromJson(json json_val, use_json_t) 
    {
        if (!json_val.is_array())
            return std::unexpected(ParseError{ParseError::Kind::TYPE_MISMATCH, "Invalid JSON object"});

        std::vector<Particles> result;

        for (const auto& item : json_val) {
            if (!item.contains("path") || !item.contains("data"))
                return std::unexpected(ParseError{ParseError::Kind::INVALID_VALUE, "path or data not found"});

            Particles particles;
            particles.set_path(item["path"].get<std::string>());

            for (const auto& val : item["data"]) {
                particles.add_data(val.get<std::uint32_t>());
            }

            result.push_back(std::move(particles));
        }

        return result;
    }

    template<>
    Result<std::vector<Particles>> decodeBytes(const std::vector<std::uint8_t>& bytes)
    {
        std::vector<Particles> result;

        // Step 1: read base offset to array
        std::size_t array_base = chain::readUint256(bytes, 0);  // should be 32

        // Step 2: read array length
        std::size_t array_len = chain::readUint256(bytes, array_base);  // at offset 32

        // Step 3: read offsets to structs (relative to array_base)
        std::vector<std::size_t> struct_offsets;
        for (std::size_t i = 0; i < array_len; ++i) {
            std::size_t struct_rel_offset = chain::readUint256(bytes, (array_base + 32) + (i * 32));
            struct_offsets.push_back((array_base + 32) + struct_rel_offset);
        }

        // Step 4: parse each struct
        for (std::size_t struct_offset : struct_offsets) 
        {
            Particles particles;

            std::size_t path_rel = chain::readUint256(bytes, struct_offset);
            std::size_t data_rel = chain::readUint256(bytes, struct_offset + 32);

            // 4.2: resolve actual offsets
            std::size_t path_offset = struct_offset + path_rel;
            std::size_t data_offset = struct_offset + data_rel;

            // 4.3: read string length and content
            std::size_t str_len = chain::readUint256(bytes, path_offset);

            particles.set_path(std::string(
                reinterpret_cast<const char*>(&bytes[path_offset + 32]),
                str_len
            ));

            // 4.4: read data length and entries
            std::size_t data_len = chain::readUint256(bytes, data_offset);
            std::vector<std::uint32_t> data;
            for (std::size_t j = 0; j < data_len; ++j) {
                std::uint32_t val = chain::readUint32Padded(bytes, data_offset + 32 + j * 32);
                particles.add_data(val);
            }

            result.emplace_back(std::move(particles));
        }

        return result;
    }
}
