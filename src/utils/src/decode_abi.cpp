#include "decode_abi.hpp"

#include "math.hpp"

namespace dcn::utils
{
    std::optional<std::string> decodeAbiString(const std::uint8_t* data, std::size_t data_size, std::size_t string_offset)
    {
        const auto length_res = utils::readWordAsSizeT(data, data_size, string_offset);
        if(!length_res)
        {
            return std::nullopt;
        }

        const std::size_t length = *length_res;
        if(string_offset + 32 > data_size || length > (data_size - (string_offset + 32)))
        {
            return std::nullopt;
        }

        return std::string(reinterpret_cast<const char*>(data + string_offset + 32), length);
    }


    std::optional<std::vector<std::string>> decodeAbiStringArray(const std::uint8_t* data, std::size_t data_size, std::size_t array_offset)
    {
        const auto length_res = utils::readWordAsSizeT(data, data_size, array_offset);
        if(!length_res)
        {
            return std::nullopt;
        }

        const std::size_t length = *length_res;
        const std::size_t head_start = array_offset + 32;

        if(head_start > data_size)
        {
            return std::nullopt;
        }

        if(length > (std::numeric_limits<std::size_t>::max() / 32))
        {
            return std::nullopt;
        }

        const std::size_t head_size = length * 32;
        if(head_size > (data_size - head_start))
        {
            return std::nullopt;
        }

        std::vector<std::string> out;
        out.reserve(length);

        for(std::size_t i = 0; i < length; ++i)
        {
            const auto rel_offset_res = utils::readWordAsSizeT(data, data_size, head_start + i * 32);
            if(!rel_offset_res)
            {
                return std::nullopt;
            }

            const std::size_t elem_offset = head_start + *rel_offset_res;
            const auto elem_res = utils::decodeAbiString(data, data_size, elem_offset);
            if(!elem_res)
            {
                return std::nullopt;
            }

            out.push_back(*elem_res);
        }

        return out;
    }

    std::optional<std::vector<std::int32_t>> decodeAbiInt32Array(const std::uint8_t* data, std::size_t data_size, std::size_t array_offset)
    {
        const auto length_res = utils::readWordAsSizeT(data, data_size, array_offset);
        if(!length_res)
        {
            return std::nullopt;
        }

        const std::size_t length = *length_res;
        const std::size_t first_value_offset = array_offset + 32;

        if(first_value_offset > data_size)
        {
            return std::nullopt;
        }

        if(length > (std::numeric_limits<std::size_t>::max() / 32))
        {
            return std::nullopt;
        }

        const std::size_t values_size = length * 32;
        if(values_size > (data_size - first_value_offset))
        {
            return std::nullopt;
        }

        std::vector<std::int32_t> out;
        out.reserve(length);

        for(std::size_t i = 0; i < length; ++i)
        {
            const std::size_t word_offset = first_value_offset + i * 32;
            const std::uint8_t* word = data + word_offset;

            const std::uint32_t raw_value =
                (static_cast<std::uint32_t>(word[28]) << 24) |
                (static_cast<std::uint32_t>(word[29]) << 16) |
                (static_cast<std::uint32_t>(word[30]) << 8) |
                static_cast<std::uint32_t>(word[31]);

            const bool negative = (raw_value & 0x80000000u) != 0;
            const std::uint8_t expected_sign = negative ? 0xFF : 0x00;
            for(std::size_t b = 0; b < 28; ++b)
            {
                if(word[b] != expected_sign)
                {
                    return std::nullopt;
                }
            }

            out.push_back(static_cast<std::int32_t>(raw_value));
        }

        return out;
    }
}