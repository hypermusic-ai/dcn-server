#include "math.hpp"
#include <cassert>
#include <limits>

namespace dcn::utils
{
    std::uint64_t readUint256(const std::vector<std::uint8_t> & bytes, std::size_t offset) {
        std::uint64_t value = 0;
        for (std::size_t i = 0; i < 32; ++i) {
            value <<= 8;
            value |= bytes[offset + i];
        }
        return value;
    }


    std::uint32_t readUint32Padded(const std::vector<uint8_t>& bytes, std::size_t offset) {
        // Read last 4 bytes of 32-byte ABI word
        assert(offset + 32 <= bytes.size());
        uint32_t value = 0;
        for (int i = 28; i < 32; ++i) {
            value = (value << 8) | bytes[offset + i];
        }
        return value;
    }

    std::uint32_t readUint32(const std::vector<std::uint8_t> & bytes, std::size_t offset) {
        std::uint32_t value = 0;
        for (std::size_t i = 0; i < 4; ++i) {
            value <<= 8;
            value |= bytes[offset + i];
        }
        return value;
    }

    std::uint64_t readOffset(const std::vector<std::uint8_t> & bytes, std::size_t offset) {
        std::size_t return_offset = 0;
        for (int i = 0; i < 32; ++i)
            return_offset = (return_offset << 8) | bytes[offset + i];
        return_offset += 32;
        return return_offset;
    }

    std::optional<std::size_t> readWordAsSizeT(const std::uint8_t* data, std::size_t data_size, std::size_t offset)
    {
        if(data == nullptr || offset + 32 > data_size)
        {
            return std::nullopt;
        }

        std::size_t value = 0;

        constexpr std::size_t prefix = 32 - sizeof(std::size_t);
        for(std::size_t i = 0; i < prefix; ++i)
        {
            if(data[offset + i] != 0)
            {
                return std::nullopt;
            }
        }

        for(std::size_t i = prefix; i < 32; ++i)
        {
            value = (value << 8) | data[offset + i];
        }

        return value;
    }

    std::optional<std::uint32_t> readUint32Word(const std::uint8_t* data, std::size_t data_size, std::size_t offset)
    {
        const auto value_res = utils::readWordAsSizeT(data, data_size, offset);
        if(!value_res || *value_res > static_cast<std::size_t>(std::numeric_limits<std::uint32_t>::max()))
        {
            return std::nullopt;
        }

        return static_cast<std::uint32_t>(*value_res);
    }

}