#pragma once

#include <cstddef>
#include <cstdint>

namespace dcn::crypto
{
    inline std::uint64_t readUint64BE(const std::uint8_t * ptr)
    {
        std::uint64_t out = 0;
        for(std::size_t i = 0; i < sizeof(std::uint64_t); ++i)
        {
            out = (out << 8) | static_cast<std::uint64_t>(ptr[i]);
        }

        return out;
    }

    inline void writeUint64BE(std::uint8_t * ptr, std::uint64_t value)
    {
        for(std::size_t i = 0; i < sizeof(std::uint64_t); ++i)
        {
            const std::size_t shift = (sizeof(std::uint64_t) - 1 - i) * 8;
            ptr[i] = static_cast<std::uint8_t>((value >> shift) & 0xFFu);
        }
    }

    inline void writeUint32BE(std::uint8_t * ptr, std::uint32_t value)
    {
        for(std::size_t i = 0; i < sizeof(std::uint32_t); ++i)
        {
            const std::size_t shift = (sizeof(std::uint32_t) - 1 - i) * 8;
            ptr[i] = static_cast<std::uint8_t>((value >> shift) & 0xFFu);
        }
    }
}
