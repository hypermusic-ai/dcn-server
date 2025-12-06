#pragma once

#include <cstdint>
#include <string>
#include <expected>

namespace dcn::parse
{   
    struct Error
    {
        enum class Type : std::uint8_t
        {
            UNKNOWN         = 0U,
            INVALID         = 1U,
            OUT_OF_RANGE    = 2U
        };

        std::string message = "";
        Type type = Type::UNKNOWN;
    };

    template<class T>
    using Result = std::expected<T, Error>;
}