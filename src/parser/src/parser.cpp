#include "parser.hpp"

#include <charconv>

namespace dcn::parse
{
    Result<std::uint32_t> parseUint32Decimal(std::string_view value)
    {
        std::uint32_t parsed = 0;
        const auto [ptr, ec] = std::from_chars(value.data(), value.data() + value.size(), parsed, 10);
        if(ec == std::errc::result_out_of_range)
        {
            return std::unexpected(ParseError{
                .kind = ParseError::Kind::OUT_OF_RANGE,
                .message = "uint32 decimal value is out of range"});
        }

        if(ec != std::errc{} || ptr != (value.data() + value.size()))
        {
            return std::unexpected(ParseError{
                .kind = ParseError::Kind::INVALID_VALUE,
                .message = "invalid uint32 decimal value"});
        }

        return parsed;
    }
}
