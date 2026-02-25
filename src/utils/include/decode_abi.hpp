#include <vector>
#include <optional>
#include <string>
#include <cstdint>
#include <limits>


namespace dcn::utils
{
    std::optional<std::string> decodeAbiString(const std::uint8_t* data, std::size_t data_size, std::size_t string_offset);
    std::optional<std::vector<std::string>> decodeAbiStringArray(const std::uint8_t* data, std::size_t data_size, std::size_t array_offset);
    std::optional<std::vector<std::int32_t>> decodeAbiInt32Array(const std::uint8_t* data, std::size_t data_size, std::size_t array_offset);

}