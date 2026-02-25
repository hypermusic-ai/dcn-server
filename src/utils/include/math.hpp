#pragma once

#include <vector>
#include <optional>
#include <string>
#include <cstdint>

namespace dcn::utils
{
    std::uint64_t readUint256(const std::vector<std::uint8_t> & bytes, std::size_t offset);

    std::uint32_t readUint32Padded(const std::vector<uint8_t>& bytes, std::size_t offset);


    std::uint32_t readUint32(const std::vector<std::uint8_t> & bytes, std::size_t offset);

    std::uint64_t readOffset(const std::vector<std::uint8_t> & bytes, std::size_t offset);

    std::optional<std::size_t> readWordAsSizeT(const std::uint8_t* data, std::size_t data_size, std::size_t offset);

    std::optional<std::uint32_t> readUint32Word(const std::uint8_t* data, std::size_t data_size, std::size_t offset);
}