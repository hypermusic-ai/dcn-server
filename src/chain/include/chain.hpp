#pragma once

#include "address.hpp"
#include "chain_interface.hpp"
#include "ingestion.hpp"
#include "deploy.hpp"
#include "execute.hpp"


namespace dcn::chain
{
    std::vector<std::uint8_t> constructSelector(std::string signature);

    evmc::bytes32 constructEventTopic(std::string signature);

    std::optional<std::vector<evmc::bytes32>> decodeTopicWords(const std::vector<std::string> & topics_hex);


    std::uint64_t readUint256(const std::vector<std::uint8_t> & bytes, std::size_t offset);

    std::uint32_t readUint32Padded(const std::vector<uint8_t>& bytes, std::size_t offset);


    std::uint32_t readUint32(const std::vector<std::uint8_t> & bytes, std::size_t offset);

    std::uint64_t readOffset(const std::vector<std::uint8_t> & bytes, std::size_t offset);

    std::optional<std::size_t> readWordAsSizeT(const std::uint8_t* data, std::size_t data_size, std::size_t offset);

    std::optional<std::uint32_t> readUint32Word(const std::uint8_t* data, std::size_t data_size, std::size_t offset);


    std::optional<std::string> decodeAbiString(const std::uint8_t* data, std::size_t data_size, std::size_t string_offset);

    std::optional<std::vector<std::string>> decodeAbiStringArray(const std::uint8_t* data, std::size_t data_size, std::size_t array_offset);

    std::optional<std::vector<std::int32_t>> decodeAbiInt32Array(const std::uint8_t* data, std::size_t data_size, std::size_t array_offset);
}