#pragma once

#include <optional>
#include <cstdint>


#ifdef interface
    #undef interface
#endif
#include <evmc/evmc.hpp>
#ifndef interface
    #define interface __STRUCT__
#endif

namespace dcn::chain
{
    using Address = evmc::address;

    std::optional<chain::Address> readAddressWord(const std::uint8_t* data, std::size_t data_size, std::size_t offset = 0);
    std::optional<chain::Address> readAddressWord(const std::vector<std::uint8_t> & data, std::size_t offset = 0);

    chain::Address topicWordToAddress(const evmc::bytes32 & topic_word);

}