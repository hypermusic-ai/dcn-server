#pragma once

#include <vector>
#include <string>

#ifdef interface
    #undef interface
#endif
#include <evmc/evmc.hpp>
#ifndef interface
    #define interface __STRUCT__
#endif

#include "keccak256.hpp"

namespace dcn::crypto
{
    std::vector<std::uint8_t> constructSelector(std::string signature);

    evmc::bytes32 constructEventTopic(std::string signature);

    std::optional<std::vector<evmc::bytes32>> decodeTopicWords(const std::vector<std::string> & topics_hex);
}