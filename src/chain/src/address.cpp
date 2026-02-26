#include <memory>

#include "address.hpp"

namespace dcn::chain
{
    std::optional<chain::Address> readAddressWord(const std::uint8_t* data, std::size_t data_size, std::size_t offset)
    {
        if(data == nullptr || offset + 32 > data_size)
        {
            return std::nullopt;
        }

        chain::Address addr{};
        std::memcpy(addr.bytes, data + offset + 12, 20);
        return addr;
    }

    std::optional<chain::Address> readAddressWord(const std::vector<std::uint8_t> & data, std::size_t offset)
    {
        return readAddressWord(data.data(), data.size(), offset);
    }

    chain::Address topicWordToAddress(const evmc::bytes32 & topic_word)
    {
        chain::Address addr{};
        std::memcpy(addr.bytes, topic_word.bytes + 12, 20);
        return addr;
    }

}