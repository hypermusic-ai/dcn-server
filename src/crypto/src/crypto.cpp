#include "crypto.hpp"

namespace dcn::crypto
{
    std::vector<std::uint8_t> constructSelector(std::string signature)
    {
        std::uint8_t hash[32];
        crypto::Keccak256::getHash(reinterpret_cast<const uint8_t*>(signature.data()), signature.size(), hash);
        return std::vector<std::uint8_t>(hash, hash + 4);
    }


    evmc::bytes32 constructEventTopic(std::string signature)
    {
        evmc::bytes32 topic{};
        crypto::Keccak256::getHash(reinterpret_cast<const uint8_t*>(signature.data()), signature.size(), topic.bytes);
        return topic;
    }

    std::optional<std::vector<evmc::bytes32>> decodeTopicWords(const std::vector<std::string> & topics_hex)
    {
        if(topics_hex.empty())
        {
            return std::nullopt;
        }

        std::vector<evmc::bytes32> topic_words;
        topic_words.reserve(topics_hex.size());
        for(const std::string & topic_hex : topics_hex)
        {
            const auto topic_word = evmc::from_hex<evmc::bytes32>(topic_hex);
            if(!topic_word)
            {
                return std::nullopt;
            }
            topic_words.push_back(*topic_word);
        }

        return topic_words;
    }

}