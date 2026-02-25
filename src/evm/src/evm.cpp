#include "evm.hpp"
#include <limits>

namespace dcn::evm
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

    template<>
    std::vector<std::uint8_t> encodeAsArg<Address>(const Address & address)
    {
        std::vector<std::uint8_t> encoded(32, 0); // Initialize with 32 zero bytes
        std::copy(address.bytes, address.bytes + 20, encoded.begin() + 12); // Right-align in last 20 bytes
        return encoded;
    }

    template<>
    std::vector<std::uint8_t> encodeAsArg<std::uint32_t>(const std::uint32_t & value)
    {
        std::vector<std::uint8_t> encoded(32, 0); // Initialize with 32 zero bytes

        // Encode as big-endian and place in the last 4 bytes (right-aligned)
        encoded[28] = static_cast<std::uint8_t>((value >> 24) & 0xFF);
        encoded[29] = static_cast<std::uint8_t>((value >> 16) & 0xFF);
        encoded[30] = static_cast<std::uint8_t>((value >> 8) & 0xFF);
        encoded[31] = static_cast<std::uint8_t>(value & 0xFF);

        return encoded;
    }

    template<>
    std::vector<std::uint8_t> encodeAsArg<std::vector<std::uint32_t>>(const std::vector<std::uint32_t>& vec)
    {
        std::vector<std::uint8_t> encoded;

        // Step 1: offset to the data section (0x20 = 32)
        encoded.resize(32, 0);
        encoded[31] = 0x20; // offset is 32 bytes

        // Step 2: dynamic data section begins
        std::vector<std::uint8_t> data;

        // 2.1: encode length (number of elements)
        data.resize(32, 0);
        std::uint32_t length = static_cast<std::uint32_t>(vec.size());
        data[28] = static_cast<std::uint8_t>((length >> 24) & 0xFF);
        data[29] = static_cast<std::uint8_t>((length >> 16) & 0xFF);
        data[30] = static_cast<std::uint8_t>((length >> 8) & 0xFF);
        data[31] = static_cast<std::uint8_t>(length & 0xFF);

        // 2.2: encode each element (right-aligned uint32 in 32 bytes)
        for (const std::uint32_t val : vec)
        {
            std::vector<std::uint8_t> element(32, 0);
            element[28] = static_cast<std::uint8_t>((val >> 24) & 0xFF);
            element[29] = static_cast<std::uint8_t>((val >> 16) & 0xFF);
            element[30] = static_cast<std::uint8_t>((val >> 8) & 0xFF);
            element[31] = static_cast<std::uint8_t>(val & 0xFF);
            data.insert(data.end(), element.begin(), element.end());
        }

        // Step 3: append the data section after the 32-byte offset
        encoded.insert(encoded.end(), data.begin(), data.end());

        return encoded;
    }

    template<>
    std::vector<std::uint8_t> encodeAsArg<std::vector<std::tuple<std::uint32_t, std::uint32_t>>>(const std::vector<std::tuple<std::uint32_t, std::uint32_t>>& vec)
    {
        std::vector<std::uint8_t> encoded;

        // Encode length
        std::vector<std::uint8_t> length(32, 0);
        uint64_t len = vec.size();
        for (int i = 0; i < 8; ++i)
            length[31 - i] = static_cast<uint8_t>(len >> (i * 8));
        
        encoded.insert(encoded.end(), length.begin(), length.end());

        // Encode each pair as two int32s in a 32-byte ABI word each
        for (const auto& [a, b] : vec)
        {
            std::vector<std::uint8_t> elem(32, 0);
            for (int i = 0; i < 4; ++i)
                elem[31 - i] = static_cast<uint8_t>(a >> (i * 8));
            
            encoded.insert(encoded.end(), elem.begin(), elem.end());

            std::fill(elem.begin(), elem.end(), 0);
            for (int i = 0; i < 4; ++i)
                elem[31 - i] = static_cast<uint8_t>(b >> (i * 8));
            
            encoded.insert(encoded.end(), elem.begin(), elem.end());
        }

        return encoded;
    }

    template<>
    std::vector<std::uint8_t> encodeAsArg<std::string>(const std::string& str)
    {
        std::vector<std::uint8_t> encoded;

        std::vector<uint8_t> len_enc(32, 0);
        len_enc[31] = static_cast<uint8_t>(str.size());

        encoded.insert(encoded.end(), len_enc.begin(), len_enc.end()); // string length
        encoded.insert(encoded.end(), str.begin(), str.end());             // string bytes

        // pad to multiple of 32 bytes
        size_t pad = (32 - (str.size() % 32)) % 32;
        encoded.insert(encoded.end(), pad, 0);
        
        return encoded;
    }


    static std::uint32_t _readUint32(const std::vector<std::uint8_t> & bytes, std::size_t offset) {
        std::uint32_t value = 0;
        for (std::size_t i = 0; i < 4; ++i) {
            value <<= 8;
            value |= bytes[offset + i];
        }
        return value;
    }

    static std::uint32_t _readUint32Padded(const std::vector<uint8_t>& bytes, std::size_t offset) {
        // Read last 4 bytes of 32-byte ABI word
        assert(offset + 32 <= bytes.size());
        uint32_t value = 0;
        for (int i = 28; i < 32; ++i) {
            value = (value << 8) | bytes[offset + i];
        }
        return value;
    }

    static std::uint64_t _readUint256(const std::vector<std::uint8_t> & bytes, std::size_t offset) {
        std::uint64_t value = 0;
        for (std::size_t i = 0; i < 32; ++i) {
            value <<= 8;
            value |= bytes[offset + i];
        }
        return value;
    }


    static std::uint64_t _readOffset(const std::vector<std::uint8_t> & bytes, std::size_t offset) {
        std::size_t return_offset = 0;
        for (int i = 0; i < 32; ++i)
            return_offset = (return_offset << 8) | bytes[offset + i];
        return_offset += 32;
        return return_offset;
    }

    static std::optional<std::size_t> _readWordAsSizeT(const std::uint8_t* data, std::size_t data_size, std::size_t offset)
    {
        if(data == nullptr || offset + 32 > data_size)
        {
            return std::nullopt;
        }

        std::size_t value = 0;

        constexpr std::size_t prefix = 32 - sizeof(std::size_t);
        for(std::size_t i = 0; i < prefix; ++i)
        {
            if(data[offset + i] != 0)
            {
                return std::nullopt;
            }
        }

        for(std::size_t i = prefix; i < 32; ++i)
        {
            value = (value << 8) | data[offset + i];
        }

        return value;
    }

    static std::optional<Address> _readAddressWord(const std::uint8_t* data, std::size_t data_size, std::size_t offset)
    {
        if(data == nullptr || offset + 32 > data_size)
        {
            return std::nullopt;
        }

        Address addr{};
        std::memcpy(addr.bytes, data + offset + 12, 20);
        return addr;
    }

    static Address _topicWordToAddress(const evmc::bytes32 & topic_word)
    {
        Address addr{};
        std::memcpy(addr.bytes, topic_word.bytes + 12, 20);
        return addr;
    }

    static std::optional<std::string> _decodeAbiString(const std::uint8_t* data, std::size_t data_size, std::size_t string_offset)
    {
        const auto length_res = _readWordAsSizeT(data, data_size, string_offset);
        if(!length_res)
        {
            return std::nullopt;
        }

        const std::size_t length = *length_res;
        if(string_offset + 32 > data_size || length > (data_size - (string_offset + 32)))
        {
            return std::nullopt;
        }

        return std::string(reinterpret_cast<const char*>(data + string_offset + 32), length);
    }

    static std::optional<std::vector<std::string>> _decodeAbiStringArray(const std::uint8_t* data, std::size_t data_size, std::size_t array_offset)
    {
        const auto length_res = _readWordAsSizeT(data, data_size, array_offset);
        if(!length_res)
        {
            return std::nullopt;
        }

        const std::size_t length = *length_res;
        const std::size_t head_start = array_offset + 32;

        if(head_start > data_size)
        {
            return std::nullopt;
        }

        if(length > (std::numeric_limits<std::size_t>::max() / 32))
        {
            return std::nullopt;
        }

        const std::size_t head_size = length * 32;
        if(head_size > (data_size - head_start))
        {
            return std::nullopt;
        }

        std::vector<std::string> out;
        out.reserve(length);

        for(std::size_t i = 0; i < length; ++i)
        {
            const auto rel_offset_res = _readWordAsSizeT(data, data_size, head_start + i * 32);
            if(!rel_offset_res)
            {
                return std::nullopt;
            }

            const std::size_t elem_offset = head_start + *rel_offset_res;
            const auto elem_res = _decodeAbiString(data, data_size, elem_offset);
            if(!elem_res)
            {
                return std::nullopt;
            }

            out.push_back(*elem_res);
        }

        return out;
    }

    static std::optional<std::vector<std::int32_t>> _decodeAbiInt32Array(const std::uint8_t* data, std::size_t data_size, std::size_t array_offset)
    {
        const auto length_res = _readWordAsSizeT(data, data_size, array_offset);
        if(!length_res)
        {
            return std::nullopt;
        }

        const std::size_t length = *length_res;
        const std::size_t first_value_offset = array_offset + 32;

        if(first_value_offset > data_size)
        {
            return std::nullopt;
        }

        if(length > (std::numeric_limits<std::size_t>::max() / 32))
        {
            return std::nullopt;
        }

        const std::size_t values_size = length * 32;
        if(values_size > (data_size - first_value_offset))
        {
            return std::nullopt;
        }

        std::vector<std::int32_t> out;
        out.reserve(length);

        for(std::size_t i = 0; i < length; ++i)
        {
            const std::size_t word_offset = first_value_offset + i * 32;
            const std::uint8_t* word = data + word_offset;

            const std::uint32_t raw_value =
                (static_cast<std::uint32_t>(word[28]) << 24) |
                (static_cast<std::uint32_t>(word[29]) << 16) |
                (static_cast<std::uint32_t>(word[30]) << 8) |
                static_cast<std::uint32_t>(word[31]);

            const bool negative = (raw_value & 0x80000000u) != 0;
            const std::uint8_t expected_sign = negative ? 0xFF : 0x00;
            for(std::size_t b = 0; b < 28; ++b)
            {
                if(word[b] != expected_sign)
                {
                    return std::nullopt;
                }
            }

            out.push_back(static_cast<std::int32_t>(raw_value));
        }

        return out;
    }

    static std::optional<std::uint32_t> _readUint32Word(const std::uint8_t* data, std::size_t data_size, std::size_t offset)
    {
        const auto value_res = _readWordAsSizeT(data, data_size, offset);
        if(!value_res || *value_res > static_cast<std::size_t>(std::numeric_limits<std::uint32_t>::max()))
        {
            return std::nullopt;
        }

        return static_cast<std::uint32_t>(*value_res);
    }

    static std::optional<std::vector<evmc::bytes32>> _decodeTopicWords(const std::vector<std::string> & topics_hex)
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

    std::optional<ParticleAddedEvent> decodeParticleAddedEvent(
        const std::uint8_t* data,
        std::size_t data_size,
        const evmc::bytes32 topics[],
        std::size_t num_topics)
    {
        if(data == nullptr || topics == nullptr || num_topics < 3 || data_size < 32 * 6)
        {
            return std::nullopt;
        }

        const evmc::bytes32 expected_topic = constructEventTopic(
            "ParticleAdded(address,address,string,address,string,string[],string,int32[])");

        if(topics[0] != expected_topic)
        {
            return std::nullopt;
        }

        ParticleAddedEvent event{};
        event.caller = _topicWordToAddress(topics[1]);
        event.owner = _topicWordToAddress(topics[2]);

        const auto name_offset = _readWordAsSizeT(data, data_size, 0);
        const auto particle_address = _readAddressWord(data, data_size, 32);
        const auto feature_offset = _readWordAsSizeT(data, data_size, 64);
        const auto composites_offset = _readWordAsSizeT(data, data_size, 96);
        const auto condition_offset = _readWordAsSizeT(data, data_size, 128);
        const auto condition_args_offset = _readWordAsSizeT(data, data_size, 160);

        if(!name_offset || !particle_address || !feature_offset || !composites_offset || !condition_offset || !condition_args_offset)
        {
            return std::nullopt;
        }

        const auto name = _decodeAbiString(data, data_size, *name_offset);
        const auto feature_name = _decodeAbiString(data, data_size, *feature_offset);
        const auto composite_names = _decodeAbiStringArray(data, data_size, *composites_offset);
        const auto condition_name = _decodeAbiString(data, data_size, *condition_offset);
        const auto condition_args = _decodeAbiInt32Array(data, data_size, *condition_args_offset);

        if(!name || !feature_name || !composite_names || !condition_name || !condition_args)
        {
            return std::nullopt;
        }

        event.name = *name;
        event.particle_address = *particle_address;
        event.feature_name = *feature_name;
        event.composite_names = *composite_names;
        event.condition_name = *condition_name;
        event.condition_args = *condition_args;

        return event;
    }

    std::optional<ParticleAddedEvent> decodeParticleAddedEvent(
        const std::string & data_hex,
        const std::vector<std::string> & topics_hex)
    {
        const auto data_bytes = evmc::from_hex(data_hex);
        if(!data_bytes)
        {
            return std::nullopt;
        }

        const auto topic_words = _decodeTopicWords(topics_hex);
        if(!topic_words)
        {
            return std::nullopt;
        }

        return decodeParticleAddedEvent(
            data_bytes->data(),
            data_bytes->size(),
            topic_words->data(),
            topic_words->size());
    }

    std::optional<FeatureAddedEvent> decodeFeatureAddedEvent(
        const std::uint8_t* data,
        std::size_t data_size,
        const evmc::bytes32 topics[],
        std::size_t num_topics)
    {
        if(data == nullptr || topics == nullptr || num_topics < 1 || data_size < 32 * 5)
        {
            return std::nullopt;
        }

        const evmc::bytes32 expected_topic = constructEventTopic(
            "FeatureAdded(address,string,address,address,uint32)");

        if(topics[0] != expected_topic)
        {
            return std::nullopt;
        }

        const auto caller = _readAddressWord(data, data_size, 0);
        const auto name_offset = _readWordAsSizeT(data, data_size, 32);
        const auto feature_address = _readAddressWord(data, data_size, 64);
        const auto owner = _readAddressWord(data, data_size, 96);
        const auto dimensions_count = _readUint32Word(data, data_size, 128);
        if(!caller || !name_offset || !feature_address || !owner || !dimensions_count)
        {
            return std::nullopt;
        }

        const auto name = _decodeAbiString(data, data_size, *name_offset);
        if(!name)
        {
            return std::nullopt;
        }

        return FeatureAddedEvent{
            .caller = *caller,
            .name = *name,
            .feature_address = *feature_address,
            .owner = *owner,
            .dimensions_count = *dimensions_count
        };
    }

    std::optional<FeatureAddedEvent> decodeFeatureAddedEvent(
        const std::string & data_hex,
        const std::vector<std::string> & topics_hex)
    {
        const auto data_bytes = evmc::from_hex(data_hex);
        if(!data_bytes)
        {
            return std::nullopt;
        }

        const auto topic_words = _decodeTopicWords(topics_hex);
        if(!topic_words)
        {
            return std::nullopt;
        }

        return decodeFeatureAddedEvent(
            data_bytes->data(),
            data_bytes->size(),
            topic_words->data(),
            topic_words->size());
    }

    std::optional<TransformationAddedEvent> decodeTransformationAddedEvent(
        const std::uint8_t* data,
        std::size_t data_size,
        const evmc::bytes32 topics[],
        std::size_t num_topics)
    {
        if(data == nullptr || topics == nullptr || num_topics < 1 || data_size < 32 * 5)
        {
            return std::nullopt;
        }

        const evmc::bytes32 expected_topic = constructEventTopic(
            "TransformationAdded(address,string,address,address,uint32)");

        if(topics[0] != expected_topic)
        {
            return std::nullopt;
        }

        const auto caller = _readAddressWord(data, data_size, 0);
        const auto name_offset = _readWordAsSizeT(data, data_size, 32);
        const auto transformation_address = _readAddressWord(data, data_size, 64);
        const auto owner = _readAddressWord(data, data_size, 96);
        const auto args_count = _readUint32Word(data, data_size, 128);
        if(!caller || !name_offset || !transformation_address || !owner || !args_count)
        {
            return std::nullopt;
        }

        const auto name = _decodeAbiString(data, data_size, *name_offset);
        if(!name)
        {
            return std::nullopt;
        }

        return TransformationAddedEvent{
            .caller = *caller,
            .name = *name,
            .transformation_address = *transformation_address,
            .owner = *owner,
            .args_count = *args_count
        };
    }

    std::optional<TransformationAddedEvent> decodeTransformationAddedEvent(
        const std::string & data_hex,
        const std::vector<std::string> & topics_hex)
    {
        const auto data_bytes = evmc::from_hex(data_hex);
        if(!data_bytes)
        {
            return std::nullopt;
        }

        const auto topic_words = _decodeTopicWords(topics_hex);
        if(!topic_words)
        {
            return std::nullopt;
        }

        return decodeTransformationAddedEvent(
            data_bytes->data(),
            data_bytes->size(),
            topic_words->data(),
            topic_words->size());
    }

    std::optional<ConditionAddedEvent> decodeConditionAddedEvent(
        const std::uint8_t* data,
        std::size_t data_size,
        const evmc::bytes32 topics[],
        std::size_t num_topics)
    {
        if(data == nullptr || topics == nullptr || num_topics < 1 || data_size < 32 * 5)
        {
            return std::nullopt;
        }

        const evmc::bytes32 expected_topic = constructEventTopic(
            "ConditionAdded(address,string,address,address,uint32)");

        if(topics[0] != expected_topic)
        {
            return std::nullopt;
        }

        const auto caller = _readAddressWord(data, data_size, 0);
        const auto name_offset = _readWordAsSizeT(data, data_size, 32);
        const auto condition_address = _readAddressWord(data, data_size, 64);
        const auto owner = _readAddressWord(data, data_size, 96);
        const auto args_count = _readUint32Word(data, data_size, 128);
        if(!caller || !name_offset || !condition_address || !owner || !args_count)
        {
            return std::nullopt;
        }

        const auto name = _decodeAbiString(data, data_size, *name_offset);
        if(!name)
        {
            return std::nullopt;
        }

        return ConditionAddedEvent{
            .caller = *caller,
            .name = *name,
            .condition_address = *condition_address,
            .owner = *owner,
            .args_count = *args_count
        };
    }

    std::optional<ConditionAddedEvent> decodeConditionAddedEvent(
        const std::string & data_hex,
        const std::vector<std::string> & topics_hex)
    {
        const auto data_bytes = evmc::from_hex(data_hex);
        if(!data_bytes)
        {
            return std::nullopt;
        }

        const auto topic_words = _decodeTopicWords(topics_hex);
        if(!topic_words)
        {
            return std::nullopt;
        }

        return decodeConditionAddedEvent(
            data_bytes->data(),
            data_bytes->size(),
            topic_words->data(),
            topic_words->size());
    }

    DeployError _decodeDeployError(const evmc::Result& r)
    {
        if(r.output_data == nullptr || r.output_size < 4)
        {
            return DeployError{};
        }

        std::vector<std::uint8_t> selector;

        selector = constructSelector("ParticleAlreadyRegistered(bytes32)");
        if (std::equal(selector.begin(), selector.end(), r.output_data))
        {
            return DeployError{DeployError::Kind::PARTICLE_ALREADY_REGISTERED};
        }

        selector = constructSelector("ParticleMissing(bytes32)");
        if (std::equal(selector.begin(), selector.end(), r.output_data))
        {
            return DeployError{DeployError::Kind::PARTICLE_MISSING};
        }

        selector = constructSelector("ParticleDimensionsMismatch(bytes32)");
        if (std::equal(selector.begin(), selector.end(), r.output_data))
        {
            return DeployError{DeployError::Kind::PARTICLE_DIMENSIONS_MISMATCH};
        }

        selector = constructSelector("FeatureAlreadyRegistered(bytes32)");
        if (std::equal(selector.begin(), selector.end(), r.output_data))
        {
            return DeployError{DeployError::Kind::FEATURE_ALREADY_REGISTERED};
        }

        selector = constructSelector("FeatureMissing(bytes32)");
        if (std::equal(selector.begin(), selector.end(), r.output_data))
        {
            return DeployError{DeployError::Kind::FEATURE_MISSING};
        }

        selector = constructSelector("TransformationAlreadyRegistered(bytes32)");
        if (std::equal(selector.begin(), selector.end(), r.output_data))
        {
            return DeployError{DeployError::Kind::TRANSFORMATION_ALREADY_REGISTERED};
        }

        selector = constructSelector("TransformationArgumentsMismatch(bytes32)");
        if (std::equal(selector.begin(), selector.end(), r.output_data))
        {
            return DeployError{DeployError::Kind::TRANSFORMATION_ARGUMENTS_MISMATCH};
        }

        selector = constructSelector("TransformationMissing(bytes32)");
        if (std::equal(selector.begin(), selector.end(), r.output_data))
        {
            return DeployError{DeployError::Kind::TRANSFORMATION_MISSING};
        }

        selector = constructSelector("ConditionAlreadyRegistered(bytes32)");
        if (std::equal(selector.begin(), selector.end(), r.output_data))
        {
            return DeployError{DeployError::Kind::CONDITION_ALREADY_REGISTERED};
        }

        selector = constructSelector("ConditionArgumentsMismatch(bytes32)");
        if (std::equal(selector.begin(), selector.end(), r.output_data))
        {
            return DeployError{DeployError::Kind::CONDITION_ARGUMENTS_MISMATCH};
        }

        selector = constructSelector("ConditionMissing(bytes32)");
        if (std::equal(selector.begin(), selector.end(), r.output_data))
        {
            return DeployError{DeployError::Kind::CONDITION_MISSING};
        }

        selector = constructSelector("RegistryError(uint32)");
        if (std::equal(selector.begin(), selector.end(), r.output_data))
        {
            return DeployError{DeployError::Kind::REGISTRY_ERROR};
        }

        return DeployError{};
    }

    ExecuteError _decodeExecuteError(const evmc::Result& r)
    {
        if(r.output_data == nullptr || r.output_size < 4)
        {
            return ExecuteError{};
        }

        std::vector<std::uint8_t> selector;

        selector = constructSelector("ConditionNotMet(bytes32)");
        spdlog::info(evmc::hex(selector.data()));
        if (std::equal(selector.begin(), selector.end(), r.output_data))
        {
            return ExecuteError{ExecuteError::Kind::CONDITION_NOT_MET};
        }

        return ExecuteError{};
    }


    template<>
    std::vector<std::vector<uint32_t>> decodeReturnedValue(const std::vector<std::uint8_t> & bytes)
    {
        assert(bytes.size() % 32 == 0);
        std::vector<std::vector<uint32_t>> result;

        std::size_t base_offset = _readUint256(bytes, 0);  // normally 32
        std::size_t offset = base_offset;

        uint64_t outer_len = _readUint256(bytes, offset);
        offset += 32;

        std::vector<std::size_t> inner_offsets;
        for (uint64_t i = 0; i < outer_len; ++i) {
            std::uint64_t inner_offset = _readUint256(bytes, offset);
            inner_offsets.push_back(inner_offset + base_offset + 32);
            offset += 32;
        }

        for (std::size_t inner_offset : inner_offsets) 
        {
            if (inner_offset + 32 > bytes.size()) {
                throw std::runtime_error("Inner array header out of range");
            }

            std::uint64_t inner_len = _readUint256(bytes, inner_offset);
            inner_offset += 32;

            std::vector<uint32_t> inner_array;
            for (std::uint64_t j = 0; j < inner_len; ++j) {
                std::uint32_t val = _readUint32Padded(bytes, inner_offset + (j * 32));
                inner_array.push_back(val);
            }
            result.push_back(std::move(inner_array));
        }

        return result;
    }

    template<>
    Address decodeReturnedValue(const std::vector<std::uint8_t> & bytes)
    {
        if (bytes.size() < 32)
            throw std::runtime_error("Invalid ABI data: less than 32 bytes");

        Address result;
        // Copy last 20 bytes from the 32-byte word (ABI stores address in the last 20 bytes)
        std::copy(bytes.begin() + 12, bytes.begin() + 32, result.bytes);
        return result;
    }


    template<>
    std::vector<Samples> decodeReturnedValue(const std::vector<std::uint8_t>& bytes)
    {
        std::vector<Samples> result;

        // Step 1: read base offset to array
        std::size_t array_base = _readUint256(bytes, 0);  // should be 32

        // Step 2: read array length
        std::size_t array_len = _readUint256(bytes, array_base);  // at offset 32

        // Step 3: read offsets to structs (relative to array_base)
        std::vector<std::size_t> struct_offsets;
        for (std::size_t i = 0; i < array_len; ++i) {
            std::size_t struct_rel_offset = _readUint256(bytes, (array_base + 32) + (i * 32));
            struct_offsets.push_back((array_base + 32) + struct_rel_offset);
        }

        // Step 4: parse each struct
        for (std::size_t struct_offset : struct_offsets) 
        {
            Samples samples;

            std::size_t path_rel = _readUint256(bytes, struct_offset);
            std::size_t data_rel = _readUint256(bytes, struct_offset + 32);

            // 4.2: resolve actual offsets
            std::size_t path_offset = struct_offset + path_rel;
            std::size_t data_offset = struct_offset + data_rel;

            // 4.3: read string length and content
            std::size_t str_len = _readUint256(bytes, path_offset);

            samples.set_path(std::string(
                reinterpret_cast<const char*>(&bytes[path_offset + 32]),
                str_len
            ));

            // 4.4: read data length and entries
            std::size_t data_len = _readUint256(bytes, data_offset);
            std::vector<std::uint32_t> data;
            for (std::size_t j = 0; j < data_len; ++j) {
                std::uint32_t val = _readUint32Padded(bytes, data_offset + 32 + j * 32);
                samples.add_data(val);
            }

            result.emplace_back(std::move(samples));
        }

        return result;
    }

    asio::awaitable<std::expected<std::vector<std::uint8_t>, ExecuteError>> fetchOwner(EVM & evm, const Address & address)
    {
        spdlog::debug(std::format("Fetching contract owner: {}", address));
        std::vector<uint8_t> input_data;
        const auto selector = constructSelector("getOwner()");
        input_data.insert(input_data.end(), selector.begin(), selector.end());
        co_return co_await evm.execute(evm.getRegistryAddress(), address, input_data, 1'000'000, 0);
    }

    EVM::EVM(asio::io_context & io_context, evmc_revision rev, std::filesystem::path solc_path, std::filesystem::path pt_path)
    :   _vm(evmc_create_evmone()),
        _rev(rev),
        _strand(asio::make_strand(io_context)),
        _solc_path(std::move(solc_path)),
        _pt_path(std::move(pt_path)),
        _storage(_vm, _rev)
    {
        if (!_vm)
        {
            throw std::runtime_error("Failed to create EVM instance");
        }

        _vm.set_option("O", "0"); // disable optimizations

        // Initialize the genesis account
        std::memcpy(_genesis_address.bytes + (20 - 7), "genesis", 7);
        addAccount(_genesis_address, DEFAULT_GAS_LIMIT);
        spdlog::info(std::format("Genesis address: {}", _genesis_address));

        // Initialize console log account
        std::memcpy(_console_log_address.bytes + (20 - 11), "console.log", 11);
        addAccount(_console_log_address, DEFAULT_GAS_LIMIT);

        co_spawn(io_context, loadPT(), [](std::exception_ptr e, bool r){
            if(e || !r)
            {
                spdlog::error("Failed to load PT");
                throw std::runtime_error("Failed to load PT");
            }
        });
    }
    
    Address EVM::getRegistryAddress() const
    {
        return _registry_address;
    }

    Address EVM::getRunnerAddress() const
    {
        return _runner_address;
    }

    const std::filesystem::path & EVM::getSolcPath() const 
    {
        return _solc_path;
    }

    const std::filesystem::path & EVM::getPTPath() const 
    {
        return _pt_path;
    }

    asio::awaitable<bool> EVM::addAccount(Address address, std::uint64_t initial_gas) noexcept
    {
        co_await utils::ensureOnStrand(_strand);

        if(_storage.account_exists(address))
        {
            spdlog::warn(std::format("addAccount: Account {} already exists", evmc::hex(address)));
            co_return false;
        }

        if(_storage.add_account(address))
        {
            _storage.set_balance(address, initial_gas);
        }
        else
        {
            co_return false;
        }

        co_return true;
    }

    asio::awaitable<bool> EVM::setGas(Address address, std::uint64_t gas) noexcept
    {
        co_await utils::ensureOnStrand(_strand);

        if(!_storage.account_exists(address))
        {
            spdlog::warn(std::format("addAccount: Account {} does not exist", evmc::hex(address)));
            co_return false;
        }

        _storage.set_balance(address, gas);
        co_return true;
    }

    asio::awaitable<bool> EVM::compile(std::filesystem::path code_path, std::filesystem::path out_dir, std::filesystem::path base_path, std::filesystem::path includes) const noexcept
    {
        co_await utils::ensureOnStrand(_strand);

        if(!std::filesystem::exists(code_path))
        {
            spdlog::error(std::format("File {} does not exist", code_path.string()));
            co_return false;
        }

        std::vector<std::string> args = {
            "--evm-version", "shanghai",
            "--overwrite", "-o", out_dir.string(),
            "--optimize", "--bin",
            "--abi",
            code_path.string()
        };

        if(!includes.empty() && base_path.empty())
        {
            spdlog::error("Base path must be specified if includes are specified");
            co_return false;
        }

        if (!base_path.empty()) 
        {
            args.emplace_back("--base-path");
            args.emplace_back(base_path.string());
        }

        if (!includes.empty()) 
        {
            args.emplace_back("--include-path");
            args.emplace_back(includes.string());
        }

        const auto [exit_code, compile_result] = dcn::native::runProcess(_solc_path.string(), std::move(args));

        spdlog::info("Solc exited with code {},\n{}\n{}", exit_code, code_path.string(), compile_result);

        if(exit_code != 0)
        {
            co_return false;
        }
        
        co_return true;
    }

    asio::awaitable<std::expected<Address, DeployError>> EVM::deploy(
                        std::istream & code_stream,
                        Address sender,
                        std::vector<std::uint8_t> constructor_args, 
                        std::uint64_t gas_limit,
                        std::uint64_t value) noexcept
    {
        const std::string code_hex = std::string(std::istreambuf_iterator<char>(code_stream), std::istreambuf_iterator<char>());
        const std::optional<evmc::bytes> bytecode_result = evmc::from_hex(code_hex);
        if(!bytecode_result)
        {
            spdlog::error("Cannot parse bytecode");
            co_return std::unexpected(DeployError{DeployError::Kind::INVALID_BYTECODE});
        }
        const auto & bytecode = *bytecode_result;

        if(bytecode.size() == 0)
        {
            spdlog::error("Empty bytecode");
            co_return std::unexpected(DeployError{DeployError::Kind::INVALID_BYTECODE});
        }

        if(!constructor_args.empty())
        {
            std::string hex_str;
            for(const std::uint8_t & b : constructor_args)
            {
                hex_str += evmc::hex(b);
            }

            spdlog::debug(std::format("Constructor args: {}", hex_str));
        }

        std::vector<uint8_t> deployment_input;
        deployment_input.reserve(bytecode.size() + constructor_args.size());
        deployment_input.insert(deployment_input.end(), bytecode.begin(), bytecode.end());
        deployment_input.insert(deployment_input.end(), constructor_args.begin(), constructor_args.end());

        evmc_message create_msg{};
        create_msg.kind       = EVMC_CREATE2;
        create_msg.sender     = sender;
        std::memcpy(create_msg.sender.bytes, sender.bytes, 20);
        
        create_msg.gas        = gas_limit;
        create_msg.input_data = deployment_input.data();
        create_msg.input_size = deployment_input.size();

        // fill message salt
        std::string salt_str = "message_salt_42";
        crypto::Keccak256::getHash(reinterpret_cast<const uint8_t*>(salt_str.data()), salt_str.size(), create_msg.create2_salt.bytes);

        // fill message value
        evmc_uint256be value256{};
        std::memcpy(&value256.bytes[24], &value, sizeof(value));  // Big endian: last 8 bytes hold the value
        create_msg.value = value256;

        co_await utils::ensureOnStrand(_strand);

        const evmc::Result result = _storage.call(create_msg);        

        if (result.status_code != EVMC_SUCCESS)
        {
            const DeployError error = _decodeDeployError(result);
            spdlog::error(std::format("Failed to deploy contract: {}, error: {}", result.status_code, error.kind));
            co_return std::unexpected(error);
        }

        // Display result
        spdlog::info("EVM deployment status: {}", evmc_status_code_to_string(result.status_code));
        spdlog::info("Gas left: {}", result.gas_left);

        if (result.output_data){
            spdlog::debug("Output size: {}", result.output_size);
        }

        co_return result.create_address;
     }

    asio::awaitable<std::expected<Address, DeployError>> EVM::deploy(
                        std::filesystem::path code_path,
                        Address sender,
                        std::vector<uint8_t> constructor_args,
                        std::uint64_t gas_limit,
                        std::uint64_t value) noexcept
    {
        spdlog::debug(std::format("Deploying contract from file: {}", code_path.string()));
        std::ifstream file(code_path, std::ios::binary);
        co_return co_await deploy(file, std::move(sender), std::move(constructor_args),  gas_limit, value);
    }

    asio::awaitable<std::expected<std::vector<std::uint8_t>, ExecuteError>> EVM::execute(
                    Address sender,
                    Address recipient,
                    std::vector<std::uint8_t> input_bytes,
                    std::uint64_t gas_limit,
                    std::uint64_t value) noexcept
    {
        if(std::ranges::all_of(recipient.bytes, [](uint8_t b) { return b == 0; }))
        {
            spdlog::error("Cannot create a contract with execute function. Use dedicated deploy method.");
            co_return std::unexpected(ExecuteError{ExecuteError::Kind::UNKNOWN});
        }

        evmc_message msg{};
        msg.gas = gas_limit;
        msg.kind = EVMC_CALL;
        msg.sender = sender;
        msg.recipient = recipient;

        if(!input_bytes.empty())
        {
            msg.input_data = input_bytes.data();
            msg.input_size = input_bytes.size();
        }
        else
        {
            msg.input_data = nullptr;
            msg.input_size = 0;
        }

        evmc_uint256be value256{};
        std::memcpy(&value256.bytes[24], &value, sizeof(value));  // Big endian: last 8 bytes hold the value
        msg.value = value256;

        co_await utils::ensureOnStrand(_strand);
        evmc::Result result = _storage.call(msg);
        
        if (result.status_code != EVMC_SUCCESS)
        {
            const ExecuteError error = _decodeExecuteError(result);
            std::string output_hex = "<empty>";
            if(result.output_data != nullptr && result.output_size > 0)
            {
                output_hex = evmc::hex(evmc::bytes_view{result.output_data, result.output_size});
            }

            spdlog::error(std::format("Failed to execute contract: {}, error: {} {}", result.status_code, error.kind, output_hex));
            co_return std::unexpected(error);
        }

        // Display result
        spdlog::info("EVM execution status: {}", evmc_status_code_to_string(result.status_code));
        spdlog::info("Gas left: {}", result.gas_left);

        if (result.output_data){
            spdlog::debug("Output size: {}", result.output_size);
        }

        co_return std::vector<std::uint8_t>(result.output_data, result.output_data + result.output_size);
    }

    asio::awaitable<bool> EVM::loadPT()
    { 
        const auto  contracts_dir = _pt_path    / "contracts";
        const auto node_modules = _pt_path      / "node_modules";
        const auto  out_dir = _pt_path          / "out";
        const auto proxy_out_dir = out_dir      / "proxy";

        std::filesystem::create_directories(out_dir);
        
        { // deploy registry implementation + proxy
            if(co_await compile(
                    contracts_dir / "registry" / "RegistryBase.sol",
                    out_dir / "registry", 
                    contracts_dir,
                    node_modules) == false)
            {
                spdlog::error("Failed to compile registry");
                co_return false;
            }

            if(co_await compile(
                    contracts_dir / "proxy" / "PTRegistryProxy.sol",
                    proxy_out_dir, 
                    contracts_dir,
                    node_modules) == false)
            {
                spdlog::error("Failed to compile registry proxy");
                co_return false;
            }

            const auto registry_impl_address_res = co_await deploy(
                    out_dir / "registry" / "RegistryBase.bin", 
                    _genesis_address,
                    {}, 
                    DEFAULT_GAS_LIMIT, 
                    0);

            if(!registry_impl_address_res)
                co_return false;

            const auto registry_impl_address = registry_impl_address_res.value();
            spdlog::info("Registry implementation address: {}", evmc::hex(registry_impl_address));

            const auto registry_proxy_address_res = co_await deploy(
                    proxy_out_dir / "PTRegistryProxy.bin",
                    _genesis_address,
                    encodeAsArg(registry_impl_address),
                    DEFAULT_GAS_LIMIT,
                    0);

            if(!registry_proxy_address_res)
                co_return false;

            _registry_address = registry_proxy_address_res.value();
            spdlog::info("Registry proxy address: {}", evmc::hex(_registry_address));
        }

        { // deploy runner implementation + proxy
            if(co_await compile(
                    contracts_dir / "runner" /  "Runner.sol",
                    out_dir / "runner", 
                    contracts_dir,
                    node_modules) == false)
            {
                spdlog::error("Failed to compile runner");
                co_return false;
            }

            if(co_await compile(
                    contracts_dir / "proxy" / "PTContractProxy.sol",
                    proxy_out_dir, 
                    contracts_dir,
                    node_modules) == false)
            {
                spdlog::error("Failed to compile contract proxy");
                co_return false;
            }
            
            spdlog::debug("Deploy runner implementation");
            const auto runner_impl_address_res = co_await deploy(
                    out_dir / "runner" / "Runner.bin", 
                    _genesis_address,
                    {}, 
                    DEFAULT_GAS_LIMIT, 
                    0);

            if(!runner_impl_address_res)
                co_return false;

            const auto runner_impl_address = runner_impl_address_res.value();
            spdlog::info("Runner implementation address: {}", evmc::hex(runner_impl_address));

            std::vector<std::uint8_t> runner_proxy_ctor_args = encodeAsArg(runner_impl_address);
            const auto registry_arg = encodeAsArg(_registry_address);
            runner_proxy_ctor_args.insert(runner_proxy_ctor_args.end(), registry_arg.begin(), registry_arg.end());

            spdlog::debug("Deploy runner proxy");
            const auto runner_proxy_address_res = co_await deploy(
                    proxy_out_dir / "PTContractProxy.bin",
                    _genesis_address,
                    std::move(runner_proxy_ctor_args),
                    DEFAULT_GAS_LIMIT,
                    0);

            if(!runner_proxy_address_res)
                co_return false;
        
            _runner_address = runner_proxy_address_res.value();
            spdlog::info("Runner proxy address: {}", evmc::hex(_runner_address));
        }

        co_return true;
    }

}
