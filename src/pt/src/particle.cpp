#include <format>

#include "crypto.hpp"

#include "particle.hpp"

namespace dcn
{
    std::string constructParticleSolidityCode(const Particle & particle)
    {
        std::string composites_code;
        std::string condition_args_code;

        /*
            conditionArgs[0] = 1;
            conditionArgs[1] = 2;
            ...
        */
        for(unsigned int i = 0; i < particle.condition_args_size(); i++)
        {
            condition_args_code += std::format("conditionArgs[{}] = int32({});\n", i, particle.condition_args().at(i));
        }

        /*
            composites[0] = "a";
            composites[1] = "b";
            ...
        */
        for(unsigned int i = 0; i < particle.composite_names_size(); i++)
        {
            composites_code += std::format("composites[{}] = \"{}\";\n", i, particle.composite_names().at(i));
        }

        return std::format(
            "//SPDX-License-Identifier: MIT\n"
            "pragma solidity >=0.8.2 <0.9.0;\n"
            "import \"particle/ParticleBase.sol\";\n"
            "contract {0} is ParticleBase{{\n"

            "function _composites() internal pure returns (string[] memory composites) {{"
            "composites = new string[]({1});"
            "{2}"
            "}}\n"
            
            "function _conditionArgs() internal pure returns (int32[] memory conditionArgs) {{"
            "conditionArgs = new int32[]({3});"
            "{4}"
            "}}\n"

            "function initialize(address registryAddr) external initializer {{\n"
            "__ParticleBase_init(registryAddr, \"{0}\", \"{5}\", _composites(), \"{6}\", _conditionArgs());\n"
            "}}\n"
            "\n}}",
            
            // def
            particle.name(), // 0
            //_composites()
            particle.composite_names_size(), // 1
            std::move(composites_code), // 2
            //_conditionArgs()
            particle.condition_args_size(), // 3
            std::move(condition_args_code), // 4

            // constructor
            particle.feature_name(), // 5
            particle.condition_name() // 6
        );

    }
}

namespace dcn::pt
{
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

        const evmc::bytes32 expected_topic = chain::constructEventTopic(
            "ParticleAdded(address,address,string,address,string,string[],string,int32[])");

        if(topics[0] != expected_topic)
        {
            return std::nullopt;
        }

        ParticleAddedEvent event{};
        event.caller = chain::topicWordToAddress(topics[1]);
        event.owner = chain::topicWordToAddress(topics[2]);

        const auto name_offset = chain::readWordAsSizeT(data, data_size, 0);
        const auto particle_address = chain::readAddressWord(data, data_size, 32);
        const auto feature_offset = chain::readWordAsSizeT(data, data_size, 64);
        const auto composites_offset = chain::readWordAsSizeT(data, data_size, 96);
        const auto condition_offset = chain::readWordAsSizeT(data, data_size, 128);
        const auto condition_args_offset = chain::readWordAsSizeT(data, data_size, 160);

        if(!name_offset || !particle_address || !feature_offset || !composites_offset || !condition_offset || !condition_args_offset)
        {
            return std::nullopt;
        }

        const auto name = chain::decodeAbiString(data, data_size, *name_offset);
        const auto feature_name = chain::decodeAbiString(data, data_size, *feature_offset);
        const auto composite_names = chain::decodeAbiStringArray(data, data_size, *composites_offset);
        const auto condition_name = chain::decodeAbiString(data, data_size, *condition_offset);
        const auto condition_args = chain::decodeAbiInt32Array(data, data_size, *condition_args_offset);

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

        const auto topic_words = chain::decodeTopicWords(topics_hex);
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
}

namespace dcn::parse
{
    template<>
    Result<json> parseToJson(Particle particle, use_json_t)
    {
        json json_obj = json::object();
        json_obj["name"] = particle.name();
        json_obj["feature_name"] = particle.feature_name();

        json_obj["composite_names"] = json::array();
        for (const auto & composite_name : particle.composite_names())
        {
            json_obj["composite_names"].emplace_back(composite_name);
        }

        json_obj["condition_name"] = particle.condition_name();

        json_obj["condition_args"] = json::array();
        for (const auto & condition_arg : particle.condition_args())
        {
            json_obj["condition_args"].emplace_back(condition_arg);
        }

        return json_obj;
    }

    template<>
    Result<Particle> parseFromJson(json json_obj, use_json_t)
    {
        Particle particle;

        if (json_obj.contains("name")) {
            particle.set_name(json_obj["name"].get<std::string>());
        }
        else return std::unexpected(ParseError{ParseError::Kind::INVALID_VALUE, "invalid name"});

        if (json_obj.contains("feature_name")) {
            particle.set_feature_name(json_obj["feature_name"].get<std::string>());
        }
        else return std::unexpected(ParseError{ParseError::Kind::INVALID_VALUE, "invalid feature_name"});

        if (json_obj.contains("composite_names")) {
            for (const std::string & composite_name : json_obj["composite_names"].get<std::vector<std::string>>())
            {
                particle.add_composite_names(composite_name);
            }
        }
        else return std::unexpected(ParseError{ParseError::Kind::INVALID_VALUE, "invalid composite_names"});

        if (json_obj.contains("condition_name")) {
            particle.set_condition_name(json_obj["condition_name"].get<std::string>());
        }
        else return std::unexpected(ParseError{ParseError::Kind::INVALID_VALUE, "invalid condition_name"});

        if (json_obj.contains("condition_args")) {
            for (const int32_t & condition_arg : json_obj["condition_args"].get<std::vector<int32_t>>())
            {
                particle.add_condition_args(condition_arg);
            }
        }
        else return std::unexpected(ParseError{ParseError::Kind::INVALID_VALUE, "invalid condition_args"});

        return particle;
    }

    template<>
    Result<std::string> parseToJson(Particle particle, use_protobuf_t)
    {
        google::protobuf::util::JsonPrintOptions options;
        options.add_whitespace = true;
        options.preserve_proto_field_names = true; // Use snake_case from proto
        options.always_print_fields_with_no_presence = true;

        std::string json_str;
        auto status = google::protobuf::util::MessageToJsonString(particle, &json_str, options);

        if (!status.ok()) return std::unexpected(ParseError{ParseError::Kind::INVALID_VALUE, "invalid particle"});

        return json_str;
    }

    template<>
    Result<Particle> parseFromJson(std::string json_str, use_protobuf_t)
    {
        google::protobuf::util::JsonParseOptions options;

        Particle particle;

        auto status = google::protobuf::util::JsonStringToMessage(json_str, &particle, options);

        if(!status.ok()) return std::unexpected(ParseError{ParseError::Kind::INVALID_VALUE, "invalid particle"});

        return particle;
    }

    template<>
    Result<json> parseToJson(ParticleRecord particle_record, use_json_t)
    {
        json json_obj = json::object();

        auto particle_result = parseToJson(particle_record.particle(), use_json);
        if(!particle_result) return std::unexpected(ParseError{ParseError::Kind::INVALID_VALUE, "invalid particle"});

        json_obj["particle"] = std::move(*particle_result);
        json_obj["owner"] = particle_record.owner();
        return json_obj;
    }

    template<>
    Result<std::string> parseToJson(ParticleRecord particle_record, use_protobuf_t)
    {
        google::protobuf::util::JsonPrintOptions options;
        options.add_whitespace = true;
        options.preserve_proto_field_names = true; // Use snake_case from proto
        options.always_print_fields_with_no_presence = true;

        std::string json_str;
        auto status = google::protobuf::util::MessageToJsonString(particle_record, &json_str, options);

        if (!status.ok()) return std::unexpected(ParseError{ParseError::Kind::INVALID_VALUE, "invalid particle record"});

        return json_str;
    }

    template<>
    Result<ParticleRecord> parseFromJson(json json_obj, use_json_t)
    {
        ParticleRecord particle_record;
        if (json_obj.contains("particle") == false)
            return std::unexpected(ParseError{ParseError::Kind::INVALID_VALUE, "invalid particle"});

        auto particle = parseFromJson<Particle>(json_obj["particle"], use_json);

        if(!particle)
            return std::unexpected(ParseError{ParseError::Kind::INVALID_VALUE, "invalid particle"});

        *particle_record.mutable_particle() = std::move(*particle);

        if (json_obj.contains("owner") == false)
            return std::unexpected(ParseError{ParseError::Kind::INVALID_VALUE, "invalid owner"});
        
        particle_record.set_owner(json_obj["owner"].get<std::string>());

        return particle_record;
    }

    template<>
    Result<ParticleRecord> parseFromJson(std::string json_str, use_protobuf_t)
    {
        google::protobuf::util::JsonParseOptions options;
        options.ignore_unknown_fields = true;

        ParticleRecord particle_record;

        auto status = google::protobuf::util::JsonStringToMessage(json_str, &particle_record, options);

        if(!status.ok()) return std::unexpected(ParseError{ParseError::Kind::INVALID_VALUE, "invalid particle record"});

        return particle_record;
    }
}
