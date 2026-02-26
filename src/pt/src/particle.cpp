#include <algorithm>
#include <format>
#include <limits>
#include <ranges>
#include <vector>

#include "crypto.hpp"

#include "particle.hpp"

namespace dcn
{
    std::string constructParticleSolidityCode(const Particle & particle)
    {
        std::string composite_dim_ids_code;
        std::string composite_names_code;
        std::string condition_args_code;

        std::vector<std::pair<std::uint32_t, std::string>> sorted_composites;
        sorted_composites.reserve(particle.composites().size());
        for(const auto & [dim_id, composite_name] : particle.composites())
        {
            sorted_composites.emplace_back(dim_id, composite_name);
        }
        std::ranges::sort(sorted_composites, [](const auto & lhs, const auto & rhs)
        {
            return lhs.first < rhs.first;
        });

        /*
            conditionArgs[0] = 1;
            conditionArgs[1] = 2;
            ...
        */
        for(unsigned int i = 0; i < particle.condition_args_size(); i++)
        {
            condition_args_code += std::format("conditionArgs[{}] = int32({});\n", i, particle.condition_args().at(i));
        }

        for(std::size_t i = 0; i < sorted_composites.size(); ++i)
        {
            composite_dim_ids_code += std::format("compositeDimIds[{}] = uint32({});\n", i, sorted_composites[i].first);
            composite_names_code += std::format("compositeNames[{}] = \"{}\";\n", i, sorted_composites[i].second);
        }

        return std::format(
            "//SPDX-License-Identifier: MIT\n"
            "pragma solidity >=0.8.2 <0.9.0;\n"
            "import \"particle/ParticleBase.sol\";\n"
            "contract {0} is ParticleBase{{\n"

            "function _compositeDimIds() internal pure returns (uint32[] memory compositeDimIds) {{"
            "compositeDimIds = new uint32[]({1});"
            "{2}"
            "}}\n"

            "function _compositeNames() internal pure returns (string[] memory compositeNames) {{"
            "compositeNames = new string[]({1});"
            "{3}"
            "}}\n"
             
            "function _conditionArgs() internal pure returns (int32[] memory conditionArgs) {{"
            "conditionArgs = new int32[]({4});"
            "{5}"
            "}}\n"

            "function initialize(address registryAddr) external initializer {{\n"
            "__ParticleBase_init(registryAddr, \"{0}\", \"{6}\", _compositeDimIds(), _compositeNames(), \"{7}\", _conditionArgs());\n"
            "}}\n"
            "\n}}",
             
            // def
            particle.name(), // 0
            //_compositeDimIds()/ _compositeNames()
            sorted_composites.size(), // 1
            std::move(composite_dim_ids_code), // 2
            std::move(composite_names_code), // 3
            //_conditionArgs()
            particle.condition_args_size(), // 4
            std::move(condition_args_code), // 5

            // constructor
            particle.feature_name(), // 6
            particle.condition_name() // 7
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
        if(data == nullptr || topics == nullptr || num_topics < 3 || data_size < 32 * 7)
        {
            return std::nullopt;
        }

        const evmc::bytes32 expected_topic = chain::constructEventTopic(
            "ParticleAdded(address,address,string,address,string,uint32[],string[],string,int32[])");

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
        const auto composite_dim_ids_offset = chain::readWordAsSizeT(data, data_size, 96);
        const auto composite_names_offset = chain::readWordAsSizeT(data, data_size, 128);
        const auto condition_offset = chain::readWordAsSizeT(data, data_size, 160);
        const auto condition_args_offset = chain::readWordAsSizeT(data, data_size, 192);

        if(!name_offset || !particle_address || !feature_offset || !composite_dim_ids_offset || !composite_names_offset || !condition_offset || !condition_args_offset)
        {
            return std::nullopt;
        }

        const auto name = chain::decodeAbiString(data, data_size, *name_offset);
        const auto feature_name = chain::decodeAbiString(data, data_size, *feature_offset);
        const auto composite_dim_ids = chain::decodeAbiUint32Array(data, data_size, *composite_dim_ids_offset);
        const auto composite_names = chain::decodeAbiStringArray(data, data_size, *composite_names_offset);
        const auto condition_name = chain::decodeAbiString(data, data_size, *condition_offset);
        const auto condition_args = chain::decodeAbiInt32Array(data, data_size, *condition_args_offset);

        if(!name || !feature_name || !composite_dim_ids || !composite_names || !condition_name || !condition_args)
        {
            return std::nullopt;
        }

        if(composite_dim_ids->size() != composite_names->size())
        {
            return std::nullopt;
        }

        event.name = *name;
        event.particle_address = *particle_address;
        event.feature_name = *feature_name;
        event.condition_name = *condition_name;
        event.condition_args = *condition_args;

        for(std::size_t i = 0; i < composite_dim_ids->size(); ++i)
        {
            if(composite_names->at(i).empty())
            {
                return std::nullopt;
            }

            const auto [_, inserted] = event.composites.try_emplace(composite_dim_ids->at(i), composite_names->at(i));
            if(!inserted)
            {
                return std::nullopt;
            }
        }

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

        json_obj["composites"] = json::object();

        std::vector<std::pair<std::uint32_t, std::string>> sorted_composites;
        sorted_composites.reserve(particle.composites().size());
        for(const auto & [dim_id, composite_name] : particle.composites())
        {
            sorted_composites.emplace_back(dim_id, composite_name);
        }
        std::ranges::sort(sorted_composites, [](const auto & lhs, const auto & rhs)
        {
            return lhs.first < rhs.first;
        });

        for(const auto & [dim_id, composite_name] : sorted_composites)
        {
            json_obj["composites"][std::to_string(dim_id)] = composite_name;
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
            return std::unexpected(ParseError{ParseError::Kind::INVALID_VALUE, "composite_names is deprecated; use composites"});
        }

        if (json_obj.contains("composites") && json_obj["composites"].is_object()) {
            for(const auto & [key, value] : json_obj["composites"].items())
            {
                if(!value.is_string())
                {
                    return std::unexpected(ParseError{ParseError::Kind::INVALID_VALUE, "invalid composites"});
                }

                std::size_t consumed = 0;
                std::uint32_t dim_id = 0;
                try
                {
                    const unsigned long raw_dim_id = std::stoul(key, &consumed, 10);
                    if(consumed != key.size() || raw_dim_id > static_cast<unsigned long>(std::numeric_limits<std::uint32_t>::max()))
                    {
                        return std::unexpected(ParseError{ParseError::Kind::INVALID_VALUE, "invalid composites"});
                    }
                    dim_id = static_cast<std::uint32_t>(raw_dim_id);
                }
                catch(const std::exception &)
                {
                    return std::unexpected(ParseError{ParseError::Kind::INVALID_VALUE, "invalid composites"});
                }

                const std::string composite_name = value.get<std::string>();
                if(composite_name.empty())
                {
                    return std::unexpected(ParseError{ParseError::Kind::INVALID_VALUE, "invalid composites"});
                }

                (*particle.mutable_composites())[dim_id] = composite_name;
            }
        }
        else return std::unexpected(ParseError{ParseError::Kind::INVALID_VALUE, "invalid composites"});

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
