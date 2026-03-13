#include <algorithm>
#include <format>
#include <limits>
#include <ranges>
#include <vector>

#include "crypto.hpp"

#include "connector.hpp"

namespace dcn
{
    std::string constructConnectorSolidityCode(const Connector & connector)
    {
        std::string composite_dim_ids_code;
        std::string composite_names_code;
        std::string condition_args_code;

        std::vector<std::pair<std::uint32_t, std::string>> sorted_composites;
        sorted_composites.reserve(connector.composites().size());
        for(const auto & [dim_id, composite_name] : connector.composites())
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
        for(unsigned int i = 0; i < connector.condition_args_size(); i++)
        {
            condition_args_code += std::format("conditionArgs[{}] = int32({});\n", i, connector.condition_args().at(i));
        }

        for(std::size_t i = 0; i < sorted_composites.size(); ++i)
        {
            composite_dim_ids_code += std::format("compositeDimIds[{}] = uint32({});\n", i, sorted_composites[i].first);
            composite_names_code += std::format("compositeNames[{}] = \"{}\";\n", i, sorted_composites[i].second);
        }

        return std::format(
            "//SPDX-License-Identifier: MIT\n"
            "pragma solidity >=0.8.2 <0.9.0;\n"
            "import \"connector/ConnectorBase.sol\";\n"
            "contract {0} is ConnectorBase{{\n"

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
            "__ConnectorBase_init(registryAddr, \"{0}\", \"{6}\", _compositeDimIds(), _compositeNames(), \"{7}\", _conditionArgs());\n"
            "}}\n"
            "\n}}",
             
            // def
            connector.name(), // 0
            //_compositeDimIds()/ _compositeNames()
            sorted_composites.size(), // 1
            std::move(composite_dim_ids_code), // 2
            std::move(composite_names_code), // 3
            //_conditionArgs()
            connector.condition_args_size(), // 4
            std::move(condition_args_code), // 5

            // constructor
            connector.feature_name(), // 6
            connector.condition_name() // 7
        );

    }
}

namespace dcn::pt
{
    std::optional<ConnectorAddedEvent> decodeConnectorAddedEvent(
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
            "ConnectorAdded(address,address,string,address,string,uint32[],string[],string,int32[])");

        if(topics[0] != expected_topic)
        {
            return std::nullopt;
        }

        ConnectorAddedEvent event{};
        event.caller = chain::topicWordToAddress(topics[1]);
        event.owner = chain::topicWordToAddress(topics[2]);

        const auto name_offset = chain::readWordAsSizeT(data, data_size, 0);
        const auto connector_address = chain::readAddressWord(data, data_size, 32);
        const auto feature_offset = chain::readWordAsSizeT(data, data_size, 64);
        const auto composite_dim_ids_offset = chain::readWordAsSizeT(data, data_size, 96);
        const auto composite_names_offset = chain::readWordAsSizeT(data, data_size, 128);
        const auto condition_offset = chain::readWordAsSizeT(data, data_size, 160);
        const auto condition_args_offset = chain::readWordAsSizeT(data, data_size, 192);

        if(!name_offset || !connector_address || !feature_offset || !composite_dim_ids_offset || !composite_names_offset || !condition_offset || !condition_args_offset)
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
        event.connector_address = *connector_address;
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

    std::optional<ConnectorAddedEvent> decodeConnectorAddedEvent(
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

        return decodeConnectorAddedEvent(
            data_bytes->data(),
            data_bytes->size(),
            topic_words->data(),
            topic_words->size());
    }
}

namespace dcn::parse
{
    template<>
    Result<json> parseToJson(Connector connector, use_json_t)
    {
        json json_obj = json::object();
        json_obj["name"] = connector.name();
        json_obj["feature_name"] = connector.feature_name();

        json_obj["composites"] = json::object();

        std::vector<std::pair<std::uint32_t, std::string>> sorted_composites;
        sorted_composites.reserve(connector.composites().size());
        for(const auto & [dim_id, composite_name] : connector.composites())
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

        json_obj["condition_name"] = connector.condition_name();

        json_obj["condition_args"] = json::array();
        for (const auto & condition_arg : connector.condition_args())
        {
            json_obj["condition_args"].emplace_back(condition_arg);
        }

        return json_obj;
    }

    template<>
    Result<Connector> parseFromJson(json json_obj, use_json_t)
    {
        Connector connector;

        if (json_obj.contains("name")) {
            connector.set_name(json_obj["name"].get<std::string>());
        }
        else return std::unexpected(ParseError{ParseError::Kind::INVALID_VALUE, "invalid name"});

        if (json_obj.contains("feature_name")) {
            connector.set_feature_name(json_obj["feature_name"].get<std::string>());
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

                (*connector.mutable_composites())[dim_id] = composite_name;
            }
        }
        else return std::unexpected(ParseError{ParseError::Kind::INVALID_VALUE, "invalid composites"});

        if (json_obj.contains("condition_name")) {
            connector.set_condition_name(json_obj["condition_name"].get<std::string>());
        }
        else return std::unexpected(ParseError{ParseError::Kind::INVALID_VALUE, "invalid condition_name"});

        if (json_obj.contains("condition_args")) {
            for (const int32_t & condition_arg : json_obj["condition_args"].get<std::vector<int32_t>>())
            {
                connector.add_condition_args(condition_arg);
            }
        }
        else return std::unexpected(ParseError{ParseError::Kind::INVALID_VALUE, "invalid condition_args"});

        return connector;
    }

    template<>
    Result<std::string> parseToJson(Connector connector, use_protobuf_t)
    {
        google::protobuf::util::JsonPrintOptions options;
        options.add_whitespace = true;
        options.preserve_proto_field_names = true; // Use snake_case from proto
        options.always_print_fields_with_no_presence = true;

        std::string json_str;
        auto status = google::protobuf::util::MessageToJsonString(connector, &json_str, options);

        if (!status.ok()) return std::unexpected(ParseError{ParseError::Kind::INVALID_VALUE, "invalid connector"});

        return json_str;
    }

    template<>
    Result<Connector> parseFromJson(std::string json_str, use_protobuf_t)
    {
        google::protobuf::util::JsonParseOptions options;

        Connector connector;

        auto status = google::protobuf::util::JsonStringToMessage(json_str, &connector, options);

        if(!status.ok()) return std::unexpected(ParseError{ParseError::Kind::INVALID_VALUE, "invalid connector"});

        return connector;
    }

    template<>
    Result<json> parseToJson(ConnectorRecord connector_record, use_json_t)
    {
        json json_obj = json::object();

        auto connector_result = parseToJson(connector_record.connector(), use_json);
        if(!connector_result) return std::unexpected(ParseError{ParseError::Kind::INVALID_VALUE, "invalid connector"});

        json_obj["connector"] = std::move(*connector_result);
        json_obj["owner"] = connector_record.owner();
        return json_obj;
    }

    template<>
    Result<std::string> parseToJson(ConnectorRecord connector_record, use_protobuf_t)
    {
        google::protobuf::util::JsonPrintOptions options;
        options.add_whitespace = true;
        options.preserve_proto_field_names = true; // Use snake_case from proto
        options.always_print_fields_with_no_presence = true;

        std::string json_str;
        auto status = google::protobuf::util::MessageToJsonString(connector_record, &json_str, options);

        if (!status.ok()) return std::unexpected(ParseError{ParseError::Kind::INVALID_VALUE, "invalid connector record"});

        return json_str;
    }

    template<>
    Result<ConnectorRecord> parseFromJson(json json_obj, use_json_t)
    {
        ConnectorRecord connector_record;
        if (json_obj.contains("connector") == false)
            return std::unexpected(ParseError{ParseError::Kind::INVALID_VALUE, "invalid connector"});

        auto connector = parseFromJson<Connector>(json_obj["connector"], use_json);

        if(!connector)
            return std::unexpected(ParseError{ParseError::Kind::INVALID_VALUE, "invalid connector"});

        *connector_record.mutable_connector() = std::move(*connector);

        if (json_obj.contains("owner") == false)
            return std::unexpected(ParseError{ParseError::Kind::INVALID_VALUE, "invalid owner"});
        
        connector_record.set_owner(json_obj["owner"].get<std::string>());

        return connector_record;
    }

    template<>
    Result<ConnectorRecord> parseFromJson(std::string json_str, use_protobuf_t)
    {
        google::protobuf::util::JsonParseOptions options;
        options.ignore_unknown_fields = true;

        ConnectorRecord connector_record;

        auto status = google::protobuf::util::JsonStringToMessage(json_str, &connector_record, options);

        if(!status.ok()) return std::unexpected(ParseError{ParseError::Kind::INVALID_VALUE, "invalid connector record"});

        return connector_record;
    }
}