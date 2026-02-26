#include <format>

#include "crypto.hpp"

#include "transformation.hpp"

namespace dcn
{
    std::string constructTransformationSolidityCode(const Transformation & transformation)
    {
        /* ------------- EXAMPLE -------------
        // SPDX-License-Identifier: GPL-3.0

        pragma solidity >=0.8.2 <0.9.0;

        import "../TransformationBase.sol";

        contract Add is TransformationBase
        {
            constructor(address registryAddr) TransformationBase(registryAddr, "Add", 1)
            {}

            function run(uint32 x, uint32 [] calldata args) view external returns (uint32)
            {
                require(args.length == this.getArgsCount(), "wrong number of arguments");
                return x + args[0];
            }
        }
        */

        std::regex used_args_pattern(R"(args\[(\d+)\])");
        std::uint32_t argc = 0;

        std::smatch match;
        auto it = transformation.sol_src().cbegin();
        while (std::regex_search(it, transformation.sol_src().cend(), match, used_args_pattern)) 
        {
            try
            {
                unsigned long value = std::stoul(match[1].str());

                if (value > std::numeric_limits<std::uint32_t>::max()) {
                    spdlog::error("Value exceeds uint32_t range");
                    return "";
                }
                argc = std::max(argc, static_cast<std::uint32_t>(value) + 1U);
            }
            catch(const std::exception& e)
            {
                spdlog::error("Invalid argument index: {}", match[1].str());
                return "";
            }

            it = match.suffix().first;
        }

        return  "//SPDX-License-Identifier: MIT\n"
                "pragma solidity ^0.8.0;\n"
                "import \"transformation/TransformationBase.sol\";\n"
                "contract " + transformation.name() + " is TransformationBase{\n" // open contract
                "function initialize(address registryAddr) external initializer {\n"
                "__TransformationBase_init(registryAddr, \"" + transformation.name() + "\"," +  std::to_string(argc) +");\n"
                "}\n"
                "function run(uint32 x, uint32 [] calldata args) view external returns (uint32){\n" // open function
                "require(args.length == this.getArgsCount(), \"wrong number of arguments\");\n"
                + transformation.sol_src() + "\n}" // close function
                "\n}"; // close contract
    }
}

namespace dcn::pt
{
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

        const evmc::bytes32 expected_topic = chain::constructEventTopic(
            "TransformationAdded(address,string,address,address,uint32)");

        if(topics[0] != expected_topic)
        {
            return std::nullopt;
        }

        const auto caller = chain::readAddressWord(data, data_size, 0);
        const auto name_offset = chain::readWordAsSizeT(data, data_size, 32);
        const auto transformation_address = chain::readAddressWord(data, data_size, 64);
        const auto owner = chain::readAddressWord(data, data_size, 96);
        const auto args_count = chain::readUint32Word(data, data_size, 128);
        if(!caller || !name_offset || !transformation_address || !owner || !args_count)
        {
            return std::nullopt;
        }

        const auto name = chain::decodeAbiString(data, data_size, *name_offset);
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

        const auto topic_words = chain::decodeTopicWords(topics_hex);
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
}

namespace dcn::parse
{
    template<>
    Result<json> parseToJson(Transformation transformation, use_json_t)
    {
        json json_obj;

        json_obj["name"] = transformation.name();
        json_obj["sol_src"] = transformation.sol_src();

        return json_obj;
    }

    template<>
    Result<Transformation> parseFromJson(json json_obj, use_json_t)
    {
        Transformation transformation;

        if (json_obj.contains("name")) {
            transformation.set_name(json_obj["name"].get<std::string>());
        }
        else return std::unexpected(ParseError{ParseError::Kind::INVALID_VALUE, "invalid name"});
        

        if (json_obj.contains("sol_src")) {
            transformation.set_sol_src(json_obj["sol_src"].get<std::string>());
        }
        else return std::unexpected(ParseError{ParseError::Kind::INVALID_VALUE, "invalid sol_src"});
        

        return transformation;
    }

    template<>
    Result<std::string> parseToJson(Transformation transformation, use_protobuf_t)
    {
        google::protobuf::util::JsonPrintOptions options;
        options.add_whitespace = true;
        options.preserve_proto_field_names = true; // Use snake_case from proto
        options.always_print_fields_with_no_presence = true;

        std::string json_str;
        auto status = google::protobuf::util::MessageToJsonString(transformation, &json_str, options);

        if (!status.ok()) return std::unexpected(ParseError{ParseError::Kind::INVALID_VALUE, "invalid transformation"});

        return json_str;
    }

    template<>
    Result<Transformation> parseFromJson(std::string json_str, use_protobuf_t)
    {
        google::protobuf::util::JsonParseOptions options;

        Transformation transformation;

        auto status = google::protobuf::util::JsonStringToMessage(json_str, &transformation, options);

        if(!status.ok()) return std::unexpected(ParseError{ParseError::Kind::INVALID_VALUE, "invalid transformation"});

        return transformation;
    }

    template<>
    Result<json> parseToJson(TransformationRecord transformation_record, use_json_t)
    {
        json json_obj = json::object();

        auto transformation_result = parseToJson(transformation_record.transformation(), use_json);
        if(!transformation_result) return std::unexpected(ParseError{ParseError::Kind::INVALID_VALUE, "invalid transformation"});

        json_obj["transformation"] = std::move(*transformation_result);
        json_obj["owner"] = transformation_record.owner();
        return json_obj;
    }

    template<>
    Result<std::string> parseToJson(TransformationRecord transformation_record, use_protobuf_t)
    {
        google::protobuf::util::JsonPrintOptions options;
        options.add_whitespace = true;
        options.preserve_proto_field_names = true; // Use snake_case from proto
        options.always_print_fields_with_no_presence = true;

        std::string json_str;
        auto status = google::protobuf::util::MessageToJsonString(transformation_record, &json_str, options);

        if (!status.ok()) return std::unexpected(ParseError{ParseError::Kind::INVALID_VALUE, "invalid transformation record"});

        return json_str;
    }

    template<>
    Result<TransformationRecord> parseFromJson(json json_obj, use_json_t)
    {
        TransformationRecord transformation_record;
        if (json_obj.contains("transformation") == false)
            return std::unexpected(ParseError{ParseError::Kind::INVALID_VALUE, "invalid transformation"});

        auto transformation = parseFromJson<Transformation>(json_obj["transformation"], use_json);

        if(!transformation)
            return std::unexpected(ParseError{ParseError::Kind::INVALID_VALUE, "invalid transformation"});

        *transformation_record.mutable_transformation() = std::move(*transformation);

        if (json_obj.contains("owner") == false)
            return std::unexpected(ParseError{ParseError::Kind::INVALID_VALUE, "invalid owner"});
        
        transformation_record.set_owner(json_obj["owner"].get<std::string>());

        return transformation_record;
    }

    template<>
    Result<TransformationRecord> parseFromJson(std::string json_str, use_protobuf_t)
    {
        google::protobuf::util::JsonParseOptions options;
        options.ignore_unknown_fields = true;

        TransformationRecord transformation_record;

        auto status = google::protobuf::util::JsonStringToMessage(json_str, &transformation_record, options);

        if(!status.ok()) return std::unexpected(ParseError{ParseError::Kind::INVALID_VALUE, "invalid transformation record"});

        return transformation_record;
    }

}
