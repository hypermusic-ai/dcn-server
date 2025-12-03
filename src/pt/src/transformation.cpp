#include "transformation.hpp"

namespace dcn
{
    std::string constructTransformationSolidityCode(const Transformation & transformation)
    {
        /* ------------- EXAMPLE -------------
        // SPDX-License-Identifier: GPL-3.0

        pragma solidity >=0.7.0 <0.9.0;

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
                "constructor(address registryAddr) TransformationBase(registryAddr, \"" + transformation.name() + "\"," +  std::to_string(argc) +"){}\n"
                "function run(uint32 x, uint32 [] calldata args) view external returns (uint32){\n" // open function
                "require(args.length == this.getArgsCount(), \"wrong number of arguments\");\n"
                + transformation.sol_src() + "\n}" // close function
                "\n}"; // close contract
    }
}

namespace dcn::parse
{
    std::optional<json> parseToJson(Transformation transformation, use_json_t)
    {
        json json_obj;

        json_obj["name"] = transformation.name();
        json_obj["sol_src"] = transformation.sol_src();

        return json_obj;
    }

    template<>
    std::optional<Transformation> parseFromJson(json json_obj, use_json_t)
    {
        Transformation transformation;

        if (json_obj.contains("name")) {
            transformation.set_name(json_obj["name"].get<std::string>());
        }
        else return std::nullopt;
        

        if (json_obj.contains("sol_src")) {
            transformation.set_sol_src(json_obj["sol_src"].get<std::string>());
        }
        else return std::nullopt;
        

        return transformation;
    }

    std::optional<std::string> parseToJson(Transformation transformation, use_protobuf_t)
    {
        google::protobuf::util::JsonPrintOptions options;
        options.add_whitespace = true;
        options.preserve_proto_field_names = true; // Use snake_case from proto
        options.always_print_fields_with_no_presence = true;

        std::string json_str;
        auto status = google::protobuf::util::MessageToJsonString(transformation, &json_str, options);

        if (!status.ok()) return std::nullopt;

        return json_str;
    }

    template<>
    std::optional<Transformation> parseFromJson(std::string json_str, use_protobuf_t)
    {
        google::protobuf::util::JsonParseOptions options;

        Transformation transformation;

        auto status = google::protobuf::util::JsonStringToMessage(json_str, &transformation, options);

        if(!status.ok()) return std::nullopt;

        return transformation;
    }

    std::optional<json> parseToJson(TransformationRecord transformation_record, use_json_t)
    {
        json json_obj = json::object();
        json_obj["transformation"] = parseToJson(transformation_record.transformation(), use_json);
        json_obj["owner"] = transformation_record.owner();
        return json_obj;
    }

    std::optional<std::string> parseToJson(TransformationRecord transformation_record, use_protobuf_t)
    {
        google::protobuf::util::JsonPrintOptions options;
        options.add_whitespace = true;
        options.preserve_proto_field_names = true; // Use snake_case from proto
        options.always_print_fields_with_no_presence = true;

        std::string json_str;
        auto status = google::protobuf::util::MessageToJsonString(transformation_record, &json_str, options);

        if (!status.ok()) return std::nullopt;

        return json_str;
    }

    template<>
    std::optional<TransformationRecord> parseFromJson(json json_obj, use_json_t)
    {
        TransformationRecord transformation_record;
        if (json_obj.contains("transformation") == false)
            return std::nullopt;

        std::optional<Transformation> transformation = parseFromJson<Transformation>(json_obj["transformation"], use_json);

        if(!transformation)
            return std::nullopt;

        *transformation_record.mutable_transformation() = std::move(*transformation);

        if (json_obj.contains("owner") == false)
            return std::nullopt;
        
        transformation_record.set_owner(json_obj["owner"].get<std::string>());

        return transformation_record;
    }

    template<>
    std::optional<TransformationRecord> parseFromJson(std::string json_str, use_protobuf_t)
    {
        google::protobuf::util::JsonParseOptions options;
        options.ignore_unknown_fields = true;

        TransformationRecord transformation_record;

        auto status = google::protobuf::util::JsonStringToMessage(json_str, &transformation_record, options);

        if(!status.ok()) return std::nullopt;

        return transformation_record;
    }

}