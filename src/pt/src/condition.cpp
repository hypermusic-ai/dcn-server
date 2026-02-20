#include "condition.hpp"

namespace dcn
{
    std::string constructConditionSolidityCode(const Condition & condition)
    {
        /* ------------- EXAMPLE -------------
        // SPDX-License-Identifier: GPL-3.0

        pragma solidity >=0.8.2 <0.9.0;

        import "../ConditionBase.sol";

        contract ConditionA is ConditionBase
        {
            constructor(address registryAddr) ConditionBase(registryAddr, "ConditionA", 1)
            {}

            function check(int32 [] calldata args) view external returns (bool)
            {
                require(args.length == this.getArgsCount(), "wrong number of arguments");
                return x + args[0];
            }
        }
        */

        std::regex used_args_pattern(R"(args\[(\d+)\])");
        std::uint32_t argc = 0;

        std::smatch match;
        auto it = condition.sol_src().cbegin();
        while (std::regex_search(it, condition.sol_src().cend(), match, used_args_pattern)) 
        {
            try
            {
                unsigned long value = std::stoul(match[1].str());

                if (value > std::numeric_limits<std::int32_t>::max()) {
                    spdlog::error("Value exceeds int32_t range");
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
                "pragma solidity >=0.8.2 <0.9.0;\n"
                "import \"condition/ConditionBase.sol\";\n"
                "contract " + condition.name() + " is ConditionBase{\n" // open contract
                "function initialize(address registryAddr) external initializer {\n"
                "__ConditionBase_init(registryAddr, \"" + condition.name() + "\"," +  std::to_string(argc) +");\n"
                "}\n"
                "function check(int32 [] calldata args) view external returns (bool){\n" // open function
                "require(args.length == this.getArgsCount(), \"wrong number of arguments\");\n"
                + condition.sol_src() + "\n}" // close function
                "\n}"; // close contract
    }
}
namespace dcn::parse
{
    template<>
    Result<json> parseToJson(Condition condition, use_json_t)
    {
        json json_obj;

        json_obj["name"] = condition.name();
        json_obj["sol_src"] = condition.sol_src();

        return json_obj;
    }

    template<>
    Result<Condition> parseFromJson(json json_obj, use_json_t)
    {
        Condition condition;

        if (json_obj.contains("name")) {
            condition.set_name(json_obj["name"].get<std::string>());
        }
        else return std::unexpected(ParseError{ParseError::Kind::INVALID_VALUE, "invalid name"});
        

        if (json_obj.contains("sol_src")) {
            condition.set_sol_src(json_obj["sol_src"].get<std::string>());
        }
        else return std::unexpected(ParseError{ParseError::Kind::INVALID_VALUE, "invalid sol_src"});
        

        return condition;
    }

    template<>
    Result<std::string> parseToJson(Condition condition, use_protobuf_t)
    {
        google::protobuf::util::JsonPrintOptions options;
        options.add_whitespace = true;
        options.preserve_proto_field_names = true; // Use snake_case from proto
        options.always_print_fields_with_no_presence = true;

        std::string json_str;
        auto status = google::protobuf::util::MessageToJsonString(condition, &json_str, options);

        if (!status.ok()) return std::unexpected(ParseError{ParseError::Kind::INVALID_VALUE, "invalid condition"});

        return json_str;
    }

    template<>
    Result<Condition> parseFromJson(std::string json_str, use_protobuf_t)
    {
        google::protobuf::util::JsonParseOptions options;

        Condition condition;

        auto status = google::protobuf::util::JsonStringToMessage(json_str, &condition, options);

        if(!status.ok()) return std::unexpected(ParseError{ParseError::Kind::INVALID_VALUE, "invalid condition"});

        return condition;
    }

    template<>
    Result<json> parseToJson(ConditionRecord condition_record, use_json_t)
    {
        json json_obj = json::object();

        auto condition_result = parseToJson(condition_record.condition(), use_json);
        if(!condition_result) return std::unexpected(ParseError{ParseError::Kind::INVALID_VALUE, "invalid condition"});

        json_obj["condition"] = std::move(*condition_result);
        json_obj["owner"] = condition_record.owner();
        return json_obj;
    }

    template<>
    Result<std::string> parseToJson(ConditionRecord condition_record, use_protobuf_t)
    {
        google::protobuf::util::JsonPrintOptions options;
        options.add_whitespace = true;
        options.preserve_proto_field_names = true; // Use snake_case from proto
        options.always_print_fields_with_no_presence = true;

        std::string json_str;
        auto status = google::protobuf::util::MessageToJsonString(condition_record, &json_str, options);

        if (!status.ok()) return std::unexpected(ParseError{ParseError::Kind::INVALID_VALUE, "invalid condition record"});

        return json_str;
    }

    template<>
    Result<ConditionRecord> parseFromJson(json json_obj, use_json_t)
    {
        ConditionRecord condition_record;
        if (json_obj.contains("condition") == false)
            return std::unexpected(ParseError{ParseError::Kind::INVALID_VALUE, "invalid condition"});

        auto condition = parseFromJson<Condition>(json_obj["condition"], use_json);

        if(!condition)
            return std::unexpected(ParseError{ParseError::Kind::INVALID_VALUE, "invalid condition"});

        *condition_record.mutable_condition() = std::move(*condition);

        if (json_obj.contains("owner") == false)
            return std::unexpected(ParseError{ParseError::Kind::INVALID_VALUE, "invalid owner"});
        
        condition_record.set_owner(json_obj["owner"].get<std::string>());

        return condition_record;
    }

    template<>
    Result<ConditionRecord> parseFromJson(std::string json_str, use_protobuf_t)
    {
        google::protobuf::util::JsonParseOptions options;
        options.ignore_unknown_fields = true;

        ConditionRecord condition_record;

        auto status = google::protobuf::util::JsonStringToMessage(json_str, &condition_record, options);

        if(!status.ok()) return std::unexpected(ParseError{ParseError::Kind::INVALID_VALUE, "invalid condition record"});

        return condition_record;
    }

}
