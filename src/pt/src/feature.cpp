#include "feature.hpp"

#include <format>

namespace dcn
{
    std::string constructFeatureSolidityCode(const Feature & feature)
    {
        /*
        // SPDX-License-Identifier: GPL-3.0

        pragma solidity >=0.8.2 <0.9.0;

        import "../FeatureBase.sol";

        contract FeatureA is FeatureBase
        {
            constructor(address registryAddr) FeatureBase(registryAddr, "FeatureA", 2)
            {
                getCallDef().push(0, "Add", [uint32(1)]);
                getCallDef().push(0, "Mul", [uint32(2)]);
                getCallDef().push(0, "Nop");
                getCallDef().push(0, "Add", [uint32(3)]);

                getCallDef().push(1, "Add", [uint32(1)]);
                getCallDef().push(1, "Add", [uint32(3)]);
                getCallDef().push(1, "Add", [uint32(2)]);

                init();
            }
        }
        */

        std::string transform_def_code;

        for(unsigned int i = 0; i < feature.dimensions_size(); i++)
        {
            for(unsigned ii = 0; ii < feature.dimensions().at(i).transformations_size(); ii++)
            {
                const auto & transform = feature.dimensions().at(i).transformations().at(ii);
                
                std::string transform_args_code;
                for(unsigned int iii = 0; iii < transform.args_size(); ++iii)
                {
                    transform_args_code += "uint32(" + std::to_string(transform.args().at(iii)) + ")";
                    if(iii + 1 != transform.args_size())transform_args_code += ", ";
                }

                if(transform.args_size() > 0)
                {
                    transform_def_code +=   
                        "getCallDef().push(" 
                        + std::to_string(i) 
                        + ", \"" + transform.name() 
                        + "\", [" + transform_args_code + "]);\n";
                }else
                {
                    transform_def_code +=   
                        "getCallDef().push(" 
                        + std::to_string(i) 
                        + ", \"" + transform.name() 
                        + "\");\n";
                }
            }
        }
        
        return std::format(
            "//SPDX-License-Identifier: MIT\n"
            "pragma solidity >=0.8.2 <0.9.0;\n"
            "import \"feature/FeatureBase.sol\";\n"
            "contract {0} is FeatureBase{{\n"

            "constructor(address registryAddr) FeatureBase(registryAddr, \"{0}\", {1}) {{\n"
            "{2}"
            "super.init();\n}}"
            "\n}}",
            
            // def
            feature.name(),             // 0
            // ctor
            feature.dimensions_size(),  // 1
            // body
            transform_def_code        // 2
        );
    }
}

namespace dcn::parse
{

    template<>
    Result<json> parseToJson(TransformationDef transform_def, use_json_t)
    {
        json json_obj = json::object();
        json_obj["name"] = transform_def.name();
        json_obj["args"] = transform_def.args();

        return json_obj;
    }

    template<>
    Result<TransformationDef> parseFromJson(json json, use_json_t)
    {
        TransformationDef transform_def;
        if (json.contains("name")) {
            transform_def.set_name(json["name"].get<std::string>());
        }
        else return std::unexpected(ParseError{ParseError::Kind::INVALID_VALUE, "name not found"});

        if (json.contains("args")) {
            for(const int32_t & arg : json["args"].get<std::vector<int32_t>>())
            {
                transform_def.add_args(arg);
            }
        }
        return transform_def;
    }

    template<>
    Result<json> parseToJson(Dimension dimension, use_json_t)
    {
        json json_obj = json::object();
        for(const TransformationDef & transform_def : dimension.transformations())
        {
            auto transform_result = parseToJson(transform_def, use_json);\
            if(!transform_result) return std::unexpected(ParseError{ParseError::Kind::INVALID_VALUE, "invalid transformation"});

            json_obj["transformations"].emplace_back(std::move(*transform_result));
        }

        return json_obj;
    }

    template<>
    Result<Dimension> parseFromJson(json json_obj, use_json_t)
    {
        Dimension dimension;

        if(json_obj.contains("transformations") == false) {
            return std::unexpected(ParseError{ParseError::Kind::INVALID_VALUE, "transformations not found"});
        }

        for(const std::string & transform_name : json_obj["transformations"])
        {
            auto transformation_def = parseFromJson<TransformationDef>(transform_name, use_json);
            if(!transformation_def) return std::unexpected(ParseError{ParseError::Kind::INVALID_VALUE, "invalid transformation"});

            dimension.add_transformations();
            *dimension.mutable_transformations(dimension.transformations_size() - 1) = *transformation_def;
        }
        return dimension;
    }

    template<>
    Result<std::string> parseToJson(Dimension dimension, use_protobuf_t)
    {
        google::protobuf::util::JsonPrintOptions options;
        options.add_whitespace = true; // Pretty print
        options.preserve_proto_field_names = true; // Use snake_case from proto
        options.always_print_fields_with_no_presence = true;

        std::string json_output;

        auto status = google::protobuf::util::MessageToJsonString(dimension, &json_output, options);

        if(!status.ok()) return std::unexpected(ParseError{ParseError::Kind::INVALID_VALUE, "invalid dimension"});

        return json_output;
    }

    template<>
    Result<Dimension> parseFromJson(std::string json_str, use_protobuf_t)
    {
        google::protobuf::util::JsonParseOptions options;

        Dimension dimension;

        auto status = google::protobuf::util::JsonStringToMessage(json_str, &dimension, options);

        if(!status.ok()) return std::unexpected(ParseError{ParseError::Kind::INVALID_VALUE, "invalid dimension"});

        return dimension;
    }

    template<>
    Result<json> parseToJson(Feature feature ,use_json_t)
    {
        json json_obj = json::object();
        json_obj["dimensions"] = json::array();
        for(const Dimension & dimension : feature.dimensions())
        {
            auto dimension_result = parseToJson(dimension, use_json);
            if(!dimension_result) return std::unexpected(ParseError{ParseError::Kind::INVALID_VALUE, "invalid dimension"});

            json_obj["dimensions"].emplace_back(std::move(*dimension_result));
        }
        json_obj["name"] = feature.name();

        return json_obj;
    }

    template<>
    Result<Feature> parseFromJson(json json_obj, use_json_t)
    {
        Feature feature;

        if (json_obj.contains("name")) {
            feature.set_name(json_obj["name"].get<std::string>());
        }
        else return std::unexpected(ParseError{ParseError::Kind::INVALID_VALUE, "name not found"});

        if (json_obj.contains("dimensions") == false) {
            return std::unexpected(ParseError{ParseError::Kind::INVALID_VALUE, "dimensions not found"});
        }

        for(const auto & dim : json_obj["dimensions"])
        {
            auto dimension = parseFromJson<Dimension>(dim, use_json);
            if(dimension) {
                feature.add_dimensions();
                *feature.mutable_dimensions(feature.dimensions_size() - 1) = *dimension;
            }else {
                // Error in parsing dimension
                return std::unexpected(ParseError{ParseError::Kind::INVALID_VALUE, "invalid dimension"}); 
            }
        }
        return feature;
    }

    template<>
    Result<std::string> parseToJson(Feature feature, use_protobuf_t)
    {
        google::protobuf::util::JsonPrintOptions options;
        options.add_whitespace = true; // Pretty print
        options.preserve_proto_field_names = true; // Use snake_case from proto
        options.always_print_fields_with_no_presence = true;

        std::string json_output;

        auto status = google::protobuf::util::MessageToJsonString(feature, &json_output, options);

        if(!status.ok()) return std::unexpected(ParseError{ParseError::Kind::INVALID_VALUE, "invalid feature"});

        return json_output;
    }

    template<>
    Result<Feature> parseFromJson(std::string json_str, use_protobuf_t)
    {
        google::protobuf::util::JsonParseOptions options;

        Feature feature;

        auto status = google::protobuf::util::JsonStringToMessage(json_str, &feature, options);

        if(!status.ok()) return std::unexpected(ParseError{ParseError::Kind::INVALID_VALUE, "invalid feature"});

        return feature;
    }

    template<>
    Result<json> parseToJson(FeatureRecord feature, use_json_t)
    {
        json json_obj = json::object();
        auto feature_result = parseToJson(feature.feature(), use_json);
        if(!feature_result) return std::unexpected(ParseError{ParseError::Kind::INVALID_VALUE, "invalid feature"});

        json_obj["feature"] = std::move(*feature_result);
        json_obj["owner"] = feature.owner();
        return json_obj;
    }

    template<>
    Result<std::string> parseToJson(FeatureRecord feature_record, use_protobuf_t)
    {
        google::protobuf::util::JsonPrintOptions options;
        options.add_whitespace = true; // Pretty print
        options.preserve_proto_field_names = true; // Use snakeCase from proto
        options.always_print_fields_with_no_presence = true;

        std::string json_output;

        auto status = google::protobuf::util::MessageToJsonString(feature_record, &json_output, options);

        if(!status.ok()) return std::unexpected(ParseError{ParseError::Kind::INVALID_VALUE, "invalid feature record"});

        return json_output;
    }

    template<>
    Result<FeatureRecord> parseFromJson(json json_obj, use_json_t)
    {
        FeatureRecord feature_record;
        if (json_obj.contains("feature") == false)
            return std::unexpected(ParseError{ParseError::Kind::INVALID_VALUE, "feature not found"});

        auto feature = parseFromJson<Feature>(json_obj["feature"], use_json);

        if(!feature)
            return std::unexpected(ParseError{ParseError::Kind::INVALID_VALUE, "invalid feature"});

        *feature_record.mutable_feature() = std::move(*feature);

        if (json_obj.contains("owner") == false)
            return std::unexpected(ParseError{ParseError::Kind::INVALID_VALUE, "owner not found"});
        
        feature_record.set_owner(json_obj["owner"].get<std::string>());

        return feature_record;
    }
    
    template<>
    Result<FeatureRecord> parseFromJson(std::string json_str, use_protobuf_t)
    {
        google::protobuf::util::JsonParseOptions options;
        options.ignore_unknown_fields = true;

        FeatureRecord feature_record;

        auto status = google::protobuf::util::JsonStringToMessage(json_str, &feature_record, options);

        if(!status.ok()) return std::unexpected(ParseError{ParseError::Kind::INVALID_VALUE, "invalid feature record"});

        return feature_record;
    }
}