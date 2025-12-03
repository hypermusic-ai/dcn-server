#include "feature.hpp"

namespace dcn
{
    std::string constructFeatureSolidityCode(const Feature & feature)
    {
        /*
        // SPDX-License-Identifier: GPL-3.0

        pragma solidity >=0.7.0 <0.9.0;

        import "../FeatureBase.sol";

        import "../../condition/conditions-examples/AlwaysTrue.sol";

        contract FeatureA is FeatureBase
        {
            string[]      private _composites   = ["Pitch", "Time"];

            constructor(address registryAddr) FeatureBase(registryAddr, new AlwaysTrue(), "FeatureA", _composites)
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

        std::string composites_code;
        std::string transform_def_code;
        std::string args_code;

        for(unsigned int i = 0; i < feature.dimensions_size(); i++)
        {
            if(i == 0)composites_code += "=[";
            composites_code += "\"" + feature.dimensions().at(i).feature_name() + "\"";
            if(i + 1 != feature.dimensions_size())composites_code += ", ";
            if(i == feature.dimensions_size() - 1)composites_code += "]";

            for(unsigned ii = 0; ii < feature.dimensions().at(i).transformations_size(); ii++)
            {
                const auto & transform = feature.dimensions().at(i).transformations().at(ii);
                args_code = "";
                for(unsigned int iii = 0; iii < transform.args_size(); ++iii)
                {
                    args_code += "uint32(" + std::to_string(transform.args().at(iii)) + ")";
                    if(iii + 1 != transform.args_size())args_code += ", ";
                }

                if(transform.args_size() > 0)
                {
                    transform_def_code +=   
                        "getCallDef().push(" 
                        + std::to_string(i) 
                        + ", \"" + transform.name() 
                        + "\", [" + args_code + "]);\n";
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

        return  "//SPDX-License-Identifier: MIT\n"
                "pragma solidity ^0.8.0;\n"
                "import \"feature/FeatureBase.sol\";\n"
                // TODO
                "import \"condition/conditions-examples/AlwaysTrue.sol\";\n"
                // ----
                "contract " + feature.name() + " is FeatureBase{\n" // open contract
                "string[] private _composites" + composites_code + ";\n"
                "constructor(address registryAddr) FeatureBase(registryAddr, new AlwaysTrue(), \"" + feature.name() + "\", _composites){\n" // open ctor // 
                + transform_def_code + // transform_def_code
                "super.init();\n}" // close ctor;
                "\n}"; // close contract
    }
}

namespace dcn::parse
{

    std::optional<json> parseToJson(TransformationDef transform_def, use_json_t)
    {
        json json_obj = json::object();
        json_obj["name"] = transform_def.name();
        json_obj["args"] = transform_def.args();

        return json_obj;
    }

    template<>
    std::optional<TransformationDef> parseFromJson(json json, use_json_t)
    {
        TransformationDef transform_def;
        if (json.contains("name")) {
            transform_def.set_name(json["name"].get<std::string>());
        }
        else return std::nullopt;

        if (json.contains("args")) {
            for(const int32_t & arg : json["args"].get<std::vector<int32_t>>())
            {
                transform_def.add_args(arg);
            }
        }
        return transform_def;
    }

    std::optional<json> parseToJson(Dimension dimension, use_json_t)
    {
        json json_obj = json::object();
        json_obj["feature_name"] = dimension.feature_name();
        for(const TransformationDef & transform_def : dimension.transformations())
        {
            json_obj["transformations"].push_back(parseToJson(transform_def, use_json));
        }

        return json_obj;
    }

    template<>
    std::optional<Dimension> parseFromJson(json json_obj, use_json_t)
    {
        Dimension dimension;

        if (json_obj.contains("feature_name")) {
            dimension.set_feature_name(json_obj["feature_name"].get<std::string>());
        }
        else return std::nullopt;

        if(json_obj.contains("transformations") == false) {
            return std::nullopt;
        }

        for(const std::string & transform_name : json_obj["transformations"])
        {
            std::optional<TransformationDef> transformation_def = parseFromJson<TransformationDef>(transform_name, use_json);
            if(!transformation_def.has_value()) return std::nullopt;
                dimension.add_transformations();
                *dimension.mutable_transformations(dimension.transformations_size() - 1) = *transformation_def;
        }
        return dimension;
    }

    std::optional<std::string> parseToJson(Dimension dimension, use_protobuf_t)
    {
        google::protobuf::util::JsonPrintOptions options;
        options.add_whitespace = true; // Pretty print
        options.preserve_proto_field_names = true; // Use snake_case from proto
        options.always_print_fields_with_no_presence = true;

        std::string json_output;

        auto status = google::protobuf::util::MessageToJsonString(dimension, &json_output, options);

        if(!status.ok()) return std::nullopt;

        return json_output;
    }

    template<>
    std::optional<Dimension> parseFromJson(std::string json_str, use_protobuf_t)
    {
        google::protobuf::util::JsonParseOptions options;

        Dimension dimension;

        auto status = google::protobuf::util::JsonStringToMessage(json_str, &dimension, options);

        if(!status.ok()) return std::nullopt;

        return dimension;
    }

    std::optional<json> parseToJson(Feature feature ,use_json_t)
    {
        json json_obj = json::object();
        json_obj["dimensions"] = json::array();
        for(const Dimension & dimension : feature.dimensions())
        {
            json_obj["dimensions"].push_back(parseToJson(dimension, use_json));
        }
        json_obj["name"] = feature.name();

        return json_obj;
    }

    template<>
    std::optional<Feature> parseFromJson(json json_obj, use_json_t)
    {
        Feature feature;

        if (json_obj.contains("name")) {
            feature.set_name(json_obj["name"].get<std::string>());
        }
        else return std::nullopt;

        if (json_obj.contains("dimensions") == false) {
            return std::nullopt;
        }

        for(const auto & dim : json_obj["dimensions"])
        {
            std::optional<Dimension> dimension = parseFromJson<Dimension>(dim, use_json);
            if(dimension) {
                feature.add_dimensions();
                *feature.mutable_dimensions(feature.dimensions_size() - 1) = *dimension;
            }else {
                return std::nullopt; // Error in parsing dimension
            }
        }
        return feature;
    }

    std::optional<std::string> parseToJson(Feature feature, use_protobuf_t)
    {
        google::protobuf::util::JsonPrintOptions options;
        options.add_whitespace = true; // Pretty print
        options.preserve_proto_field_names = true; // Use snake_case from proto
        options.always_print_fields_with_no_presence = true;

        std::string json_output;

        auto status = google::protobuf::util::MessageToJsonString(feature, &json_output, options);

        if(!status.ok()) return std::nullopt;

        return json_output;
    }

    template<>
    std::optional<Feature> parseFromJson(std::string json_str, use_protobuf_t)
    {
        google::protobuf::util::JsonParseOptions options;

        Feature feature;

        auto status = google::protobuf::util::JsonStringToMessage(json_str, &feature, options);

        if(!status.ok()) return std::nullopt;

        return feature;
    }


    std::optional<json> parseToJson(FeatureRecord feature, use_json_t)
    {
        json json_obj = json::object();
        json_obj["feature"] = parseToJson(feature.feature(), use_json);
        json_obj["owner"] = feature.owner();
        return json_obj;
    }

    std::optional<std::string> parseToJson(FeatureRecord feature_record, use_protobuf_t)
    {
        google::protobuf::util::JsonPrintOptions options;
        options.add_whitespace = true; // Pretty print
        options.preserve_proto_field_names = true; // Use snakeCase from proto
        options.always_print_fields_with_no_presence = true;

        std::string json_output;

        auto status = google::protobuf::util::MessageToJsonString(feature_record, &json_output, options);

        if(!status.ok()) return std::nullopt;

        return json_output;
    }

    template<>
    std::optional<FeatureRecord> parseFromJson(json json_obj, use_json_t)
    {
        FeatureRecord feature_record;
        if (json_obj.contains("feature") == false)
            return std::nullopt;

        std::optional<Feature> feature = parseFromJson<Feature>(json_obj["feature"], use_json);

        if(!feature)
            return std::nullopt;

        *feature_record.mutable_feature() = std::move(*feature);

        if (json_obj.contains("owner") == false)
            return std::nullopt;
        
        feature_record.set_owner(json_obj["owner"].get<std::string>());

        return feature_record;
    }
    
    template<>
    std::optional<FeatureRecord> parseFromJson(std::string json_str, use_protobuf_t)
    {
        google::protobuf::util::JsonParseOptions options;
        options.ignore_unknown_fields = true;

        FeatureRecord feature_record;

        auto status = google::protobuf::util::JsonStringToMessage(json_str, &feature_record, options);

        if(!status.ok()) return std::nullopt;

        return feature_record;
    }
}