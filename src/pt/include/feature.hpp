#pragma once
#include <absl/hash/hash.h>

#include "feature.pb.h"

#include "parser.hpp"
#include "address.hpp"

namespace dcn
{

    /**
     * @brief Combines hash values for a TransformationDef object.
     *
     * @tparam H The hash state type.
     * @param h The initial hash state.
     * @param td The TransformationDef object whose attributes will be hashed.
     * @return A combined hash state incorporating the feature name and all transformation names of the TransformationDef.
     */
    template <typename H>
    inline H AbslHashValue(H h, const TransformationDef & td) 
    {
        h = H::combine(std::move(h), td.name());
        for (const int32_t & arg : td.args()) {
            h = H::combine(std::move(h), arg);
        }
        return h;
    }

    /**
     * @brief Combines hash values for a Dimension object.
     *
     * @tparam H The hash state type.
     * @param h The initial hash state.
     * @param d The Dimension object whose attributes will be hashed.
     * @return A combined hash state
     */
    template <typename H>
    inline H AbslHashValue(H h, const Dimension& d) 
    {
        for (const TransformationDef & t : d.transformations()) {
            h = H::combine(std::move(h), t);
        }
        return h;
    }

    /**
     * @brief Combines hash values for a Feature object.
     *
     * @tparam H The hash state type.
     * @param h The initial hash state.
     * @param f The Feature object whose attributes will be hashed.
     * @return A combined hash state incorporating the name and dimensions of the Feature.
     */
    template <typename H>
    inline H AbslHashValue(H h, const Feature& f) {
        h = H::combine(std::move(h), f.name());
        for (const Dimension & d : f.dimensions()) {
            h = H::combine(std::move(h), d);
        }
        return h;
    }

    std::string constructFeatureSolidityCode(const Feature & feature);
}


namespace dcn::pt
{
    struct FeatureAddedEvent
    {
        chain::Address caller{};
        std::string name;
        chain::Address feature_address{};
        chain::Address owner{};
        std::uint32_t dimensions_count{};
    };

    std::optional<FeatureAddedEvent> decodeFeatureAddedEvent(
        const std::uint8_t* data,
        std::size_t data_size,
        const evmc::bytes32 topics[],
        std::size_t num_topics);

    std::optional<FeatureAddedEvent> decodeFeatureAddedEvent(
        const std::string & data_hex,
        const std::vector<std::string> & topics_hex);

}

namespace dcn::parse
{
    /**
     * @brief Parses a TransformationDef object to a JSON object.
     * @param transform_def The TransformationDef object to parse.
     */
    template<>
    Result<json> parseToJson(TransformationDef transform_def, use_json_t);

    /**
     * @brief Parses a JSON object to a TransformationDef object.
     * @param json The JSON object to parse.
     */
    template<>
    Result<TransformationDef> parseFromJson(json json, use_json_t);

    /**
     * @brief Parses a Dimension object to a JSON object.
     * @param dimension The Dimension object to parse.
     */
    template<>
    Result<json> parseToJson(Dimension dimension, use_json_t);

    /**
     * @brief Parses a JSON object to a Dimension object.
     * @param json The JSON object to parse.
     */
    template<>
    Result<Dimension> parseFromJson(json json, use_json_t);

    /**
     * @brief Parses a Dimension object to a JSON string using Protobuf.
     * @param dimension The Dimension object to parse.
     */
    template<>
    Result<std::string> parseToJson(Dimension dimension, use_protobuf_t);

    /**
     * @brief Parses a JSON string to a Dimension object.
     * @param json_str The JSON string to parse.
     */
    template<>
    Result<Dimension> parseFromJson(std::string json_str, use_protobuf_t);
    
    /**
     * @brief Parses a Feature object to a JSON object.
     * @param feature The Feature object to parse.
     */
    template<>
    Result<json> parseToJson(Feature feature, use_json_t);

    /**
     * @brief Parses a JSON object to a Feature object.
     * @param json_obj The JSON object to parse.
     */
    template<>
    Result<Feature> parseFromJson(json json_obj, use_json_t);

    /**
     * @brief Converts a Feature object to a JSON string using protobuf
     * @param feature The Feature object to convert
     * @return A JSON string representation of the Feature object
     */
    template<>
    Result<std::string> parseToJson(Feature feature, use_protobuf_t);

    /**
     * @brief Parses a JSON string to a Feature object using Protobuf.
     * @param json_str The JSON string to parse.
     */
    template<>
    Result<Feature> parseFromJson(std::string json_str, use_protobuf_t);

    /**
     * @brief Converts a FeatureRecord object to a JSON object using JSON.
     * @param feature The FeatureRecord object to convert.
     */
    template<>
    Result<json> parseToJson(FeatureRecord feature, use_json_t);

    /**
     * @brief Converts a FeatureRecord to a JSON string using Protobuf.
     * @param feature_record The FeatureRecord object to convert.
     */
    template<>
    Result<std::string> parseToJson(FeatureRecord feature_record, use_protobuf_t);

    /**
     * @brief Parses a JSON object to a FeatureRecord object using JSON.
     * @param json_obj The JSON object to parse.
     */
    template<>
    Result<FeatureRecord> parseFromJson(json json_obj, use_json_t);
    
    /**
     * @brief Parses a JSON string to a FeatureRecord object using Protobuf.
     * @param json_str The JSON string to parse.
     */
    template<>
    Result<FeatureRecord> parseFromJson(std::string json_str, use_protobuf_t);
}