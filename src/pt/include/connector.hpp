#pragma once
#include <algorithm>
#include <cstdint>
#include <map>
#include <utility>
#include <vector>

#include <absl/hash/hash.h>

#include "connector.pb.h"

#include "parser.hpp"
#include "chain.hpp"

namespace dcn
{
    /**
     * @brief Combines hash values for a Connector object.
     *
     * @tparam H The hash state type.
     * @param h The initial hash state.
     * @param p The Connector object whose attributes will be hashed.
     * @return A combined hash state
     */
    template <typename H>
    inline H AbslHashValue(H h, const Connector & p) {
        h = H::combine(std::move(h), p.name(), p.feature_name());

        std::vector<std::pair<std::uint32_t, std::string>> sorted_composites;
        sorted_composites.reserve(p.composites().size());
        for(const auto & [dim_id, composite_name] : p.composites())
        {
            sorted_composites.emplace_back(dim_id, composite_name);
        }
        std::ranges::sort(sorted_composites, [](const auto & lhs, const auto & rhs)
        {
            return lhs.first < rhs.first;
        });

        for(const auto & [dim_id, composite_name] : sorted_composites)
        {
            h = H::combine(std::move(h), dim_id, composite_name);
        }

        h = H::combine(std::move(h), p.condition_name());
        for(const auto & arg : p.condition_args()) {
            h = H::combine(std::move(h), arg);
        }
        return h;
    }

    std::string constructConnectorSolidityCode(const Connector & connector);
}

namespace dcn::pt
{
    struct ConnectorAddedEvent
    {
        chain::Address caller{};
        chain::Address owner{};
        std::string name;
        chain::Address connector_address{};
        std::string feature_name;
        std::map<std::uint32_t, std::string> composites;
        std::string condition_name;
        std::vector<std::int32_t> condition_args;
    };

    std::optional<ConnectorAddedEvent> decodeConnectorAddedEvent(
        const std::uint8_t* data,
        std::size_t data_size,
        const evmc::bytes32 topics[],
        std::size_t num_topics);

    std::optional<ConnectorAddedEvent> decodeConnectorAddedEvent(
        const std::string & data_hex,
        const std::vector<std::string> & topics_hex);

}

namespace dcn::parse
{
    /**
     * @brief Parses a Connector object to a JSON object.
     * @param connector The Connector object to parse.
     */
    template<>
    Result<json> parseToJson(Connector connector, use_json_t);

    /**
     * @brief Parses a JSON object to a Connector object.
     * @param json_obj The JSON object to parse.
     */
    template<>
    Result<Connector> parseFromJson(json json_obj, use_json_t);

    /**
     * @brief Converts a Connector object to a JSON string using protobuf
     * @param connector The Connector object to convert
     * @return A JSON string representation of the Connector object
     */
    template<>
    Result<std::string> parseToJson(Connector connector, use_protobuf_t);

    /**
     * @brief Parses a JSON string to a Connector object using Protobuf.
     * @param json_str The JSON string to parse.
     */
    template<>
    Result<Connector> parseFromJson(std::string json_str, use_protobuf_t);

    /**
     * @brief Converts a ConnectorRecord object to a JSON object using JSON.
     * @param connector_record The ConnectorRecord object to convert.
     */
    template<>
    Result<json> parseToJson(ConnectorRecord connector_record, use_json_t);

    /**
     * @brief Converts a ConnectorRecord to a JSON string using Protobuf.
     * @param connector_record The ConnectorRecord object to convert.
     */
    template<>
    Result<std::string> parseToJson(ConnectorRecord connector_record, use_protobuf_t);

    /**
     * @brief Parses a JSON object to a ConnectorRecord object using JSON.
     * @param json_obj The JSON object to parse.
     */
    template<>
    Result<ConnectorRecord> parseFromJson(json json_obj, use_json_t);
    
    /**
     * @brief Parses a JSON string to a ConnectorRecord object using Protobuf.
     * @param json_str The JSON string to parse.
     */
    template<>
    Result<ConnectorRecord> parseFromJson(std::string json_str, use_protobuf_t);
}