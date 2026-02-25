#pragma once
#include <absl/hash/hash.h>

#include "particle.pb.h"

#include "parser.hpp"

#include "address.hpp"

namespace dcn
{
    /**
     * @brief Combines hash values for a Particle object.
     *
     * @tparam H The hash state type.
     * @param h The initial hash state.
     * @param p The Particle object whose attributes will be hashed.
     * @return A combined hash state
     */
    template <typename H>
    inline H AbslHashValue(H h, const Particle & p) {
        h = H::combine(std::move(h), p.name(), p.feature_name());
        for (const auto & c : p.composite_names()) {
            h = H::combine(std::move(h), c);
        }
        h = H::combine(std::move(h), p.condition_name());
        for(const auto & arg : p.condition_args()) {
            h = H::combine(std::move(h), arg);
        }
        return h;
    }

    std::string constructParticleSolidityCode(const Particle & particle);
}

namespace dcn::pt
{
    struct ParticleAddedEvent
    {
        chain::Address caller{};
        chain::Address owner{};
        std::string name;
        chain::Address particle_address{};
        std::string feature_name;
        std::vector<std::string> composite_names;
        std::string condition_name;
        std::vector<std::int32_t> condition_args;
    };

    std::optional<ParticleAddedEvent> decodeParticleAddedEvent(
        const std::uint8_t* data,
        std::size_t data_size,
        const evmc::bytes32 topics[],
        std::size_t num_topics);

    std::optional<ParticleAddedEvent> decodeParticleAddedEvent(
        const std::string & data_hex,
        const std::vector<std::string> & topics_hex);

}

namespace dcn::parse
{
    /**
     * @brief Parses a Particle object to a JSON object.
     * @param particle The Particle object to parse.
     */
    template<>
    Result<json> parseToJson(Particle particle, use_json_t);

    /**
     * @brief Parses a JSON object to a Particle object.
     * @param json_obj The JSON object to parse.
     */
    template<>
    Result<Particle> parseFromJson(json json_obj, use_json_t);

    /**
     * @brief Converts a Particle object to a JSON string using protobuf
     * @param particle The Particle object to convert
     * @return A JSON string representation of the Particle object
     */
    template<>
    Result<std::string> parseToJson(Particle particle, use_protobuf_t);

    /**
     * @brief Parses a JSON string to a Particle object using Protobuf.
     * @param json_str The JSON string to parse.
     */
    template<>
    Result<Particle> parseFromJson(std::string json_str, use_protobuf_t);

    /**
     * @brief Converts a ParticleRecord object to a JSON object using JSON.
     * @param particle_record The ParticleRecord object to convert.
     */
    template<>
    Result<json> parseToJson(ParticleRecord particle_record, use_json_t);

    /**
     * @brief Converts a ParticleRecord to a JSON string using Protobuf.
     * @param particle_record The ParticleRecord object to convert.
     */
    template<>
    Result<std::string> parseToJson(ParticleRecord particle_record, use_protobuf_t);

    /**
     * @brief Parses a JSON object to a ParticleRecord object using JSON.
     * @param json_obj The JSON object to parse.
     */
    template<>
    Result<ParticleRecord> parseFromJson(json json_obj, use_json_t);
    
    /**
     * @brief Parses a JSON string to a ParticleRecord object using Protobuf.
     * @param json_str The JSON string to parse.
     */
    template<>
    Result<ParticleRecord> parseFromJson(std::string json_str, use_protobuf_t);
}