#pragma once

#include <array>
#include <cstdint>
#include <optional>
#include <string>

#include "parser.hpp"

namespace dcn::events
{
    constexpr std::string_view CONNECTOR_ADDED_TYPE = "connector_added";
    constexpr std::string_view TRANSFORMATION_ADDED_TYPE = "transformation_added";
    constexpr std::string_view CONDITION_ADDED_TYPE = "condition_added";

    constexpr std::string_view OBSERVED_STATE = "observed";
    constexpr std::string_view SAFE_STATE = "safe";
    constexpr std::string_view FINALIZED_STATE = "finalized";
    constexpr std::string_view REMOVED_STATE = "removed";

    enum class EventState : std::uint8_t
    {
        OBSERVED = 0,
        SAFE = 1,
        FINALIZED = 2,
        REMOVED = 3
    };

    enum class EventType : std::uint8_t
    {
        CONNECTOR_ADDED = 0,
        TRANSFORMATION_ADDED = 1,
        CONDITION_ADDED = 2
    };

    struct RawChainLog
    {
        int chain_id = 1;
        std::int64_t block_number = 0;
        std::string block_hash;
        std::string parent_hash;
        std::int64_t tx_index = 0;
        std::string tx_hash;
        std::int64_t log_index = 0;
        std::string address;
        std::array<std::optional<std::string>, 4> topics{};
        std::string data_hex;
        bool removed = false;
        std::optional<std::int64_t> block_time = std::nullopt;
        std::int64_t seen_at_ms = 0;
    };

    struct DecodedEvent
    {
        RawChainLog raw;
        EventType event_type = EventType::CONNECTOR_ADDED;
        EventState state = EventState::OBSERVED;

        std::string name;
        std::string caller;
        std::string owner;
        std::string entity_address;
        std::optional<std::uint32_t> args_count = std::nullopt;
        std::optional<std::string> format_hash = std::nullopt;

        std::string decoded_json;
    };

    std::string toString(EventState state);
    std::string toString(EventType type);
}

namespace dcn::parse
{
    Result<dcn::events::EventState> parseEventState(const std::string & value);
    Result<dcn::events::EventType> parseEventType(const std::string & value);

    Result<dcn::events::RawChainLog> parseRawLog(const json & log_json, const std::int64_t seen_at_ms, int chain_id);
}
