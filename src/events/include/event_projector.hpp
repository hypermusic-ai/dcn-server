#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>

#include "native.h"
#include <asio.hpp>

namespace dcn::events
{
    constexpr std::string_view FEED_PROJECTOR_ID = "feed";
    constexpr std::string_view REGISTRY_PROJECTOR_ID = "registry";

    struct ChangeRecord
    {
        int chain_id = 0;
        std::string block_hash;
        std::int64_t log_index = 0;
        std::int64_t change_seq = 0;
        std::string event_type;
        std::string state;
        std::string name;
        std::string owner;
        std::string tx_hash;
        std::optional<std::int64_t> block_time;
        std::string data_hex;
        std::array<std::optional<std::string>, 4> topics;
        std::int64_t block_number = 0;
        std::int64_t tx_index = 0;
    };

    class IEventProjector
    {
        public:
            virtual ~IEventProjector() = default;
            virtual std::string_view id() const = 0;
            virtual std::int64_t cursor() const = 0;
            virtual asio::awaitable<std::size_t> projectBatch(std::size_t limit, std::int64_t now_ms) = 0;
    };
}
