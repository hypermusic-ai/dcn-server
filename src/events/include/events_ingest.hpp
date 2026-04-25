#pragma once

#include <cstdint>
#include <optional>
#include <vector>

#include "decoded_event.hpp"

namespace dcn::events
{
    struct FinalityHeights
    {
        std::int64_t head = 0;
        std::int64_t safe = 0;
        std::int64_t finalized = 0;
    };

    class IEventSink
    {
        public:
            virtual ~IEventSink() = default;
            virtual void enqueueBatch(std::vector<DecodedEvent> events, std::int64_t next_from_block) = 0;
    };

    class IEventDecoder
    {
        public:
            virtual ~IEventDecoder() = default;
            virtual std::optional<DecodedEvent> decode(const RawChainLog & log) const = 0;
    };

    class PTEventDecoder final : public IEventDecoder
    {
        public:
            PTEventDecoder();

            std::optional<DecodedEvent> decode(const RawChainLog & log) const override;

        private:
            std::string _connector_topic;
            std::string _transformation_topic;
            std::string _condition_topic;
    };

    class IChainEventSource
    {
        public:
            virtual ~IChainEventSource() = default;

            virtual void start() = 0;
            virtual void stop() = 0;
            virtual bool running() const = 0;
    };
}
