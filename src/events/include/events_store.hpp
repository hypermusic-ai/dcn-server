#pragma once

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <vector>

#include "decoded_event.hpp"
#include "events_ingest.hpp"

namespace dcn::events
{
    struct IngestResumeState
    {
        int chain_id = 1;
        std::int64_t next_from_block = 0;
        std::int64_t updated_at_ms = 0;
    };

    struct FinalityState
    {
        int chain_id = 1;
        std::int64_t head_block = 0;
        std::int64_t safe_block = 0;
        std::int64_t finalized_block = 0;
        std::int64_t updated_at_ms = 0;
    };

    struct ChainBlockInfo
    {
        int chain_id = 1;
        std::int64_t block_number = 0;
        std::string block_hash;
        std::string parent_hash;
        std::int64_t block_time = 0;
        std::int64_t seen_at_ms = 0;
    };

    class IHotEventStore
    {
        public:
            virtual ~IHotEventStore() = default;

            virtual std::optional<std::int64_t> loadNextFromBlock(int chain_id) = 0;
            virtual bool ingestBatch(
                int chain_id,
                const std::vector<DecodedEvent> & events,
                const std::vector<ChainBlockInfo> & block_infos,
                std::int64_t next_from_block,
                std::int64_t now_ms) = 0;
            virtual bool applyFinality(
                int chain_id,
                const FinalityHeights & heights,
                std::int64_t now_ms,
                std::size_t reorg_window_blocks) = 0;
            virtual std::size_t projectBatch(std::size_t limit, std::int64_t now_ms) = 0;
            virtual bool runArchiveCycle(
                int chain_id,
                std::size_t hot_window_days,
                std::int64_t now_ms) = 0;
    };
}
