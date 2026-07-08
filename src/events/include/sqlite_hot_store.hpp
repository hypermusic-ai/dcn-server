#pragma once

#include <cstdint>
#include <filesystem>
#include <optional>
#include <string>
#include <vector>

#include <sqlite3.h>

#include "sqlite/wal.hpp"

#include "event_projector.hpp"
#include "events_store.hpp"

namespace dcn::events
{
    class SQLiteHotStore final : public IHotEventStore
    {
        public:
            SQLiteHotStore(
                const std::filesystem::path & hot_db_path,
                const int default_chain_id);

            ~SQLiteHotStore() override;

            // ---- hot DB / serialized by runtime strand / synchronous ----

            std::optional<std::int64_t> loadNextFromBlock(const int chain_id) override;
            std::optional<std::uint64_t> loadNextLocalSeq(const int chain_id);

            bool saveNextLocalSeq(const int chain_id, const std::uint64_t next_seq, const std::int64_t now_ms);

            std::vector<std::int64_t> loadReorgWindowBlocks(
                    const int chain_id,
                    const std::int64_t from_block,
                    const std::int64_t to_block) const;

            bool ingestBatch(
                    const int chain_id,
                    const std::vector<RawChainLog> & raw_events,
                    const std::vector<DecodedEvent> & decoded_events,
                    const std::vector<ChainBlockInfo> & block_infos,
                    const std::int64_t next_from_block,
                    const std::int64_t now_ms,
                    const std::optional<std::uint64_t> next_local_seq = std::nullopt) override;

            bool ingestBatch(
                    const int chain_id,
                    const std::vector<DecodedEvent> & events,
                    const std::vector<ChainBlockInfo> & block_infos,
                    const std::int64_t next_from_block,
                    const std::int64_t now_ms);

            bool applyFinality(
                    const int chain_id,
                    const FinalityHeights & heights,
                    const std::int64_t now_ms,
                    const std::size_t reorg_window_blocks) override;

            // ---- min-cursor pruning / write-strand only ----

            std::size_t pruneConsumedRaw(
                    std::int64_t watermark,
                    std::int64_t finalized_floor_block,
                    std::size_t batch_limit);

            std::int64_t loadHeadBlock(int chain_id);

            storage::sqlite::WalCheckpointStats checkpointWal(storage::sqlite::WalCheckpointMode mode);

            // ---- read side / synchronous ----

            std::vector<ChangeRecord> readChangesSince(std::int64_t after_change_seq, std::size_t limit) const;

        private:
            bool _initializeHotSchema();
            std::int64_t _nextChangeSeq();

        private:
            std::filesystem::path _hot_db_path;

            sqlite3 * _write_db = nullptr;
            sqlite3 * _read_db = nullptr;

            int _default_chain_id = 1;
    };
}
