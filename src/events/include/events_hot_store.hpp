#pragma once

#include <filesystem>
#include <mutex>

#include <sqlite3.h>

#include "events_store.hpp"
#include "events_archive.hpp"
#include "events_feed.hpp"
#include "events_shard.hpp"

namespace dcn::events
{
    struct CursorKey
    {
        int chain_id = 1;
        std::int64_t block_number = 0;
        std::int64_t tx_index = 0;
        std::int64_t log_index = 0;
        std::string feed_id;
    };

    class SQLiteHotStore final : public IHotEventStore, public IArchiveManager
        {
            public:
                SQLiteHotStore(
                    const std::filesystem::path & hot_db_path,
                    const std::filesystem::path & archive_root,
                    const std::int64_t outbox_retention_ms,
                    const int default_chain_id);

                ~SQLiteHotStore() override;

                std::optional<std::int64_t> loadNextFromBlock(const int chain_id) override;

                std::optional<std::uint64_t> loadNextLocalSeq(const int chain_id);

                bool saveNextLocalSeq(const int chain_id, const std::uint64_t next_seq, const std::int64_t now_ms);

                std::vector<std::int64_t> loadReorgWindowBlocks(
                    const int chain_id,
                    const std::int64_t from_block,
                    const std::int64_t to_block) const;

                bool ingestBatch(
                    const int chain_id,
                    const std::vector<DecodedEvent> & events,
                    const std::vector<ChainBlockInfo> & block_infos,
                    const std::int64_t next_from_block,
                    const std::int64_t now_ms) override;

                bool applyFinality(
                    const int chain_id,
                    const FinalityHeights & heights,
                    const std::int64_t now_ms,
                    const std::size_t reorg_window_blocks) override;
                
                std::size_t projectBatch(const std::size_t limit, const std::int64_t now_ms) override;
                

                bool runArchiveCycle(
                    const int chain_id,
                    const std::size_t hot_window_days,
                    const std::int64_t now_ms) override;
                
                bool runCycle(const int chain_id, const std::size_t hot_window_days, const std::int64_t now_ms) override;
                

                FeedPage getFeedPage(const FeedQuery & query) const;
                

                StreamPage getStreamPage(const StreamQuery & query) const;
                
                std::int64_t minAvailableStreamSeq() const;
                

            private:
                bool _initializeHotSchema();
                

                bool _initializeArchiveSchema(sqlite3 * archive_db) const;
                
                bool _exportMonth(const int chain_id, const std::string & month_token, const std::int64_t now_ms);
                
                std::vector<std::filesystem::path> _candidateArchivePaths(const std::optional<CursorKey> & before_key) const;
                
                void _appendFeedRowsFromDatabase(
                    sqlite3 * db,
                    const char * table_name,
                    const FeedQuery & query,
                    const std::optional<CursorKey> & before_key,
                    const std::size_t limit,
                    std::vector<FeedItem> & out_items,
                    std::set<std::string> & seen_feed_ids) const;
                
            private:
                std::filesystem::path _hot_db_path;
                std::filesystem::path _archive_root;
                std::int64_t _outbox_retention_ms = 0;
                sqlite3 * _write_db = nullptr;
                sqlite3 * _read_db = nullptr;
                mutable std::mutex _read_mutex;
                std::unique_ptr<IEventShardRouter> _shard_router;
                int _default_chain_id = 1;
        };
}