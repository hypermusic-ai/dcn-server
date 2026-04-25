#pragma once

#include <atomic>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "native.h"
#include <asio.hpp>

#include "sqlite/wal_store.hpp"

#include "events_feed.hpp"
#include "sqlite_hot_store.hpp"

namespace dcn::evm
{
    class EVM;
}

namespace dcn::events
{
    constexpr std::size_t DEFAULT_PROJECT_BATCH_SIZE = 256;

    std::int64_t reorgLookbackStart(std::int64_t next_from_block, std::size_t reorg_window_blocks);


    struct EventRuntimeConfig
    {
        std::filesystem::path hot_db_path;
        std::filesystem::path archive_root;

        int chain_id = 1;
        bool ingestion_enabled = false;
        bool use_local_evm_source = false;
        evm::EVM * local_evm = nullptr;
        std::string rpc_url;
        std::string registry_address;
        std::optional<std::int64_t> start_block = std::nullopt;
        unsigned int rpc_timeout_ms = 7000;
        unsigned int poll_interval_ms = 5000;
        unsigned int confirmations = 12;
        unsigned int block_batch_size = 500;

        std::size_t hot_window_days = 90;
        std::size_t reorg_window_blocks = 2048;
        std::int64_t outbox_retention_ms = 7LL * 24 * 60 * 60 * 1000;

        unsigned int projector_interval_ms = 200;
        unsigned int archive_interval_ms = 30 * 1000;
        unsigned int wal_checkpoint_interval_ms = 15 * 1000;
    };

    class EventRuntime final : public IFeedRepository, public storage::sqlite::IWalStore
    {
        public:
            EventRuntime(asio::io_context & io_context, EventRuntimeConfig config);
            ~EventRuntime();

            EventRuntime(const EventRuntime &) = delete;
            EventRuntime & operator=(const EventRuntime &) = delete;

            EventRuntime(EventRuntime &&) = delete;
            EventRuntime & operator=(EventRuntime &&) = delete;

            void start();
            void requestStop();
            asio::awaitable<void> stop();
            bool running() const;
            bool ingestionEnabled() const;
            bool blockingTransportObservedOnHotWriteStrand() const;
            std::uint64_t rpcTransportCallCount() const;

            FeedPage getFeedPage(const FeedQuery & query) const override;
            StreamPage getStreamPage(const StreamQuery & query) const override;
            std::int64_t minAvailableStreamSeq() const override;
        
            asio::awaitable<storage::sqlite::WalCheckpointStats> checkpointWal(storage::sqlite::WalCheckpointMode mode) const override;

        private:
            asio::awaitable<void> _sleepFor(const std::uint64_t ms) const;
            asio::awaitable<std::optional<nlohmann::json>> _rpcResult(const std::string & method, nlohmann::json params) const;

            asio::awaitable<std::optional<std::int64_t>> _storeLoadNextFromBlock(int chain_id) const;
            asio::awaitable<std::optional<std::uint64_t>> _storeLoadNextLocalSeq(int chain_id) const;
            asio::awaitable<std::vector<std::int64_t>> _storeLoadReorgWindowBlocks(
                int chain_id,
                std::int64_t from_block,
                std::int64_t to_block) const;
            asio::awaitable<bool> _storeIngestBatch(
                int chain_id,
                std::vector<RawChainLog> raw_events,
                std::vector<DecodedEvent> decoded_events,
                std::vector<ChainBlockInfo> block_infos,
                std::int64_t next_from_block,
                std::int64_t now_ms,
                std::optional<std::uint64_t> next_local_seq = std::nullopt) const;
            asio::awaitable<bool> _storeApplyFinality(
                int chain_id,
                FinalityHeights heights,
                std::int64_t now_ms,
                std::size_t reorg_window_blocks) const;
            asio::awaitable<std::size_t> _storeProjectBatch(std::size_t limit, std::int64_t now_ms) const;
            asio::awaitable<bool> _storeRunArchiveCycle(int chain_id, std::size_t hot_window_days, std::int64_t now_ms) const;

            asio::awaitable<parse::Result<std::int64_t>> _ethBlockNumber() const;
            asio::awaitable<parse::Result<std::int64_t>> _ethTaggedBlockNumber(const std::string & tag) const;
            asio::awaitable<std::optional<ChainBlockInfo>> _ethGetBlockInfo(const std::int64_t block_number) const;
            asio::awaitable<std::optional<nlohmann::json>> _ethGetLogs(const std::int64_t from_block, const std::int64_t to_block) const;

            asio::awaitable<FinalityHeights> _resolveFinality(const std::int64_t head) const;

            asio::awaitable<void> _runLocalIngestionLoop();
            asio::awaitable<void> _runIngestionLoop();
            asio::awaitable<void> _runProjectorLoop();
            asio::awaitable<void> _runArchiveLoop();
            asio::awaitable<void> _runMaintenanceLoop();
            asio::awaitable<void> _waitForLoops();

        private:
            asio::io_context & _io_context;
            EventRuntimeConfig _config;
            mutable asio::thread_pool _rpc_pool{2};
            asio::strand<asio::io_context::executor_type> _write_strand;
            
            std::shared_ptr<SQLiteHotStore> _store;
            std::unique_ptr<IEventDecoder> _decoder;

            std::atomic<bool> _stop_requested{false};
            std::atomic<bool> _running{false};

            mutable std::atomic<bool> _blocking_transport_on_hot_write_strand{false};
            mutable std::atomic<std::uint64_t> _rpc_transport_call_count{0};

            std::atomic<std::size_t> _active_loop_count{0};
            std::atomic<bool> _rpc_pool_joined{false};
    };
}
