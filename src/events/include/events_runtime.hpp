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

#include "emitted_log_source.hpp"
#include "event_projector.hpp"
#include "sqlite_hot_store.hpp"

namespace dcn::events
{
    constexpr std::size_t DEFAULT_PROJECT_BATCH_SIZE = 256;

    std::int64_t reorgLookbackStart(std::int64_t next_from_block, std::size_t reorg_window_blocks);


    struct EventRuntimeConfig
    {
        std::filesystem::path hot_db_path;

        int chain_id = 1;
        bool ingestion_enabled = false;
        std::vector<std::shared_ptr<IEmittedLogSource>> sources;
        std::optional<std::int64_t> start_block = std::nullopt;
        unsigned int poll_interval_ms = 5000;
        unsigned int confirmations = 12;
        unsigned int block_batch_size = 500;

        std::size_t reorg_window_blocks = 2048;

        unsigned int projector_interval_ms = 200;
        unsigned int prune_interval_ms = 5000;
        unsigned int wal_checkpoint_interval_ms = 15 * 1000;
    };

    class EventRuntime final : public storage::sqlite::IWalStore
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

            void addProjector(std::unique_ptr<IEventProjector> projector);
            SQLiteHotStore & projectionStore();
            const asio::strand<asio::io_context::executor_type> & writeStrand() const { return _write_strand; }

            asio::awaitable<storage::sqlite::WalCheckpointStats> checkpointWal(storage::sqlite::WalCheckpointMode mode) const override;

        private:
            asio::awaitable<void> _sleepFor(const std::uint64_t ms) const;

            asio::awaitable<std::optional<std::int64_t>> _storeLoadNextFromBlock(int chain_id) const;
            asio::awaitable<std::optional<std::uint64_t>> _storeLoadNextLocalSeq(int chain_id) const;
            asio::awaitable<bool> _storeSaveNextLocalSeq(int chain_id, std::uint64_t next_seq, std::int64_t now_ms) const;
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

            asio::awaitable<void> _runSourceIngestionLoop(std::shared_ptr<IEmittedLogSource> source);
            asio::awaitable<void> _runProjectorLoop();
            asio::awaitable<void> _runPruneLoop();
            asio::awaitable<void> _runMaintenanceLoop();
            asio::awaitable<void> _waitForLoops();

        private:
            asio::io_context & _io_context;
            EventRuntimeConfig _config;
            asio::strand<asio::io_context::executor_type> _write_strand;

            std::shared_ptr<SQLiteHotStore> _store;
            std::unique_ptr<IEventDecoder> _decoder;

            std::vector<std::unique_ptr<IEventProjector>> _projectors;

            std::atomic<bool> _stop_requested{false};
            std::atomic<bool> _running{false};

            std::atomic<std::size_t> _active_loop_count{0};
    };
}
