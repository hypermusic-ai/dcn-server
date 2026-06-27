#include <algorithm>
#include <chrono>
#include <exception>
#include <limits>
#include <memory>
#include <spdlog/spdlog.h>
#include <tuple>
#include <variant>
#include <set>

#include "events.hpp"
#include "async.hpp"
#include "utils.hpp"
#include "parser.hpp"
#include "chain.hpp"
#include "evm.hpp"
#include "local_evm_source.hpp"

namespace dcn::events
{
    static std::string _resolveChainNamespace(const EventRuntimeConfig & config)
    {
        if(!config.chain_namespace.empty())
        {
            return config.chain_namespace;
        }

        return "local";
    }

    std::int64_t reorgLookbackStart(const std::int64_t next_from_block, const std::size_t reorg_window_blocks)
    {
        const std::int64_t lookback_blocks = static_cast<std::int64_t>(reorg_window_blocks);
        return (next_from_block > lookback_blocks) ? (next_from_block - lookback_blocks) : 0;
    }

    EventRuntime::EventRuntime(asio::io_context & io_context, EventRuntimeConfig cfg)
        : _io_context(io_context)
        , _config(std::move(cfg))
        , _write_strand(asio::make_strand(io_context))
        , _store(std::make_shared<SQLiteHotStore>(
            _config.hot_db_path,
            _config.archive_root,
            _config.outbox_retention_ms,
            _config.chain_id,
            _resolveChainNamespace(_config)))
        , _decoder(std::make_unique<PTEventDecoder>())
    {
    }

    EventRuntime::~EventRuntime()
    {
        requestStop();

        if (_active_loop_count.load(std::memory_order_acquire) != 0)
        {
            spdlog::critical("EventRuntime destroyed without awaiting stop()");
            std::terminate();
        }
    }

    void EventRuntime::start()
    {
        if(_running.exchange(true, std::memory_order_acq_rel))
        {
            return;
        }

        auto spawn_loop = [this](asio::awaitable<void> loop, const char * error_context)
        {
            _active_loop_count.fetch_add(1, std::memory_order_acq_rel);

            asio::co_spawn(
                _io_context,
                [loop = std::move(loop)]() mutable -> asio::awaitable<void>
                {
                    co_await std::move(loop);
                    co_return;
                },
                [this, error_context](std::exception_ptr e)
                {
                    if (e)
                    {
                        utils::logException(e, error_context);
                    }

                    _active_loop_count.fetch_sub(1, std::memory_order_acq_rel);
                });
        };

        spawn_loop(_runProjectorLoop(), "events projector loop failed");
        spawn_loop(_runArchiveLoop(), "events archive loop failed");
        spawn_loop(_runMaintenanceLoop(), "events maintenance loop failed");

        if(_config.ingestion_enabled)
        {
            // ponytail: one cursor per chain_id in the store; reject duplicate sources.
            // Per-source cursor keying is the upgrade path if same-chain failover is ever needed.
            std::set<int> seen_chain_ids;
            for(const auto & source : _config.sources)
            {
                if(!source)
                {
                    continue;
                }

                if(!seen_chain_ids.insert(source->chainId()).second)
                {
                    spdlog::error(
                        "events ingestion: duplicate source for chain_id={} skipped "
                        "(one source per chain_id; shared cursor would corrupt)",
                        source->chainId());
                    continue;
                }

                spawn_loop(_runSourceIngestionLoop(source), "events ingestion loop failed");
            }
        }
    }

    void EventRuntime::requestStop()
    {
        if(_stop_requested.exchange(true, std::memory_order_acq_rel))
        {
            return;
        }

        _running.store(false, std::memory_order_release);
    }

    asio::awaitable<void> EventRuntime::stop()
    {
        requestStop();
        co_await _waitForLoops();

        try
        {
            (void)co_await checkpointWal(storage::sqlite::WalCheckpointMode::TRUNCATE);
        }
        catch(...)
        {
            utils::logException(std::current_exception(), "events stop checkpoint failed");
        }

        _running.store(false, std::memory_order_release);
        spdlog::info("Events runtime stopped");
        co_return;
    }

    bool EventRuntime::running() const
    {
        return _running.load(std::memory_order_acquire);
    }

    bool EventRuntime::ingestionEnabled() const
    {
        return _config.ingestion_enabled;
    }

    FeedPage EventRuntime::getFeedPage(const FeedQuery & query) const
    {
        return _store->getFeedPage(query);
    }

    StreamPage EventRuntime::getStreamPage(const StreamQuery & query) const
    {
        return _store->getStreamPage(query);
    }

    std::int64_t EventRuntime::minAvailableStreamSeq() const
    {
        return _store->minAvailableStreamSeq();
    }

    asio::awaitable<void> EventRuntime::_sleepFor(const std::uint64_t ms) const
    {
        asio::steady_timer timer(co_await asio::this_coro::executor);

        std::uint64_t remaining_ms = ms;
        while(remaining_ms > 0 && !_stop_requested.load(std::memory_order_acquire))
        {
            const std::uint64_t slice_ms = std::min<std::uint64_t>(remaining_ms, 100);
            timer.expires_after(std::chrono::milliseconds(slice_ms));
            std::error_code ec;
            co_await timer.async_wait(asio::redirect_error(asio::use_awaitable, ec));
            if(ec == asio::error::operation_aborted)
            {
                break;
            }
            remaining_ms = (remaining_ms > slice_ms) ? (remaining_ms - slice_ms) : 0;
        }
        co_return;
    }

    asio::awaitable<std::optional<std::int64_t>> EventRuntime::_storeLoadNextFromBlock(const int chain_id) const
    {
        co_await async::ensureOnStrand(_write_strand);
        co_return _store->loadNextFromBlock(chain_id);
    }

    asio::awaitable<std::optional<std::uint64_t>> EventRuntime::_storeLoadNextLocalSeq(const int chain_id) const
    {
        co_await async::ensureOnStrand(_write_strand);
        co_return _store->loadNextLocalSeq(chain_id);
    }

    asio::awaitable<bool> EventRuntime::_storeSaveNextLocalSeq(
        const int chain_id,
        const std::uint64_t next_seq,
        const std::int64_t now_ms) const
    {
        co_await async::ensureOnStrand(_write_strand);
        co_return _store->saveNextLocalSeq(chain_id, next_seq, now_ms);
    }

    asio::awaitable<std::vector<std::int64_t>> EventRuntime::_storeLoadReorgWindowBlocks(
        const int chain_id,
        const std::int64_t from_block,
        const std::int64_t to_block) const
    {
        co_await async::ensureOnStrand(_write_strand);
        co_return _store->loadReorgWindowBlocks(chain_id, from_block, to_block);
    }

    asio::awaitable<bool> EventRuntime::_storeIngestBatch(
        const int chain_id,
        std::vector<RawChainLog> raw_events,
        std::vector<DecodedEvent> decoded_events,
        std::vector<ChainBlockInfo> block_infos,
        const std::int64_t next_from_block,
        const std::int64_t now_ms,
        const std::optional<std::uint64_t> next_local_seq) const
    {
        co_await async::ensureOnStrand(_write_strand);
        co_return _store->ingestBatch(
            chain_id,
            raw_events,
            decoded_events,
            block_infos,
            next_from_block,
            now_ms,
            next_local_seq);
    }

    asio::awaitable<bool> EventRuntime::_storeApplyFinality(
        const int chain_id,
        FinalityHeights heights,
        const std::int64_t now_ms,
        const std::size_t reorg_window_blocks) const
    {
        co_await async::ensureOnStrand(_write_strand);
        co_return _store->applyFinality(chain_id, heights, now_ms, reorg_window_blocks);
    }

    asio::awaitable<std::size_t> EventRuntime::_storeProjectBatch(const std::size_t limit, const std::int64_t now_ms) const
    {
        co_await async::ensureOnStrand(_write_strand);
        co_return _store->projectBatch(limit, now_ms);
    }

    asio::awaitable<bool> EventRuntime::_storeRunArchiveCycle(
        const int chain_id,
        const std::size_t hot_window_days,
        const std::int64_t now_ms) const
    {
        co_await async::ensureOnStrand(_write_strand);
        co_return _store->runArchiveCycle(chain_id, hot_window_days, now_ms);
    }

    asio::awaitable<storage::sqlite::WalCheckpointStats> EventRuntime::checkpointWal(storage::sqlite::WalCheckpointMode mode) const
    {
        co_await async::ensureOnStrand(_write_strand);
        co_return _store->checkpointWal(mode);
    }

    asio::awaitable<void> EventRuntime::_runSourceIngestionLoop(std::shared_ptr<IEmittedLogSource> source)
    {
        const int chain_id = source->chainId();
        const bool ephemeral_entities = source->ephemeralEntities();

        spdlog::info("Events ingestion loop started for chain={}", chain_id);
        std::uint64_t next_seq = 0;
        std::optional<std::int64_t> next_from_block_hint = std::nullopt;

        if(const auto persisted = co_await _storeLoadNextLocalSeq(chain_id); persisted.has_value())
        {
            next_seq = *persisted;
        }
        next_from_block_hint = co_await _storeLoadNextFromBlock(chain_id);

        const std::size_t poll_limit = std::max<std::size_t>(1, std::min<std::size_t>(MAX_STREAM_LIMIT, _config.block_batch_size * 8ULL));
        constexpr const char* EPHEMERAL_ENTITY_ADDRESS = "0x0";

        while(!_stop_requested.load(std::memory_order_acquire))
        {
            const SourcePoll poll = co_await source->pollSince(next_seq, poll_limit);
            const std::vector<evm::EVM::EmittedLogRecord> & emitted_logs = poll.records;
            const std::int64_t head_block = poll.finality.head;
            const std::int64_t source_next_block = std::max<std::int64_t>(head_block, 0) + 1;
            const bool source_regressed =
                next_from_block_hint.has_value() && (source_next_block < *next_from_block_hint);
            const bool missing_block_cursor_with_progress =
                !next_from_block_hint.has_value() && emitted_logs.empty() && next_seq > 0;

            if(source_regressed || missing_block_cursor_with_progress)
            {
                spdlog::warn(
                    "Events local ingestion cursor rewind detected: source_next_block={} persisted_next_from_block={} "
                    "persisted_next_local_seq={}; resetting local cursor to seq=0",
                    source_next_block,
                    next_from_block_hint.value_or(-1),
                    next_seq);
                const std::int64_t rewind_reset_ms = utils::nowMs();
                next_seq = 0;
                next_from_block_hint = source_next_block;
                if(!co_await _storeSaveNextLocalSeq(chain_id, 0, rewind_reset_ms))
                {
                    spdlog::warn("Failed to persist ingestion cursor reset for chain={}", chain_id);
                }
            }

            if(emitted_logs.empty())
            {
                if(!co_await _storeApplyFinality(chain_id, poll.finality, utils::nowMs(), _config.reorg_window_blocks))
                {
                    spdlog::warn("Failed to apply finality for chain={}, during ingestion loop", chain_id);
                }

                co_await _sleepFor(_config.poll_interval_ms);

                continue;
            }

            std::vector<RawChainLog> raw_logs;
            raw_logs.reserve(emitted_logs.size());

            std::vector<DecodedEvent> decoded_events;
            decoded_events.reserve(emitted_logs.size());

            std::unordered_map<std::int64_t, ChainBlockInfo> block_map;
            block_map.reserve(emitted_logs.size());

            for(const auto & row : emitted_logs)
            {
                ChainBlockInfo info{
                    .chain_id = chain_id,
                    .block_number = row.block_number,
                    .block_hash = chain::normalizeHex(row.block_hash),
                    .parent_hash = chain::normalizeHex(row.parent_hash),
                    .block_time = row.block_time,
                    .seen_at_ms = utils::nowMs()
                };

                block_map.insert_or_assign(info.block_number, std::move(info));

                RawChainLog raw{
                    .chain_id = chain_id,
                    .block_number = row.block_number,
                    .block_hash = chain::normalizeHex(row.block_hash),
                    .parent_hash = chain::normalizeHex(row.parent_hash),
                    .tx_index = row.tx_index,
                    .tx_hash = chain::normalizeHex(row.tx_hash),
                    .log_index = row.log_index,
                    .address = chain::normalizeHex(row.address),
                    .topics = {},
                    .data_hex = chain::normalizeHex(row.data_hex),
                    .removed = false,
                    .block_time = row.block_time,
                    .seen_at_ms = utils::nowMs()
                };

                for(std::size_t i = 0; i < std::min<std::size_t>(raw.topics.size(), row.topics.size()); ++i)
                {
                    raw.topics[i] = chain::normalizeHex(row.topics[i]);
                }

                raw_logs.push_back(raw);
                const auto decoded = _decoder->decode(raw);

                if(decoded.has_value())
                {
                    DecodedEvent event = std::move(*decoded);

                    if(ephemeral_entities)
                    {
                        event.entity_address = EPHEMERAL_ENTITY_ADDRESS;

                        json payload = json::parse(event.decoded_json, nullptr, false);

                        if(!payload.is_discarded() && payload.is_object())
                        {
                            switch(event.event_type)
                            {
                            case EventType::CONNECTOR_ADDED:
                                payload["connector_address"] = EPHEMERAL_ENTITY_ADDRESS;
                                break;
                            case EventType::TRANSFORMATION_ADDED:
                                payload["transformation_address"] = EPHEMERAL_ENTITY_ADDRESS;
                                break;
                            case EventType::CONDITION_ADDED:
                                payload["condition_address"] = EPHEMERAL_ENTITY_ADDRESS;
                                break;
                            }
                            event.decoded_json = payload.dump(-1, ' ', false, json::error_handler_t::replace);
                        }
                    }

                    decoded_events.push_back(std::move(event));
                }
            }

            std::vector<ChainBlockInfo> block_infos;
            block_infos.reserve(block_map.size());

            for(auto & [_, info] : block_map)
            {
                block_infos.push_back(std::move(info));
            }

            std::ranges::sort(block_infos, [](const ChainBlockInfo & lhs, const ChainBlockInfo & rhs)
            {
                return lhs.block_number < rhs.block_number;
            });

            const std::int64_t max_seen_block = block_infos.empty()
                ? std::max<std::int64_t>(head_block, 0)
                : block_infos.back().block_number;
            const std::int64_t next_block = max_seen_block + 1;

            const bool ingest_ok = co_await _storeIngestBatch(
                chain_id,
                std::move(raw_logs),
                std::move(decoded_events),
                std::move(block_infos),
                next_block,
                utils::nowMs(),
                poll.next_cursor);

            if(!ingest_ok)
            {
                co_await _sleepFor(_config.poll_interval_ms);
                continue;
            }

            if(!co_await _storeApplyFinality(chain_id, poll.finality, utils::nowMs(), _config.reorg_window_blocks))
            {
                spdlog::error("Failed to apply finality during ingestion loop for chain={}", chain_id);
            }

            next_seq = poll.next_cursor;
            next_from_block_hint = next_block;

            if(emitted_logs.size() < poll_limit)
            {
                co_await _sleepFor(_config.poll_interval_ms);
            }
        }

        co_await source->stop();
        spdlog::info("Events ingestion loop stopped for chain={}", chain_id);
        co_return;
    }

    asio::awaitable<void> EventRuntime::_runProjectorLoop()
    {
        spdlog::info("Events projector loop started");

        while(!_stop_requested.load(std::memory_order_acquire))
        {
            const std::size_t projected = co_await _storeProjectBatch(DEFAULT_PROJECT_BATCH_SIZE, utils::nowMs());

            if(projected == 0)
            {
                co_await _sleepFor(_config.projector_interval_ms);
            }
        }

        spdlog::info("Events projector loop stopped");
        co_return;
    }

    asio::awaitable<void> EventRuntime::_runArchiveLoop()
    {
        spdlog::info("Events archive loop started");

        while(!_stop_requested.load(std::memory_order_acquire))
        {
            co_await _sleepFor(_config.archive_interval_ms);
            if(_stop_requested.load(std::memory_order_acquire))
            {
                break;
            }

            (void)co_await _storeRunArchiveCycle(_config.chain_id, _config.hot_window_days, utils::nowMs());
        }

        spdlog::info("Events archive loop stopped");
        co_return;
    }

    asio::awaitable<void> EventRuntime::_runMaintenanceLoop()
    {
        spdlog::info("Events maintenance loop started");

        while(!_stop_requested.load(std::memory_order_acquire))
        {
            co_await _sleepFor(_config.wal_checkpoint_interval_ms);
            if(_stop_requested.load(std::memory_order_acquire))
            {
                break;
            }

            (void)co_await checkpointWal(storage::sqlite::WalCheckpointMode::PASSIVE);
        }

        spdlog::info("Events maintenance loop stopped");
        co_return;
    }

    asio::awaitable<void> EventRuntime::_waitForLoops()
    {
        asio::steady_timer timer(co_await asio::this_coro::executor);

        while(_active_loop_count.load(std::memory_order_acquire) > 0)
        {
            timer.expires_after(std::chrono::milliseconds(5));
            std::error_code ec;
            co_await timer.async_wait(asio::redirect_error(asio::use_awaitable, ec));
            if(ec == asio::error::operation_aborted)
            {
                continue;
            }
        }
        co_return;
    }

}
