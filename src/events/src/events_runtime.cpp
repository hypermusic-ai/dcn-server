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
#include "rpc_client.hpp"

namespace dcn::events
{
    static std::string _resolveChainNamespace(const EventRuntimeConfig & config)
    {
        if(!config.chain_namespace.empty())
        {
            return config.chain_namespace;
        }

        return config.use_local_evm_source ? "local" : "eth";
    }

    std::int64_t reorgLookbackStart(const std::int64_t next_from_block, const std::size_t reorg_window_blocks)
    {
        const std::int64_t lookback_blocks = static_cast<std::int64_t>(reorg_window_blocks);
        return (next_from_block > lookback_blocks) ? (next_from_block - lookback_blocks) : 0;
    }

    static std::optional<std::int64_t> _parseEthTaggedBlockNumberResult(const json & result)
    {
        if(!result.is_object() || result.is_null())
        {
            return std::nullopt;
        }

        const auto number_it = result.find("number");
        if(number_it == result.end() || !number_it->is_string())
        {
            return std::nullopt;
        }

        const auto parsed = parse::parseHexQuantity(number_it->get<std::string>());
        if(!parsed.has_value())
        {
            return std::nullopt;
        }

        return *parsed;
    }

    static std::optional<std::int64_t> _parseEthHeadBlockNumberResult(const json & result)
    {
        if(!result.is_string())
        {
            return std::nullopt;
        }

        const auto parsed = parse::parseHexQuantity(result.get<std::string>());
        if(!parsed.has_value())
        {
            return std::nullopt;
        }

        return *parsed;
    }

    static std::optional<ChainBlockInfo> _parseEthBlockInfoResult(
        const json & result,
        const std::int64_t requested_block_number,
        const int chain_id)
    {
        if(!result.is_object() || result.is_null())
        {
            return std::nullopt;
        }

        if(!result.contains("number") || !result.at("number").is_string() ||
            !result.contains("hash") || !result.at("hash").is_string() ||
            !result.contains("parentHash") || !result.at("parentHash").is_string() ||
            !result.contains("timestamp") || !result.at("timestamp").is_string())
        {
            return std::nullopt;
        }

        const auto parsed_number = parse::parseHexQuantity(result.at("number").get<std::string>());
        const auto parsed_time = parse::parseHexQuantity(result.at("timestamp").get<std::string>());
        if(!parsed_number.has_value() || !parsed_time.has_value())
        {
            return std::nullopt;
        }

        if(*parsed_number != requested_block_number)
        {
            spdlog::warn(
                "eth_getBlockByNumber returned block {} when requesting block {}; rejecting response",
                *parsed_number,
                requested_block_number);
            return std::nullopt;
        }

        std::string hash = chain::normalizeHex(result.at("hash").get<std::string>());
        std::string parent_hash = chain::normalizeHex(result.at("parentHash").get<std::string>());
        if(hash.empty() || parent_hash.empty())
        {
            return std::nullopt;
        }

        return ChainBlockInfo{
            .chain_id = chain_id,
            .block_number = *parsed_number,
            .block_hash = std::move(hash),
            .parent_hash = std::move(parent_hash),
            .block_time = *parsed_time,
            .seen_at_ms = utils::nowMs()
        };
    }

    EventRuntime::EventRuntime(asio::io_context & io_context, EventRuntimeConfig cfg)
        : _io_context(io_context)
        , _config(std::move(cfg))
        , _write_strand(asio::make_strand(io_context))
        , _rpc_client(std::make_shared<RpcClient>(_config.rpc_url, _config.rpc_timeout_ms))
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
            spawn_loop(_runIngestionLoop(), "events ingestion loop failed");
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

        _rpc_client->stop();
        spdlog::info("RPC pool stopped");

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

    bool EventRuntime::blockingTransportObservedOnHotWriteStrand() const
    {
        return _blocking_transport_on_hot_write_strand.load(std::memory_order_acquire);
    }

    std::uint64_t EventRuntime::rpcTransportCallCount() const
    {
        return _rpc_client->callCount();
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


    json EventRuntime::_rpcCall(const std::string & method, json params) const
    {
        if(_write_strand.running_in_this_thread())
        {
            _blocking_transport_on_hot_write_strand.store(true, std::memory_order_release);
        }

        // TEMPORARY GCC 13 workaround:
        // Coroutine-based async RPC helper paths (co_await + callback/async initiation)
        // repeatedly trigger internal compiler error:
        // "internal compiler error: in build_special_member_call, at cp/call.cc:11096".
        // Keep this synchronous until we have a reliable compiler-side fix/minimal repro.
        return _rpc_client->callSync(method, std::move(params));
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

    asio::awaitable<FinalityHeights> EventRuntime::_resolveFinality(const std::int64_t head) const
    {
        FinalityHeights heights{
            .head = std::max<std::int64_t>(0, head),
            .safe = std::max<std::int64_t>(0, head - static_cast<std::int64_t>(_config.confirmations)),
            .finalized = std::max<std::int64_t>(0, head - static_cast<std::int64_t>(_config.confirmations))
        };

        json safe_params = json::array();
        safe_params.push_back("safe");
        safe_params.push_back(false);
        json safe_result = _rpcCall("eth_getBlockByNumber", std::move(safe_params));
        const auto safe = _parseEthTaggedBlockNumberResult(safe_result);

        json finalized_params = json::array();
        finalized_params.push_back("finalized");
        finalized_params.push_back(false);
        json finalized_result = _rpcCall("eth_getBlockByNumber", std::move(finalized_params));
        const auto finalized = _parseEthTaggedBlockNumberResult(finalized_result);

        if(safe.has_value() && *safe >= 0 && *safe <= head)
        {
            heights.safe = *safe;
        }

        if(finalized.has_value() && *finalized >= 0 && *finalized <= heights.safe)
        {
            heights.finalized = *finalized;
        }

        heights.safe = std::clamp<std::int64_t>(heights.safe, 0, heights.head);
        heights.finalized = std::clamp<std::int64_t>(heights.finalized, 0, heights.safe);
        co_return heights;
    }

    asio::awaitable<void> EventRuntime::_runLocalIngestionLoop()
    {
        if(_config.local_evm == nullptr)
        {
            co_return;
        }

        spdlog::info("Events ingestion loop (local EVM source) started for chain={}", _config.chain_id);
        std::uint64_t next_seq = 0;
        std::optional<std::int64_t> next_from_block_hint = std::nullopt;

        if(const auto persisted = co_await _storeLoadNextLocalSeq(_config.chain_id); persisted.has_value())
        {
            next_seq = *persisted;
        }
        next_from_block_hint = co_await _storeLoadNextFromBlock(_config.chain_id);

        const std::size_t poll_limit = std::max<std::size_t>(1, std::min<std::size_t>(MAX_STREAM_LIMIT, _config.block_batch_size * 8ULL));
        constexpr const char* EPHEMERAL_ENTITY_ADDRESS = "0x0";

        while(!_stop_requested.load(std::memory_order_acquire))
        {
            const std::vector<evm::EVM::EmittedLogRecord> emitted_logs = co_await _config.local_evm->getLogsSince(next_seq, poll_limit);
            const std::int64_t head_block = co_await _config.local_evm->getHeadBlockNumber();
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
                if(!co_await _storeSaveNextLocalSeq(_config.chain_id, 0, rewind_reset_ms))
                {
                    spdlog::warn("Failed to persist local ingestion cursor reset for chain={}", _config.chain_id);
                }
            }

            if(emitted_logs.empty())
            {
                const FinalityHeights heights{
                    .head = std::max<std::int64_t>(head_block, 0),
                    .safe = std::max<std::int64_t>(head_block, 0),
                    .finalized = std::max<std::int64_t>(head_block, 0)
                };

                if(!co_await _storeApplyFinality(_config.chain_id, heights, utils::nowMs(), _config.reorg_window_blocks))
                {
                    spdlog::warn("Failed to apply finality for chain={}, during ingestion loop (local EVM source)", _config.chain_id);
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
                    .chain_id = _config.chain_id,
                    .block_number = row.block_number,
                    .block_hash = chain::normalizeHex(row.block_hash),
                    .parent_hash = chain::normalizeHex(row.parent_hash),
                    .block_time = row.block_time,
                    .seen_at_ms = utils::nowMs()
                };

                block_map.insert_or_assign(info.block_number, std::move(info));

                RawChainLog raw{
                    .chain_id = _config.chain_id,
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
            const std::uint64_t last_seq = emitted_logs.back().seq;

            const bool ingest_ok = co_await _storeIngestBatch(
                _config.chain_id,
                std::move(raw_logs),
                std::move(decoded_events),
                std::move(block_infos),
                next_block,
                utils::nowMs(),
                last_seq + 1);

            if(!ingest_ok)
            {
                co_await _sleepFor(_config.poll_interval_ms);
                continue;
            }

            const FinalityHeights heights{
                .head = std::max<std::int64_t>(head_block, max_seen_block),
                .safe = std::max<std::int64_t>(head_block, max_seen_block),
                .finalized = std::max<std::int64_t>(head_block, max_seen_block)
            };

            if(!co_await _storeApplyFinality(_config.chain_id, heights, utils::nowMs(), _config.reorg_window_blocks))
            {
                spdlog::error("Failed to apply finality, during ingestion loop (local EVM source)");
            }

            next_seq = last_seq + 1;
            next_from_block_hint = next_block;

            if(emitted_logs.size() < poll_limit)
            {
                co_await _sleepFor(_config.poll_interval_ms);
            }
        }

        spdlog::info("Events ingestion loop (local EVM source) stopped");
        co_return;
    }

    asio::awaitable<void> EventRuntime::_runIngestionLoop()
    {
        if(_config.use_local_evm_source && _config.local_evm != nullptr)
        {
            co_return co_await _runLocalIngestionLoop();
        }

        std::optional<std::int64_t> next_from_block = std::nullopt;
        spdlog::info("Events ingestion loop started for chain={} registry={}", _config.chain_id, _config.registry_address);

        bool should_backoff = false;

        while(!_stop_requested.load(std::memory_order_acquire))
        {
            try
            {
                json head_result = _rpcCall("eth_blockNumber", json::array());
                const auto head_opt = _parseEthHeadBlockNumberResult(head_result);
                if(!head_opt.has_value())
                {
                    co_await _sleepFor(_config.poll_interval_ms);
                    continue;
                }

                const std::int64_t head = *head_opt;
                const FinalityHeights heights = co_await _resolveFinality(head);

                if(!next_from_block.has_value())
                {
                    const auto persisted_next = co_await _storeLoadNextFromBlock(_config.chain_id);

                    if(persisted_next.has_value())
                    {
                        next_from_block = persisted_next;
                    }
                    else if(_config.start_block.has_value())
                    {
                        next_from_block = _config.start_block;
                    }
                    else
                    {
                        next_from_block = head;
                    }
                }

                if(!next_from_block.has_value())
                {
                    co_await _sleepFor(_config.poll_interval_ms);
                    continue;
                }

                if (*next_from_block > head)
                {
                    const std::int64_t cursor_gap = *next_from_block - head;
                    const std::int64_t rewind_threshold =
                        std::max<std::int64_t>(16, static_cast<std::int64_t>(_config.confirmations) * 2);
                    if (cursor_gap > rewind_threshold)
                    {
                        const std::int64_t rewind_to = _config.start_block.has_value()
                            ? std::max<std::int64_t>(0, *_config.start_block)
                            : std::max<std::int64_t>(0, head);
                        spdlog::warn(
                            "Events ingestion cursor rewind detected: persisted_next_from_block={} head={} gap={} "
                            "(threshold={}); rewinding to {}",
                            *next_from_block,
                            head,
                            cursor_gap,
                            rewind_threshold,
                            rewind_to);
                        next_from_block = rewind_to;
                        continue;
                    }

                    if (!co_await _storeApplyFinality(_config.chain_id, heights, utils::nowMs(), _config.reorg_window_blocks))
                    {
                        spdlog::error("Failed to apply finality while next_from_block is ahead of head");
                    }

                    co_await _sleepFor(_config.poll_interval_ms);
                    continue;
                }

                const std::int64_t max_to_block = (std::numeric_limits<std::int64_t>::max() - static_cast<std::int64_t>(_config.block_batch_size))
                    > *next_from_block
                    ? (*next_from_block + static_cast<std::int64_t>(_config.block_batch_size) - 1)
                    : std::numeric_limits<std::int64_t>::max();
                const std::int64_t to_block = std::min<std::int64_t>(head, max_to_block);
                const std::int64_t from_block = reorgLookbackStart(*next_from_block, _config.reorg_window_blocks);

                std::optional<json> logs_result = std::nullopt;
                if(!_config.registry_address.empty())
                {
                    if(from_block > to_block)
                    {
                        logs_result = json::array();
                    }
                    else
                    {
                        const std::string connector_topic = chain::normalizeHex(evmc::hex(chain::constructEventTopic(
                            "ConnectorAdded(address,address,string,address,uint32,uint32[],string[],uint32[],uint32[],string[],string,int32[],bytes32)")));
                        const std::string transformation_topic = chain::normalizeHex(evmc::hex(chain::constructEventTopic(
                            "TransformationAdded(address,string,address,address,uint32)")));
                        const std::string condition_topic = chain::normalizeHex(evmc::hex(chain::constructEventTopic(
                            "ConditionAdded(address,string,address,address,uint32)")));

                        json filter{
                            {"address", chain::normalizeHex(_config.registry_address)},
                            {"fromBlock", chain::toHexQuantity(from_block)},
                            {"toBlock", chain::toHexQuantity(to_block)},
                            {"topics", json::array({
                                json::array({
                                    connector_topic,
                                    transformation_topic,
                                    condition_topic
                                })
                            })}
                        };

                        json rpc_logs_result = _rpcCall("eth_getLogs", json::array({std::move(filter)}));
                        if(rpc_logs_result.is_array())
                        {
                            logs_result = std::move(rpc_logs_result);
                        }
                    }
                }

                if(!logs_result.has_value())
                {
                    co_await _sleepFor(_config.poll_interval_ms);
                    continue;
                }

                std::vector<json> logs;
                logs.reserve(logs_result->size());
                for(const auto & value : *logs_result)
                {
                    logs.push_back(value);
                }

                std::ranges::sort(logs, [](const json & lhs, const json & rhs)
                {
                    const auto lhs_block = parse::parseHexQuantity(lhs.value("blockNumber", "0x0")).value_or(0);
                    const auto rhs_block = parse::parseHexQuantity(rhs.value("blockNumber", "0x0")).value_or(0);
                    if(lhs_block != rhs_block)
                    {
                        return lhs_block < rhs_block;
                    }
                    const auto lhs_tx = parse::parseHexQuantity(lhs.value("transactionIndex", "0x0")).value_or(0);
                    const auto rhs_tx = parse::parseHexQuantity(rhs.value("transactionIndex", "0x0")).value_or(0);
                    if(lhs_tx != rhs_tx)
                    {
                        return lhs_tx < rhs_tx;
                    }
                    const auto lhs_log = parse::parseHexQuantity(lhs.value("logIndex", "0x0")).value_or(0);
                    const auto rhs_log = parse::parseHexQuantity(rhs.value("logIndex", "0x0")).value_or(0);
                    return lhs_log < rhs_log;
                });

                const std::int64_t batch_seen_at = utils::nowMs();

                std::vector<RawChainLog> raw_logs;
                raw_logs.reserve(logs.size());

                std::set<std::int64_t> required_blocks;
                std::set<std::int64_t> log_blocks;

                const auto tracked_reorg_blocks = co_await _storeLoadReorgWindowBlocks(_config.chain_id, from_block, to_block);

                required_blocks.insert(tracked_reorg_blocks.begin(), tracked_reorg_blocks.end());

                bool registry_log_parse_failed = false;
                for(const auto & log_json : logs)
                {
                    const auto parsed = parse::parseRawLog(log_json, batch_seen_at, _config.chain_id);

                    if(!parsed.has_value())
                    {
                        registry_log_parse_failed = true;
                        spdlog::warn(
                            "Events ingestion: parseRawLog failed for log in range [{}..{}] from chain={} registry={}; "
                            "halting cursor advance until next poll. log_json={}",
                            from_block,
                            to_block,
                            _config.chain_id,
                            _config.registry_address,
                            log_json.dump());
                        break;
                    }
                    raw_logs.push_back(*parsed);
                    required_blocks.insert(parsed->block_number);
                    log_blocks.insert(parsed->block_number);
                }

                if(registry_log_parse_failed)
                {
                    co_await _sleepFor(_config.poll_interval_ms);
                    continue;
                }

                std::vector<ChainBlockInfo> block_infos;
                block_infos.reserve(required_blocks.size());

                bool missing_required_log_block_metadata = false;
                std::int64_t missing_required_block_number = 0;

                std::unordered_map<std::int64_t, ChainBlockInfo> block_info_map;
                block_info_map.reserve(required_blocks.size());

                for(const std::int64_t block_number : required_blocks)
                {
                    json block_params = json::array();
                    block_params.push_back(chain::toHexQuantity(block_number));
                    block_params.push_back(false);
                    json block_info_result = _rpcCall("eth_getBlockByNumber", std::move(block_params));
                    const auto block_info = _parseEthBlockInfoResult(
                        block_info_result,
                        block_number,
                        _config.chain_id);

                    if(!block_info.has_value())
                    {
                        if(log_blocks.find(block_number) != log_blocks.end())
                        {
                            missing_required_log_block_metadata = true;
                            missing_required_block_number = block_number;
                            break;
                        }
                        continue;
                    }
                    block_info_map.insert_or_assign(block_info->block_number, *block_info);
                    block_infos.push_back(*block_info);
                }

                if(missing_required_log_block_metadata)
                {
                    spdlog::warn(
                        "Missing block metadata for required log block {}; not advancing ingest cursor",
                        missing_required_block_number);
                    co_await _sleepFor(_config.poll_interval_ms);
                    continue;
                }

                bool missing_log_block_in_map = false;
                std::int64_t missing_in_map_block_number = 0;
                for(const std::int64_t log_block_number : log_blocks)
                {
                    if(block_info_map.find(log_block_number) == block_info_map.end())
                    {
                        missing_log_block_in_map = true;
                        missing_in_map_block_number = log_block_number;
                        break;
                    }
                }

                if(missing_log_block_in_map)
                {
                    spdlog::warn(
                        "Block metadata invariant violated: log block {} has no metadata after fetch; "
                        "not advancing ingest cursor",
                        missing_in_map_block_number);
                    co_await _sleepFor(_config.poll_interval_ms);
                    continue;
                }

                std::vector<DecodedEvent> decoded_events;
                decoded_events.reserve(raw_logs.size());
                for(RawChainLog & raw : raw_logs)
                {
                    const auto block_it = block_info_map.find(raw.block_number);
                    if(block_it != block_info_map.end())
                    {
                        raw.block_time = block_it->second.block_time;
                        raw.parent_hash = block_it->second.parent_hash;

                        if(raw.block_hash.empty())
                        {
                            raw.block_hash = block_it->second.block_hash;
                        }
                    }

                    const auto decoded = _decoder->decode(raw);
                    if(!decoded.has_value())
                    {
                        continue;
                    }
                    decoded_events.push_back(*decoded);
                }

                const std::int64_t next_block = (to_block == std::numeric_limits<std::int64_t>::max())
                        ? to_block
                        : to_block + 1;

                const bool ingest_ok = co_await _storeIngestBatch(
                    _config.chain_id,
                    std::move(raw_logs),
                    std::move(decoded_events),
                    std::move(block_infos),
                    next_block,
                    utils::nowMs());

                if(!ingest_ok)
                {
                    co_await _sleepFor(_config.poll_interval_ms);
                    continue;
                }

                if(!co_await _storeApplyFinality(_config.chain_id, heights, utils::nowMs(), _config.reorg_window_blocks))
                {
                    spdlog::error("Failed to apply finality, during ingestion loop");
                }

                next_from_block = next_block;

                should_backoff = false;
            }
            catch(const std::exception & e)
            {
                spdlog::warn("Events ingestion loop iteration failed: {}", e.what());

                should_backoff = true;
            }

            if(should_backoff)
            {
                co_await _sleepFor(_config.poll_interval_ms);
            }
        }

        spdlog::info("Events ingestion loop stopped");
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
