#include <spdlog/spdlog.h>

#include "events.hpp"
#include "async.hpp"
#include "utils.hpp"
#include "parser.hpp"
#include "chain.hpp"
#include "evm.hpp"

namespace dcn::events
{
    class EventRuntime::Impl
    {
        public:
            Impl(asio::io_context & io_context, EventRuntimeConfig cfg)
                : _io_context(io_context)
                , _config(std::move(cfg))
                , _write_strand(asio::make_strand(io_context))
                , _store(std::make_unique<SQLiteHotStore>(_config.hot_db_path, _config.archive_root, _config.outbox_retention_ms, _config.chain_id))
                , _decoder(std::make_unique<PTEventDecoder>())
            {
            }

            void start()
            {
                if(_running.exchange(true, std::memory_order_acq_rel))
                {
                    return;
                }

                asio::co_spawn(
                    _io_context,
                    _runProjectorLoop(),
                    [](std::exception_ptr e)
                    {
                        utils::logException(e, "events projector loop failed");
                    });

                asio::co_spawn(
                    _io_context,
                    _runArchiveLoop(),
                    [](std::exception_ptr e)
                    {
                        utils::logException(e, "events archive loop failed");
                    });

                if(_config.ingestion_enabled)
                {
                    asio::co_spawn(
                        _io_context,
                        _runIngestionLoop(),
                        [](std::exception_ptr e)
                        {
                            utils::logException(e, "events ingestion loop failed");
                        });
                }
            }

            void requestStop()
            {
                _stop_requested.store(true, std::memory_order_release);
                _running.store(false, std::memory_order_release);
            }

            bool running() const
            {
                return _running.load(std::memory_order_acquire);
            }

            bool ingestionEnabled() const
            {
                return _config.ingestion_enabled;
            }

            FeedPage getFeedPage(const FeedQuery & query) const
            {
                return _store->getFeedPage(query);
            }

            StreamPage getStreamPage(const StreamQuery & query) const
            {
                return _store->getStreamPage(query);
            }

            std::int64_t minAvailableStreamSeq() const
            {
                return _store->minAvailableStreamSeq();
            }

        private:
            asio::awaitable<void> _sleepFor(const std::uint64_t ms) const
            {
                asio::steady_timer timer(co_await asio::this_coro::executor);
                timer.expires_after(std::chrono::milliseconds(ms));
                std::error_code ec;
                co_await timer.async_wait(asio::redirect_error(asio::use_awaitable, ec));
                co_return;
            }

            std::optional<json> _rpcResult(const std::string & method, json params) const
            {
                if(_config.rpc_url.empty())
                {
                    return std::nullopt;
                }

                const json request{
                    {"jsonrpc", "2.0"},
                    {"id", 1},
                    {"method", method},
                    {"params", std::move(params)}
                };

                std::vector<std::string> args{
                    "-sS",
                    "-X", "POST",
                    _config.rpc_url,
                    "-H", "Content-Type: application/json",
                    "--data", request.dump()
                };

                const auto [exit_code, output] = native::runProcess("curl", std::move(args));
                if(exit_code != 0)
                {
                    spdlog::warn("Events RPC call '{}' failed with exit={}: {}", method, exit_code, output);
                    return std::nullopt;
                }

                const json response = json::parse(output, nullptr, false);
                if(response.is_discarded() || !response.is_object())
                {
                    spdlog::warn("Events RPC call '{}' returned malformed JSON", method);
                    return std::nullopt;
                }
                if(response.contains("error"))
                {
                    spdlog::warn("Events RPC call '{}' returned error: {}", method, response["error"].dump());
                    return std::nullopt;
                }
                if(!response.contains("result"))
                {
                    spdlog::warn("Events RPC call '{}' returned response without result", method);
                    return std::nullopt;
                }
                return response["result"];
            }

            parse::Result<std::int64_t> _ethBlockNumber() const
            {
                const auto result = _rpcResult("eth_blockNumber", json::array());
                if(!result || !result->is_string())
                {
                    return std::unexpected(parse::ParseError{parse::ParseError::Kind::INVALID_VALUE});
                }
                return parse::parseHexQuantity(result->get<std::string>());
            }

            parse::Result<std::int64_t> _ethTaggedBlockNumber(const std::string & tag) const
            {
                const auto result = _rpcResult("eth_getBlockByNumber", json::array({tag, false}));
                if(!result || !result->is_object())
                {
                    return std::unexpected(parse::ParseError{parse::ParseError::Kind::INVALID_VALUE});
                }
                if(!result->contains("number") || !(*result)["number"].is_string())
                {
                    return std::unexpected(parse::ParseError{parse::ParseError::Kind::INVALID_VALUE});
                }
                return parse::parseHexQuantity((*result)["number"].get<std::string>());
            }

            std::optional<ChainBlockInfo> _ethGetBlockInfo(const std::int64_t block_number) const
            {
                const auto result = _rpcResult("eth_getBlockByNumber", json::array({chain::toHexQuantity(block_number), false}));
                if(!result || !result->is_object())
                {
                    return std::nullopt;
                }

                const parse::Result<std::int64_t> number =
                    result->contains("number") && (*result)["number"].is_string()
                    ? parse::parseHexQuantity((*result)["number"].get<std::string>())
                    : std::unexpected(parse::ParseError{parse::ParseError::Kind::INVALID_VALUE});
                
                const parse::Result<std::int64_t> timestamp =
                    result->contains("timestamp") && (*result)["timestamp"].is_string()
                    ? parse::parseHexQuantity((*result)["timestamp"].get<std::string>())
                    : std::unexpected(parse::ParseError{parse::ParseError::Kind::INVALID_VALUE});
                
                if(!number.has_value() || !timestamp.has_value())
                {
                    return std::nullopt;
                }
                if(!result->contains("hash") || !(*result)["hash"].is_string())
                {
                    return std::nullopt;
                }
                if(!result->contains("parentHash") || !(*result)["parentHash"].is_string())
                {
                    return std::nullopt;
                }

                return ChainBlockInfo{
                    .chain_id = _config.chain_id,
                    .block_number = *number,
                    .block_hash = chain::normalizeHex((*result)["hash"].get<std::string>()),
                    .parent_hash = chain::normalizeHex((*result)["parentHash"].get<std::string>()),
                    .block_time = *timestamp,
                    .seen_at_ms = utils::nowMs()
                };
            }

            std::optional<json> _ethGetLogs(const std::int64_t from_block, const std::int64_t to_block) const
            {
                if(_config.registry_address.empty())
                {
                    return std::nullopt;
                }

                const std::string connector_topic = chain::normalizeHex(evmc::hex(
                    chain::constructEventTopic(
                        "ConnectorAdded(address,address,string,address,uint32,uint32[],string[],uint32[],uint32[],string[],string,int32[],bytes32)")));
                const std::string transformation_topic = chain::normalizeHex(evmc::hex(
                    chain::constructEventTopic("TransformationAdded(address,string,address,address,uint32)")));
                const std::string condition_topic = chain::normalizeHex(evmc::hex(
                    chain::constructEventTopic("ConditionAdded(address,string,address,address,uint32)")));

                json filter{
                    {"address", chain::normalizeHex(_config.registry_address)},
                    {"fromBlock", chain::toHexQuantity(from_block)},
                    {"toBlock", chain::toHexQuantity(to_block)},
                    {"topics", json::array({json::array({connector_topic, transformation_topic, condition_topic})})}
                };
                const auto result = _rpcResult("eth_getLogs", json::array({std::move(filter)}));
                if(!result || !result->is_array())
                {
                    return std::nullopt;
                }
                return *result;
            }

            std::optional<RawChainLog> _parseRawLog(const json & log_json, const std::int64_t seen_at_ms) const
            {
                if(!log_json.is_object())
                {
                    return std::nullopt;
                }

                if(!log_json.contains("blockNumber") || !log_json["blockNumber"].is_string() ||
                   !log_json.contains("blockHash") || !log_json["blockHash"].is_string() ||
                   !log_json.contains("transactionHash") || !log_json["transactionHash"].is_string() ||
                   !log_json.contains("transactionIndex") || !log_json["transactionIndex"].is_string() ||
                   !log_json.contains("logIndex") || !log_json["logIndex"].is_string() ||
                   !log_json.contains("address") || !log_json["address"].is_string() ||
                   !log_json.contains("data") || !log_json["data"].is_string() ||
                   !log_json.contains("topics") || !log_json["topics"].is_array())
                {
                    return std::nullopt;
                }

                const parse::Result<std::int64_t> block_number = parse::parseHexQuantity(log_json["blockNumber"].get<std::string>());
                const parse::Result<std::int64_t> tx_index = parse::parseHexQuantity(log_json["transactionIndex"].get<std::string>());
                const parse::Result<std::int64_t> log_index = parse::parseHexQuantity(log_json["logIndex"].get<std::string>());

                if(!block_number.has_value() || !tx_index.has_value() || !log_index.has_value())
                {
                    return std::nullopt;
                }

                RawChainLog log{
                    .chain_id = _config.chain_id,
                    .block_number = *block_number,
                    .block_hash = chain::normalizeHex(log_json["blockHash"].get<std::string>()),
                    .parent_hash = {},
                    .tx_index = *tx_index,
                    .tx_hash = chain::normalizeHex(log_json["transactionHash"].get<std::string>()),
                    .log_index = *log_index,
                    .address = chain::normalizeHex(log_json["address"].get<std::string>()),
                    .topics = {},
                    .data_hex = chain::normalizeHex(log_json["data"].get<std::string>()),
                    .removed = log_json.value("removed", false),
                    .block_time = std::nullopt,
                    .seen_at_ms = seen_at_ms
                };

                const auto & topics = log_json["topics"];
                for(std::size_t i = 0; i < std::min<std::size_t>(topics.size(), log.topics.size()); ++i)
                {
                    if(!topics.at(i).is_string())
                    {
                        continue;
                    }
                    log.topics[i] = chain::normalizeHex(topics.at(i).get<std::string>());
                }
                return log;
            }

            FinalityHeights _resolveFinality(const std::int64_t head) const
            {
                FinalityHeights heights{
                    .head = head,
                    .safe = std::max<std::int64_t>(0, head - static_cast<std::int64_t>(_config.confirmations)),
                    .finalized = std::max<std::int64_t>(0, head - static_cast<std::int64_t>(_config.confirmations))
                };

                const auto safe = _ethTaggedBlockNumber("safe");
                const auto finalized = _ethTaggedBlockNumber("finalized");

                if(safe.has_value() && *safe >= 0 && *safe <= head)
                {
                    heights.safe = *safe;
                }
                if(finalized.has_value() && *finalized >= 0 && *finalized <= heights.safe)
                {
                    heights.finalized = *finalized;
                }
                return heights;
            }

            asio::awaitable<void> _runLocalIngestionLoop()
            {
                if(_config.local_evm == nullptr)
                {
                    co_return;
                }

                spdlog::info("Events ingestion loop (local EVM source) started for chain={}", _config.chain_id);

                std::uint64_t next_seq = 0;
                co_await async::ensureOnStrand(_write_strand);
                if(const auto persisted = _store->loadNextLocalSeq(_config.chain_id); persisted.has_value())
                {
                    next_seq = *persisted;
                }

                const std::size_t poll_limit = std::max<std::size_t>(1, std::min<std::size_t>(MAX_STREAM_LIMIT, _config.block_batch_size * 8ULL));
                constexpr const char* EPHEMERAL_ENTITY_ADDRESS = "0x0";

                while(!_stop_requested.load(std::memory_order_acquire))
                {
                    const std::vector<evm::EVM::EmittedLogRecord> emitted_logs = co_await _config.local_evm->getLogsSince(next_seq, poll_limit);
                    const std::int64_t head_block = co_await _config.local_evm->getHeadBlockNumber();

                    if(emitted_logs.empty())
                    {
                        co_await async::ensureOnStrand(_write_strand);
                        const FinalityHeights heights{
                            .head = std::max<std::int64_t>(head_block, 0),
                            .safe = std::max<std::int64_t>(head_block, 0),
                            .finalized = std::max<std::int64_t>(head_block, 0)
                        };
                        (void)_store->applyFinality(_config.chain_id, heights, utils::nowMs(), _config.reorg_window_blocks);
                        co_await _sleepFor(_config.poll_interval_ms);
                        continue;
                    }

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

                    co_await async::ensureOnStrand(_write_strand);
                    const bool ingest_ok = _store->ingestBatch(
                        _config.chain_id,
                        decoded_events,
                        block_infos,
                        next_block,
                        utils::nowMs());
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
                    (void)_store->applyFinality(_config.chain_id, heights, utils::nowMs(), _config.reorg_window_blocks);
                    (void)_store->saveNextLocalSeq(_config.chain_id, last_seq + 1, utils::nowMs());
                    next_seq = last_seq + 1;

                    if(emitted_logs.size() < poll_limit)
                    {
                        co_await _sleepFor(_config.poll_interval_ms);
                    }
                }

                spdlog::info("Events ingestion loop (local EVM source) stopped");
                co_return;
            }

            asio::awaitable<void> _runIngestionLoop()
            {
                if(_config.use_local_evm_source && _config.local_evm != nullptr)
                {
                    co_return co_await _runLocalIngestionLoop();
                }

                std::optional<std::int64_t> next_from_block = std::nullopt;
                spdlog::info("Events ingestion loop started for chain={} registry={}", _config.chain_id, _config.registry_address);

                while(!_stop_requested.load(std::memory_order_acquire))
                {
                    const auto head_opt = _ethBlockNumber();
                    if(!head_opt.has_value())
                    {
                        co_await _sleepFor(_config.poll_interval_ms);
                        continue;
                    }
                    const std::int64_t head = *head_opt;
                    const FinalityHeights heights = _resolveFinality(head);

                    co_await async::ensureOnStrand(_write_strand);
                    (void)_store->applyFinality(_config.chain_id, heights, utils::nowMs(), _config.reorg_window_blocks);

                    if(!next_from_block.has_value())
                    {
                        const auto persisted_next = _store->loadNextFromBlock(_config.chain_id);
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

                    if(*next_from_block > head)
                    {
                        co_await _sleepFor(_config.poll_interval_ms);
                        continue;
                    }

                    const std::int64_t max_to_block = (std::numeric_limits<std::int64_t>::max() - static_cast<std::int64_t>(_config.block_batch_size))
                        > *next_from_block
                        ? (*next_from_block + static_cast<std::int64_t>(_config.block_batch_size) - 1)
                        : std::numeric_limits<std::int64_t>::max();
                    const std::int64_t to_block = std::min<std::int64_t>(head, max_to_block);

                    const std::int64_t lookback_blocks = std::min<std::int64_t>(
                        static_cast<std::int64_t>(_config.reorg_window_blocks),
                        static_cast<std::int64_t>(_config.block_batch_size));
                    const std::int64_t from_block = (*next_from_block > lookback_blocks)
                        ? (*next_from_block - lookback_blocks)
                        : 0;

                    const auto logs_result = _ethGetLogs(from_block, to_block);
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
                    const auto tracked_reorg_blocks = _store->loadReorgWindowBlocks(_config.chain_id, from_block, to_block);
                    required_blocks.insert(tracked_reorg_blocks.begin(), tracked_reorg_blocks.end());
                    for(const auto & log_json : logs)
                    {
                        const auto parsed = _parseRawLog(log_json, batch_seen_at);
                        if(!parsed.has_value())
                        {
                            continue;
                        }
                        raw_logs.push_back(*parsed);
                        required_blocks.insert(parsed->block_number);
                    }

                    std::vector<ChainBlockInfo> block_infos;
                    block_infos.reserve(required_blocks.size());
                    for(const std::int64_t block_number : required_blocks)
                    {
                        const auto block_info = _ethGetBlockInfo(block_number);
                        if(!block_info.has_value())
                        {
                            continue;
                        }
                        block_infos.push_back(*block_info);
                    }

                    std::unordered_map<std::int64_t, ChainBlockInfo> block_info_map;
                    block_info_map.reserve(block_infos.size());
                    for(const ChainBlockInfo & block_info : block_infos)
                    {
                        block_info_map.insert_or_assign(block_info.block_number, block_info);
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

                    co_await async::ensureOnStrand(_write_strand);
                    const bool ingest_ok = _store->ingestBatch(_config.chain_id, decoded_events, block_infos, next_block, utils::nowMs());
                    if(!ingest_ok)
                    {
                        co_await _sleepFor(_config.poll_interval_ms);
                        continue;
                    }
                    next_from_block = next_block;
                }

                spdlog::info("Events ingestion loop stopped");
                co_return;
            }

            asio::awaitable<void> _runProjectorLoop()
            {
                spdlog::info("Events projector loop started");
                while(!_stop_requested.load(std::memory_order_acquire))
                {
                    co_await async::ensureOnStrand(_write_strand);
                    const std::size_t projected = _store->projectBatch(DEFAULT_PROJECT_BATCH_SIZE, utils::nowMs());
                    if(projected == 0)
                    {
                        co_await _sleepFor(_config.projector_interval_ms);
                    }
                }
                spdlog::info("Events projector loop stopped");
                co_return;
            }

            asio::awaitable<void> _runArchiveLoop()
            {
                spdlog::info("Events archive loop started");
                while(!_stop_requested.load(std::memory_order_acquire))
                {
                    co_await _sleepFor(_config.archive_interval_ms);
                    if(_stop_requested.load(std::memory_order_acquire))
                    {
                        break;
                    }

                    co_await async::ensureOnStrand(_write_strand);
                    (void)_store->runArchiveCycle(_config.chain_id, _config.hot_window_days, utils::nowMs());
                }
                spdlog::info("Events archive loop stopped");
                co_return;
            }

        private:
            asio::io_context & _io_context;
            EventRuntimeConfig _config;
            asio::strand<asio::io_context::executor_type> _write_strand;
            std::unique_ptr<SQLiteHotStore> _store;
            std::unique_ptr<IEventDecoder> _decoder;
            std::atomic<bool> _stop_requested{false};
            std::atomic<bool> _running{false};
    };

    EventRuntime::EventRuntime(asio::io_context & io_context, EventRuntimeConfig config)
        : _impl(std::make_unique<Impl>(io_context, std::move(config)))
    {
    }

    EventRuntime::~EventRuntime() = default;

    void EventRuntime::start()
    {
        _impl->start();
    }

    void EventRuntime::requestStop()
    {
        _impl->requestStop();
    }

    bool EventRuntime::running() const
    {
        return _impl->running();
    }

    bool EventRuntime::ingestionEnabled() const
    {
        return _impl->ingestionEnabled();
    }

    FeedPage EventRuntime::getFeedPage(const FeedQuery & query) const
    {
        return _impl->getFeedPage(query);
    }

    StreamPage EventRuntime::getStreamPage(const StreamQuery & query) const
    {
        return _impl->getStreamPage(query);
    }

    std::int64_t EventRuntime::minAvailableStreamSeq() const
    {
        return _impl->minAvailableStreamSeq();
    }  
}
