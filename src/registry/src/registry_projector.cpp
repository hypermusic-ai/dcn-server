#include <optional>
#include <string>
#include <vector>

#include <spdlog/spdlog.h>

#include "registry_projector.hpp"

#include "connector.hpp"
#include "transformation.hpp"
#include "condition.hpp"
#include "address.hpp"
#include "format_hash.hpp"
#include "hex.hpp"
#include "async.hpp"

namespace dcn::registry
{
    RegistryProjector::RegistryProjector(
        events::SQLiteHotStore & store,
        Registry & registry,
        const asio::strand<asio::io_context::executor_type> & write_strand)
        : _store(store)
        , _registry(registry)
        , _write_strand(write_strand)
    {
        // Load the persisted cursor so we resume from the right position after restart.
        _cursor = _registry.getMaterializationCursor();
    }

    std::string_view RegistryProjector::id() const
    {
        return events::REGISTRY_PROJECTOR_ID;
    }

    asio::awaitable<std::size_t> RegistryProjector::projectBatch(std::size_t limit, std::int64_t now_ms)
    {
        (void)now_ms;

        // Serialize the hot-store read on the write strand, as FeedProjector does.
        co_await async::ensureOnStrand(_write_strand);
        const auto rows = _store.readChangesSince(_cursor, limit);
        std::size_t done = 0;
        std::int64_t last_advanced_seq = _cursor;

        for(const auto & row : rows)
        {
            if(row.state != events::FINALIZED_STATE)
            {
                // Only finalized rows are materialized. observed/safe rows re-surface with
                // a strictly higher change_seq once finalized, so advancing past them here
                // is safe. A "removed" row is a reorg rollback — and a reorg can only touch
                // blocks inside the reorg window, i.e. rows that were never finalized and
                // therefore never materialized. So a removal can never strand a stale
                // registry row, and skipping it is correct.
                last_advanced_seq = row.change_seq;
                ++done;
                continue;
            }

            const bool ok = co_await _materializeOne(row);
            if(!ok)
            {
                if(row.change_seq == _failing_seq)
                {
                    ++_failure_count;
                }
                else
                {
                    _failing_seq = row.change_seq;
                    _failure_count = 1;
                }

                if(_failure_count < MAX_MATERIALIZE_ATTEMPTS)
                {
                    // Leave the cursor before this row so it is retried next pass;
                    // transient failures (e.g. busy registry DB) heal across passes.
                    break;
                }

                // Poison row: skip it rather than wedge the pipeline. A permanently
                // parked cursor also freezes the prune watermark for every projector,
                // growing the hot store without bound. The error logs emitted by
                // _materializeOne carry the block/log identifiers as evidence.
                spdlog::error("[RegistryProjector] poison-skipping change_seq={} "
                    "(block={} log_index={} event_type='{}') after {} failed attempts",
                    row.change_seq, row.block_number, row.log_index, row.event_type,
                    _failure_count);
                _failing_seq = 0;
                _failure_count = 0;
            }

            last_advanced_seq = row.change_seq;
            ++done;
        }

        if(last_advanced_seq > _cursor)
        {
            // The cursor write hops onto the Registry's own strand (inside
            // setMaterializationCursor), not the hot-store write strand, so it serializes
            // with every other registry DB access.
            //
            // Only advance the in-memory cursor once the write is durable. The prune loop
            // uses this cursor as the deletion watermark for raw source rows; advancing it
            // past an unpersisted write would let those rows be pruned before the registry
            // cursor catches up, dropping events on the floor after a restart.
            if(co_await _registry.setMaterializationCursor(last_advanced_seq))
            {
                _cursor = last_advanced_seq;
            }
            else
            {
                spdlog::error("[RegistryProjector] Failed to persist cursor seq={} — retrying next pass",
                    last_advanced_seq);
            }
        }

        co_return done;
    }

    asio::awaitable<bool> RegistryProjector::_materializeOne(const events::ChangeRecord & row)
    {
        // Collect non-null topics in order; stop at first nullopt.
        std::vector<std::string> topics;
        for(const auto & t : row.topics)
        {
            if(!t.has_value())
            {
                break;
            }
            topics.push_back(*t);
        }

        // ---- transformation_added ----
        if(row.event_type == events::TRANSFORMATION_ADDED_TYPE)
        {
            const auto event = pt::decodeTransformationAddedEvent(row.data_hex, topics);
            if(!event)
            {
                spdlog::error("[RegistryProjector] Undecodable transformation_added at block={} log_index={} — "
                    "will retry, then poison-skip",
                    row.block_number, row.log_index);
                co_return false;
            }

            if(const auto existing = co_await _registry.getTransformationRecordHandle(event->name))
            {
                const TransformationRecord & rec = **existing;
                if(rec.transformation().args_count() != event->args_count
                    || chain::normalizeHex(rec.owner()) != chain::addressToHex(event->owner))
                {
                    // A pre-existing row diverges from canonical chain data. There is no
                    // update/delete path on the registry, so we cannot reconcile here;
                    // the error log records the divergence before the row is skipped.
                    spdlog::error("[RegistryProjector] transformation '{}' diverges from chain "
                        "(existing owner={} args={}, chain owner={} args={}) — will retry, then poison-skip",
                        event->name, rec.owner(), rec.transformation().args_count(),
                        chain::addressToHex(event->owner), event->args_count);
                    co_return false;
                }
                co_return true; // already present and matches — idempotent
            }

            TransformationRecord record;
            record.mutable_transformation()->set_name(event->name);
            record.mutable_transformation()->set_args_count(event->args_count);
            record.set_owner(chain::addressToHex(event->owner));

            co_return co_await _registry.addTransformation(event->transformation_address, std::move(record));
        }

        // ---- condition_added ----
        if(row.event_type == events::CONDITION_ADDED_TYPE)
        {
            const auto event = pt::decodeConditionAddedEvent(row.data_hex, topics);
            if(!event)
            {
                spdlog::error("[RegistryProjector] Undecodable condition_added at block={} log_index={} — "
                    "will retry, then poison-skip",
                    row.block_number, row.log_index);
                co_return false;
            }

            if(const auto existing = co_await _registry.getConditionRecordHandle(event->name))
            {
                const ConditionRecord & rec = **existing;
                if(rec.condition().args_count() != event->args_count
                    || chain::normalizeHex(rec.owner()) != chain::addressToHex(event->owner))
                {
                    spdlog::error("[RegistryProjector] condition '{}' diverges from chain "
                        "(existing owner={} args={}, chain owner={} args={}) — will retry, then poison-skip",
                        event->name, rec.owner(), rec.condition().args_count(),
                        chain::addressToHex(event->owner), event->args_count);
                    co_return false;
                }
                co_return true;
            }

            ConditionRecord record;
            record.mutable_condition()->set_name(event->name);
            record.mutable_condition()->set_args_count(event->args_count);
            record.set_owner(chain::addressToHex(event->owner));

            co_return co_await _registry.addCondition(event->condition_address, std::move(record));
        }

        // ---- connector_added ----
        if(row.event_type == events::CONNECTOR_ADDED_TYPE)
        {
            const auto event = pt::decodeConnectorAddedEvent(row.data_hex, topics);
            if(!event)
            {
                spdlog::error("[RegistryProjector] Undecodable connector_added at block={} log_index={} — "
                    "will retry, then poison-skip",
                    row.block_number, row.log_index);
                co_return false;
            }

            if(const auto existing = co_await _registry.getConnectorRecordHandle(event->name))
            {
                const ConnectorRecord & rec = **existing;
                if(chain::normalizeHex(rec.owner()) != chain::addressToHex(event->owner))
                {
                    spdlog::error("[RegistryProjector] connector '{}' diverges from chain "
                        "(existing owner={}, chain owner={}) — will retry, then poison-skip",
                        event->name, rec.owner(), chain::addressToHex(event->owner));
                    co_return false;
                }
                co_return true;
            }

            // Pass the chain-emitted format hash so the registry refuses to store a locally-recomputed
            // hash that disagrees with the chain (keeps POST response/event and registry GET/feed in sync).
            // The PT contract emits a keccak-derived (never all-zero) hash; an all-zero word means the
            // log carried none, so let the registry fall back to its locally computed hash.
            std::optional<evmc::bytes32> expected_format_hash;
            if(!chain::equalBytes32(event->format_hash, evmc::bytes32{}))
            {
                expected_format_hash = event->format_hash;
            }

            ConnectorRecord record = pt::buildConnectorRecordFromEvent(*event);
            co_return co_await _registry.addConnector(event->connector_address, std::move(record), expected_format_hash);
        }

        // Unknown event type — cannot be materialized; the error log records the row
        // before projectBatch's retry budget poison-skips it.
        spdlog::error("[RegistryProjector] Unknown event_type='{}' at block={} log_index={} — will retry, then poison-skip",
            row.event_type, row.block_number, row.log_index);
        co_return false;
    }
}
