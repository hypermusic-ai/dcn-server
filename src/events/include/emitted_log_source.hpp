#pragma once

#include <cstddef>
#include <cstdint>
#include <vector>

#include <asio.hpp>

#include "evm.hpp"          // evm::EVM::EmittedLogRecord
#include "events_ingest.hpp" // FinalityHeights

namespace dcn::events
{
    // Result of a single poll against an attached log source: the new records,
    // the source's own finality view (EmittedLogRecord carries none), and the
    // cursor to resume from next poll.
    struct SourcePoll
    {
        std::vector<evm::EVM::EmittedLogRecord> records;
        FinalityHeights finality;
        std::uint64_t next_cursor = 0;
    };

    // An attachable source of EmittedLogRecord batches for event ingestion.
    // Each source owns exactly one chain_id. Ingestion drives the source (pull):
    // it repeatedly calls pollSince() with the persisted cursor. RPC providers,
    // the local EVM, archives, etc. all plug in behind this interface.
    class IEmittedLogSource
    {
        public:
            virtual ~IEmittedLogSource() = default;

            virtual int chainId() const = 0;

            // When true, ingestion rewrites decoded entity addresses to the
            // ephemeral sentinel for this source's events (local EVM policy).
            virtual bool ephemeralEntities() const { return false; }

            virtual asio::awaitable<SourcePoll> pollSince(std::uint64_t cursor, std::size_t limit) = 0;

            virtual asio::awaitable<void> stop() { co_return; }
    };
}
