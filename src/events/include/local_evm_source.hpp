#pragma once

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <utility>
#include <vector>

#include <asio.hpp>

#include "emitted_log_source.hpp"
#include "evm.hpp"

namespace dcn::events
{
    // Adapts the in-process EVM into an EmittedLogRecord source. Keeps the evm
    // module generic: the ephemeral-entity rewrite stays a policy flag applied
    // by ingestion, not baked into the EVM. All blocks are reported finalized
    // immediately (local EVM has no reorg/finality staging).
    class LocalEvmSource final : public IEmittedLogSource
    {
        public:
            LocalEvmSource(evm::EVM & evm, int chain_id)
                : _evm(evm)
                , _chain_id(chain_id)
            {
            }

            int chainId() const override { return _chain_id; }
            bool ephemeralEntities() const override { return true; }

            asio::awaitable<SourcePoll> pollSince(std::uint64_t cursor, std::size_t limit) override
            {
                std::vector<evm::EVM::EmittedLogRecord> records = co_await _evm.getLogsSince(cursor, limit);
                const std::int64_t head = co_await _evm.getHeadBlockNumber();

                std::int64_t max_block = std::max<std::int64_t>(head, 0);
                for(const auto & record : records)
                {
                    max_block = std::max<std::int64_t>(max_block, record.block_number);
                }

                const std::uint64_t next_cursor = records.empty() ? cursor : records.back().seq + 1;

                co_return SourcePoll{
                    .records = std::move(records),
                    .finality = FinalityHeights{
                        .head = max_block,
                        .safe = max_block,
                        .finalized = max_block
                    },
                    .next_cursor = next_cursor
                };
            }

        private:
            evm::EVM & _evm;
            int _chain_id;
    };
}
