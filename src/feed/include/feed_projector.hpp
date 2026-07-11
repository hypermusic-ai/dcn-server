#pragma once

#include <cstddef>
#include <cstdint>
#include <string_view>

#include "native.h"
#include <asio.hpp>

#include "event_projector.hpp"
#include "sqlite_hot_store.hpp"
#include "feed.hpp"

namespace dcn::feed
{
    /**
     * @brief Materialises change-log rows from the events hot-store into the
     *        Feed database, using an independent change_seq cursor persisted in
     *        the feed DB.  Implements events::IEventProjector so it can be driven
     *        by the existing EventsRuntime projection loop.
     *
     * Cursor policy:
     *   - cursor is loaded from the Feed DB at construction (via Feed::getFeedCursor)
     *     and persisted after each successful batch (via Feed::setFeedCursor).
     *
     * Failure policy (per applyChange):
     *   - on failure the cursor is left before the row and the row is retried on
     *     subsequent projectBatch passes, so transient failures (e.g. busy feed DB)
     *     heal on their own. After ProjectorRetryConfig::max_attempts consecutive
     *     failing passes the row is dead-lettered (FEED_DEAD_LETTER_BIT in the hot
     *     store) and the cursor advances past it: a permanently parked cursor would
     *     freeze the prune watermark for every projector and grow the hot store
     *     without bound. Dead-lettered rows are exempt from pruning, so no chain
     *     event data is lost; a periodic idle-time sweep
     *     (ProjectorRetryConfig::sweep_interval_ms) retries them and clears the bit
     *     once applyChange succeeds. If the dead-letter mark itself cannot be
     *     written, the cursor stays parked.
     */
    class FeedProjector final : public events::IEventProjector
    {
        public:
            FeedProjector(events::SQLiteHotStore & store,
                          Feed & feed,
                          const asio::strand<asio::io_context::executor_type> & write_strand,
                          events::ProjectorRetryConfig retry = {});

            std::string_view id() const override;
            std::int64_t cursor() const override { return _last_change_seq; }
            asio::awaitable<std::size_t> projectBatch(std::size_t limit, std::int64_t now_ms) override;

        private:
            // Retries dead-lettered rows (throttled by _retry.sweep_interval_ms)
            // and clears their bit on success. Returns the number of rows resolved.
            asio::awaitable<std::size_t> _sweepDeadLetters(std::size_t limit, std::int64_t now_ms);

            events::SQLiteHotStore & _store;
            Feed & _feed;
            asio::strand<asio::io_context::executor_type> _write_strand;
            events::ProjectorRetryConfig _retry;
            std::int64_t _last_change_seq{0};

            // Consecutive-failure tracking for the row currently at the cursor head.
            std::int64_t _failing_seq{0};
            std::size_t _failure_count{0};

            std::int64_t _last_sweep_ms{0};
    };
}
