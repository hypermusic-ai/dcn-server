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
     * Error handling:
     *   - on applyChange failure the loop breaks; cursor is NOT advanced past
     *     the failed row so it will be retried on the next projectBatch call.
     */
    class FeedProjector final : public events::IEventProjector
    {
        public:
            FeedProjector(events::SQLiteHotStore & store,
                          Feed & feed,
                          const asio::strand<asio::io_context::executor_type> & write_strand);

            std::string_view id() const override;
            std::int64_t cursor() const override { return _last_change_seq; }
            asio::awaitable<std::size_t> projectBatch(std::size_t limit, std::int64_t now_ms) override;

        private:
            events::SQLiteHotStore & _store;
            Feed & _feed;
            asio::strand<asio::io_context::executor_type> _write_strand;
            std::int64_t _last_change_seq{0};
    };
}
