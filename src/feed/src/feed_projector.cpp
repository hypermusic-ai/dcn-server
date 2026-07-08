#include <spdlog/spdlog.h>

#include "feed_projector.hpp"
#include "async.hpp"

namespace dcn::feed
{
    FeedProjector::FeedProjector(
        events::SQLiteHotStore & store,
        Feed & feed,
        const asio::strand<asio::io_context::executor_type> & write_strand)
        : _store(store)
        , _feed(feed)
        , _write_strand(write_strand)
        , _last_change_seq(feed.getFeedCursor())
    {
    }

    std::string_view FeedProjector::id() const
    {
        return events::FEED_PROJECTOR_ID;
    }

    asio::awaitable<std::size_t> FeedProjector::projectBatch(std::size_t limit, std::int64_t now_ms)
    {
        co_await async::ensureOnStrand(_write_strand);
        auto rows = _store.readChangesSince(_last_change_seq, limit);

        std::int64_t last_successful_seq = _last_change_seq;
        std::size_t processed_count = 0;
        for(const auto & row : rows)
        {
            try
            {
                co_await _feed.applyChange(row, now_ms);
                last_successful_seq = row.change_seq;
                ++processed_count;
            }
            catch(const std::exception & e)
            {
                spdlog::error("FeedProjector::projectBatch: applyChange failed at change_seq={}: {}",
                              row.change_seq, e.what());
                break;
            }
        }
        if(last_successful_seq > _last_change_seq)
        {
            co_await _feed.setFeedCursor(last_successful_seq);
            _last_change_seq = last_successful_seq;
        }
        co_await _feed.maintainOutbox(now_ms);
        co_return processed_count;
    }
}
