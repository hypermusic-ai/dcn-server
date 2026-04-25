#include "unit-tests.hpp"

#include "events_test_harness.hpp"

#include <algorithm>

using namespace dcn;
using namespace dcn::tests;
using namespace dcn::tests::events_harness;

namespace
{
    void ingestAndProjectOne(
        events::SQLiteHotStore & store,
        const std::int64_t block_number,
        const std::uint8_t block_hash_byte,
        const std::uint8_t tx_hash_byte,
        const std::int64_t block_time,
        const std::int64_t now_ms)
    {
        const events::DecodedEvent event = makeDecodedEvent(
            block_number,
            0,
            1,
            block_hash_byte,
            tx_hash_byte,
            events::EventType::CONNECTOR_ADDED,
            events::EventState::OBSERVED,
            block_time,
            now_ms - 50);
        const events::ChainBlockInfo block = makeBlockInfo(
            block_number,
            event.raw.block_hash,
            hexBytes(static_cast<std::uint8_t>(block_hash_byte - 1), 32),
            block_time,
            now_ms - 40);
        ASSERT_TRUE(store.ingestBatch(CHAIN_ID, {event}, {block}, block_number + 1, now_ms - 30));
        EXPECT_EQ(projectAll(store, now_ms), 1u);
    }
}

TEST_F(UnitTest, Events_StreamPage_SinceSeqAndOrdering_AreMonotonic)
{
    const auto paths = makeTempEventsPaths("stream_monotonic");
    events::SQLiteHotStore store(paths.hot_db, paths.archive_root, 60 * 60 * 1000, CHAIN_ID);

    ingestAndProjectOne(store, 80, 0xD0, 0xF0, 1'700'000'700, 1'700'000'700'100);
    ingestAndProjectOne(store, 81, 0xD1, 0xF1, 1'700'000'701, 1'700'000'701'100);
    ingestAndProjectOne(store, 82, 0xD2, 0xF2, 1'700'000'702, 1'700'000'702'100);

    const events::StreamPage page = store.getStreamPage(events::StreamQuery{
        .since_seq = 1,
        .limit = 100
    });

    ASSERT_EQ(page.deltas.size(), 2u);
    EXPECT_EQ(page.deltas.at(0).stream_seq, 2);
    EXPECT_EQ(page.deltas.at(1).stream_seq, 3);
    ASSERT_TRUE(page.last_seq.has_value());
    EXPECT_EQ(*page.last_seq, 3);
    EXPECT_FALSE(page.has_more);
}

TEST_F(UnitTest, Events_StreamPage_ReconnectWithLastSeq_DoesNotDuplicate)
{
    const auto paths = makeTempEventsPaths("stream_reconnect");
    events::SQLiteHotStore store(paths.hot_db, paths.archive_root, 60 * 60 * 1000, CHAIN_ID);

    ingestAndProjectOne(store, 83, 0xD3, 0xF3, 1'700'000'703, 1'700'000'703'100);
    ingestAndProjectOne(store, 84, 0xD4, 0xF4, 1'700'000'704, 1'700'000'704'100);

    const events::StreamPage first = store.getStreamPage(events::StreamQuery{
        .since_seq = 0,
        .limit = 100
    });
    ASSERT_EQ(first.deltas.size(), 2u);
    ASSERT_TRUE(first.last_seq.has_value());

    const events::StreamPage second = store.getStreamPage(events::StreamQuery{
        .since_seq = *first.last_seq,
        .limit = 100
    });
    EXPECT_TRUE(second.deltas.empty());
    EXPECT_FALSE(second.last_seq.has_value());
}

TEST_F(UnitTest, Events_API_FeedStream_EmitsSseFramesWithFloorAndMeta)
{
    const auto paths = makeTempEventsPaths("stream_sse_contract");
    events::SQLiteHotStore writer(paths.hot_db, paths.archive_root, 10, CHAIN_ID);

    ingestAndProjectOne(writer, 85, 0xD5, 0xF5, 1'700'000'705, 1'700'000'705'100);
    ingestAndProjectOne(writer, 86, 0xD6, 0xF6, 1'700'000'706, 1'700'000'706'100);

    asio::io_context io_context;
    events::EventRuntime runtime(
        io_context,
        events::EventRuntimeConfig{
            .hot_db_path = paths.hot_db,
            .archive_root = paths.archive_root,
            .chain_id = CHAIN_ID
        });

    http::Request request;
    request.setMethod(http::Method::GET).setPath(http::URL("/feed/stream?since_seq=0&limit=10")).setVersion("HTTP/1.1");

    server::QueryArgsList query_args;
    query_args.emplace("since_seq", makeUintQueryArg(0));
    query_args.emplace("limit", makeUintQueryArg(10));

    const http::Response response = runAwaitable(io_context, GET_feedStream(request, {}, std::move(query_args), runtime));
    ASSERT_EQ(response.getCode(), http::Code::OK);

    const std::vector<SseFrame> frames = parseSseFrames(response.getBody());
    ASSERT_GE(frames.size(), 3u);
    EXPECT_TRUE(frames.front().is_comment);
    EXPECT_EQ(frames.front().comment, "min_available_seq=2");

    ASSERT_TRUE(frames.at(1).id.has_value());
    EXPECT_EQ(*frames.at(1).id, 2);
    EXPECT_EQ(frames.at(1).event, "feed_delta");
    const auto first_data = json::parse(frames.at(1).data, nullptr, false);
    ASSERT_FALSE(first_data.is_discarded());
    EXPECT_EQ(first_data.value("stream_seq", 0), 2);

    const SseFrame & meta_frame = frames.back();
    EXPECT_EQ(meta_frame.event, "stream_meta");
    const auto meta_data = json::parse(meta_frame.data, nullptr, false);
    ASSERT_FALSE(meta_data.is_discarded());
    EXPECT_EQ(meta_data.value("has_more", true), false);
    EXPECT_EQ(meta_data["last_seq"], 2);
}

TEST_F(UnitTest, Events_API_FeedStream_AfterRestart_UsesDurableOutboxState)
{
    const auto paths = makeTempEventsPaths("stream_restart");
    {
        events::SQLiteHotStore writer(paths.hot_db, paths.archive_root, 60 * 60 * 1000, CHAIN_ID);
        ingestAndProjectOne(writer, 87, 0xD7, 0xF7, 1'700'000'707, 1'700'000'707'100);
        ingestAndProjectOne(writer, 88, 0xD8, 0xF8, 1'700'000'708, 1'700'000'708'100);
    }

    asio::io_context io_context;
    events::EventRuntime runtime(
        io_context,
        events::EventRuntimeConfig{
            .hot_db_path = paths.hot_db,
            .archive_root = paths.archive_root,
            .chain_id = CHAIN_ID
        });

    http::Request request;
    request.setMethod(http::Method::GET).setPath(http::URL("/feed/stream?since_seq=1&limit=10")).setVersion("HTTP/1.1");

    server::QueryArgsList query_args;
    query_args.emplace("since_seq", makeUintQueryArg(1));
    query_args.emplace("limit", makeUintQueryArg(10));

    const http::Response response = runAwaitable(io_context, GET_feedStream(request, {}, std::move(query_args), runtime));
    ASSERT_EQ(response.getCode(), http::Code::OK);

    const std::vector<SseFrame> frames = parseSseFrames(response.getBody());
    ASSERT_GE(frames.size(), 2u);

    const auto delta_it = std::find_if(
        frames.begin(),
        frames.end(),
        [](const SseFrame & frame)
        {
            return frame.event == "feed_delta";
        });
    ASSERT_NE(delta_it, frames.end());
    ASSERT_TRUE(delta_it->id.has_value());
    EXPECT_EQ(*delta_it->id, 2);
}
