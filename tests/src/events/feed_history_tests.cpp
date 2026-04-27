#include "unit-tests.hpp"

#include "events_sql_assertions.hpp"
#include "events_test_harness.hpp"

using namespace dcn;
using namespace dcn::tests;
using namespace dcn::tests::events_harness;

namespace
{
    void ingestProjectFinalize(
        asio::io_context & store_io_context,
        events::SQLiteHotStore & store,
        const events::DecodedEvent & event,
        const std::int64_t now_ms)
    {
        const events::ChainBlockInfo block = makeBlockInfo(
            event.raw.block_number,
            event.raw.block_hash,
            hexBytes(0x70, 32),
            event.raw.block_time.value_or(0),
            now_ms - 30);
        ASSERT_TRUE(awaitIngestBatch(
            store_io_context,
            store,
            CHAIN_ID,
            {event},
            {block},
            event.raw.block_number + 1,
            now_ms - 20));
        EXPECT_EQ(projectAll(store_io_context, store, now_ms - 10), 1u);

        const events::FinalityHeights heights{
            .head = event.raw.block_number + 100,
            .safe = event.raw.block_number,
            .finalized = event.raw.block_number
        };
        ASSERT_TRUE(awaitApplyFinality(store_io_context, store, CHAIN_ID, heights, now_ms, 2048));
        EXPECT_EQ(projectAll(store_io_context, store, now_ms + 10), 1u);
    }
}

TEST_F(UnitTest, Events_History_HotFeedRead_PrefersHotRows)
{
    const auto paths = makeTempEventsPaths("history_hot_first");
    asio::io_context store_io_context;
    events::SQLiteHotStore store(paths.hot_db, paths.archive_root, 60 * 60 * 1000, CHAIN_ID);

    const events::DecodedEvent first = makeDecodedEvent(90, 0, 1, 0xE0, 0x20, events::EventType::CONNECTOR_ADDED, events::EventState::OBSERVED, 1'700'000'800);
    const events::DecodedEvent second = makeDecodedEvent(91, 0, 1, 0xE1, 0x21, events::EventType::TRANSFORMATION_ADDED, events::EventState::OBSERVED, 1'700'000'801);
    ASSERT_TRUE(awaitIngestBatch(
        store_io_context,
        store,
        CHAIN_ID,
        {first, second},
        {
            makeBlockInfo(90, first.raw.block_hash, hexBytes(0x6F, 32), 1'700'000'800, 1'700'000'800'100),
            makeBlockInfo(91, second.raw.block_hash, hexBytes(0x70, 32), 1'700'000'801, 1'700'000'801'100)
        },
        92,
        1'700'000'801'200));
    EXPECT_EQ(projectAll(store_io_context, store, 1'700'000'801'300), 2u);

    const events::FeedPage page = store.getFeedPage(events::FeedQuery{
        .limit = 10,
        .include_unfinalized = true
    });
    ASSERT_EQ(page.items.size(), 2u);
    EXPECT_EQ(page.items.at(0).block_number, 91);
    EXPECT_EQ(page.items.at(1).block_number, 90);
}

TEST_F(UnitTest, Events_History_HotFeedRead_FiltersByDefaultChainId)
{
    const auto paths = makeTempEventsPaths("history_chain_filter");
    asio::io_context store_io_context;
    events::SQLiteHotStore store(paths.hot_db, paths.archive_root, 60 * 60 * 1000, CHAIN_ID);

    const events::DecodedEvent chain_one_event = makeDecodedEvent(
        100,
        0,
        1,
        0xEE,
        0x2E,
        events::EventType::CONNECTOR_ADDED,
        events::EventState::OBSERVED,
        1'700'100'000);
    events::ChainBlockInfo chain_one_block = makeBlockInfo(
        100,
        chain_one_event.raw.block_hash,
        hexBytes(0x6D, 32),
        1'700'100'000,
        1'700'100'000'100);

    const events::DecodedEvent chain_two_event = makeDecodedEvent(
        200,
        0,
        1,
        0xEF,
        0x2F,
        events::EventType::TRANSFORMATION_ADDED,
        events::EventState::OBSERVED,
        1'700'200'000);
    events::ChainBlockInfo chain_two_block = makeBlockInfo(
        200,
        chain_two_event.raw.block_hash,
        hexBytes(0x6C, 32),
        1'700'200'000,
        1'700'200'000'100);
    chain_two_block.chain_id = 2;

    ASSERT_TRUE(awaitIngestBatch(
        store_io_context,
        store,
        CHAIN_ID,
        {chain_one_event},
        {chain_one_block},
        101,
        1'700'100'000'200));
    ASSERT_TRUE(awaitIngestBatch(
        store_io_context,
        store,
        2,
        {chain_two_event},
        {chain_two_block},
        201,
        1'700'200'000'200));

    EXPECT_EQ(projectAll(store_io_context, store, 1'700'200'000'300), 2u);

    const events::FeedPage page = store.getFeedPage(events::FeedQuery{
        .limit = 10,
        .include_unfinalized = true
    });
    ASSERT_EQ(page.items.size(), 1u);
    EXPECT_EQ(page.items.front().block_number, 100);
    EXPECT_EQ(page.items.front().feed_id.rfind("eth:1:", 0), 0u);

    const events::FeedPage cross_chain_cursor_page = store.getFeedPage(events::FeedQuery{
        .limit = 10,
        .before_cursor = std::string("c999:0:0:eth:2:deadbeef:1"),
        .include_unfinalized = true
    });
    EXPECT_TRUE(cross_chain_cursor_page.items.empty());
}

TEST_F(UnitTest, Events_History_DedupesAcrossHotAndArchive_ByFeedId)
{
    const auto paths = makeTempEventsPaths("history_dedupe_hot_archive");
    asio::io_context store_io_context;
    events::SQLiteHotStore store(paths.hot_db, paths.archive_root, 60 * 60 * 1000, CHAIN_ID);

    const events::DecodedEvent finalized = makeDecodedEvent(92, 0, 1, 0xE2, 0x22, events::EventType::CONDITION_ADDED, events::EventState::OBSERVED, 1'700'000'802);
    ingestProjectFinalize(store_io_context, store, finalized, 1'700'000'802'200);

    ASSERT_TRUE(awaitRunArchiveCycle(store_io_context, store, CHAIN_ID, 36500, 1'700'000'900'000));

    events_sql::expectRowCount(paths.hot_db, "feed_items_hot", 1);
    {
        SqliteReadonly db(paths.hot_db);
        EXPECT_EQ(db.scalarInt64("SELECT COUNT(1) FROM shard_catalog WHERE state='READY';"), 1);
    }

    const events::FeedPage page = store.getFeedPage(events::FeedQuery{
        .limit = 10,
        .include_unfinalized = false
    });
    ASSERT_EQ(page.items.size(), 1u);
    EXPECT_EQ(page.items.front().status, "finalized");
}

TEST_F(UnitTest, Events_History_OrderingAcrossHotArchiveBoundary_IsCanonical)
{
    const auto paths = makeTempEventsPaths("history_boundary_order");
    asio::io_context store_io_context;
    events::SQLiteHotStore store(paths.hot_db, paths.archive_root, 60 * 60 * 1000, CHAIN_ID);

    const events::DecodedEvent older = makeDecodedEvent(93, 0, 1, 0xE3, 0x23, events::EventType::CONNECTOR_ADDED, events::EventState::OBSERVED, 1'700'000'803);
    ingestProjectFinalize(store_io_context, store, older, 1'700'000'803'200);
    ASSERT_TRUE(awaitRunArchiveCycle(store_io_context, store, CHAIN_ID, 0, 1'900'000'000'000));

    events_sql::expectRowCount(paths.hot_db, "feed_items_hot", 0);

    const events::DecodedEvent newer = makeDecodedEvent(120, 0, 1, 0xE4, 0x24, events::EventType::TRANSFORMATION_ADDED, events::EventState::OBSERVED, 1'700'000'900);
    ASSERT_TRUE(awaitIngestBatch(
        store_io_context,
        store,
        CHAIN_ID,
        {newer},
        {makeBlockInfo(120, newer.raw.block_hash, hexBytes(0x6E, 32), 1'700'000'900, 1'700'000'900'100)},
        121,
        1'700'000'900'200));
    EXPECT_EQ(projectAll(store_io_context, store, 1'700'000'900'300), 1u);

    const events::FeedPage page = store.getFeedPage(events::FeedQuery{
        .limit = 10,
        .include_unfinalized = true
    });
    ASSERT_EQ(page.items.size(), 2u);
    EXPECT_EQ(page.items.at(0).block_number, 120);
    EXPECT_EQ(page.items.at(1).block_number, 93);
}

TEST_F(UnitTest, Events_History_OrderingUsesCreatedAtMsAndCursorPaginatesByIt)
{
    const auto paths = makeTempEventsPaths("history_updated_at_order");
    asio::io_context store_io_context;
    events::SQLiteHotStore store(paths.hot_db, paths.archive_root, 60 * 60 * 1000, CHAIN_ID);

    const events::DecodedEvent high_block_first = makeDecodedEvent(
        200,
        0,
        1,
        0xF0,
        0x30,
        events::EventType::CONNECTOR_ADDED,
        events::EventState::OBSERVED,
        1'700'300'000);
    ASSERT_TRUE(awaitIngestBatch(
        store_io_context,
        store,
        CHAIN_ID,
        {high_block_first},
        {makeBlockInfo(200, high_block_first.raw.block_hash, hexBytes(0x71, 32), 1'700'300'000, 1'700'300'000'100)},
        201,
        1'700'300'000'200));
    EXPECT_EQ(projectAll(store_io_context, store, 1'700'300'000'300), 1u);

    const events::DecodedEvent low_block_later = makeDecodedEvent(
        100,
        0,
        1,
        0xF1,
        0x31,
        events::EventType::TRANSFORMATION_ADDED,
        events::EventState::OBSERVED,
        1'700'300'100);
    ASSERT_TRUE(awaitIngestBatch(
        store_io_context,
        store,
        CHAIN_ID,
        {low_block_later},
        {makeBlockInfo(100, low_block_later.raw.block_hash, hexBytes(0x72, 32), 1'700'300'100, 1'700'300'100'100)},
        201,
        1'700'300'100'200));
    EXPECT_EQ(projectAll(store_io_context, store, 1'700'300'100'300), 1u);

    const events::FeedPage ordered_page = store.getFeedPage(events::FeedQuery{
        .limit = 10,
        .include_unfinalized = true
    });
    ASSERT_EQ(ordered_page.items.size(), 2u);
    EXPECT_EQ(ordered_page.items.at(0).block_number, 100);
    EXPECT_EQ(ordered_page.items.at(1).block_number, 200);
    EXPECT_GT(ordered_page.items.at(0).created_at_ms, ordered_page.items.at(1).created_at_ms);

    const parse::Result<events::CursorKey> parsed_cursor = parse::parseHistoryCursor(ordered_page.items.at(0).history_cursor);
    ASSERT_TRUE(parsed_cursor.has_value());
    EXPECT_EQ(parsed_cursor->chain_namespace, "eth");
    EXPECT_EQ(parsed_cursor->created_at_ms, ordered_page.items.at(0).created_at_ms);

    const events::FeedPage first_page = store.getFeedPage(events::FeedQuery{
        .limit = 1,
        .include_unfinalized = true
    });
    ASSERT_EQ(first_page.items.size(), 1u);
    ASSERT_TRUE(first_page.next_before_cursor.has_value());
    EXPECT_EQ(first_page.items.front().block_number, 100);

    const events::FeedPage second_page = store.getFeedPage(events::FeedQuery{
        .limit = 10,
        .before_cursor = first_page.next_before_cursor,
        .include_unfinalized = true
    });
    ASSERT_EQ(second_page.items.size(), 1u);
    EXPECT_EQ(second_page.items.front().block_number, 200);
}

TEST_F(UnitTest, Events_History_OrderingReadsArchiveEvenWhenHotExceedsLimit)
{
    const auto paths = makeTempEventsPaths("history_archive_scan_with_hot_limit");
    asio::io_context store_io_context;
    events::SQLiteHotStore store(paths.hot_db, paths.archive_root, 60 * 60 * 1000, CHAIN_ID);

    const std::int64_t archived_projected_at_ms = 1'700'400'000'200;
    const std::int64_t hot_projected_before_archive_ms = archived_projected_at_ms - 1000;

    const events::DecodedEvent archived_top = makeDecodedEvent(
        50,
        0,
        1,
        0xF2,
        0x32,
        events::EventType::CONDITION_ADDED,
        events::EventState::OBSERVED,
        1'700'400'000);
    ingestProjectFinalize(store_io_context, store, archived_top, archived_projected_at_ms);
    ASSERT_TRUE(awaitRunArchiveCycle(store_io_context, store, CHAIN_ID, 0, 1'900'000'000'000));

    const events::DecodedEvent hot_one = makeDecodedEvent(
        300,
        0,
        1,
        0xF3,
        0x33,
        events::EventType::CONNECTOR_ADDED,
        events::EventState::OBSERVED,
        1'700'500'000);
    const events::DecodedEvent hot_two = makeDecodedEvent(
        301,
        0,
        1,
        0xF4,
        0x34,
        events::EventType::TRANSFORMATION_ADDED,
        events::EventState::OBSERVED,
        1'700'500'001);
    ASSERT_TRUE(awaitIngestBatch(
        store_io_context,
        store,
        CHAIN_ID,
        {hot_one, hot_two},
        {
            makeBlockInfo(300, hot_one.raw.block_hash, hexBytes(0x73, 32), 1'700'500'000, 1'700'500'000'100),
            makeBlockInfo(301, hot_two.raw.block_hash, hexBytes(0x74, 32), 1'700'500'001, 1'700'500'001'100)
        },
        302,
        1'700'500'001'200));
    EXPECT_EQ(projectAll(store_io_context, store, hot_projected_before_archive_ms), 2u);

    const events::FeedPage page = store.getFeedPage(events::FeedQuery{
        .limit = 1,
        .include_unfinalized = true
    });
    ASSERT_EQ(page.items.size(), 1u);
    EXPECT_EQ(page.items.front().block_number, 50);
}
