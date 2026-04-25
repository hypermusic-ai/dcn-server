#include "unit-tests.hpp"

#include "events_sql_assertions.hpp"
#include "events_test_harness.hpp"

using namespace dcn;
using namespace dcn::tests;
using namespace dcn::tests::events_harness;

namespace
{
    void ingestProjectFinalize(events::SQLiteHotStore & store, const events::DecodedEvent & event, const std::int64_t now_ms)
    {
        const events::ChainBlockInfo block = makeBlockInfo(
            event.raw.block_number,
            event.raw.block_hash,
            hexBytes(0x70, 32),
            event.raw.block_time.value_or(0),
            now_ms - 30);
        ASSERT_TRUE(store.ingestBatch(CHAIN_ID, {event}, {block}, event.raw.block_number + 1, now_ms - 20));
        EXPECT_EQ(projectAll(store, now_ms - 10), 1u);

        const events::FinalityHeights heights{
            .head = event.raw.block_number + 100,
            .safe = event.raw.block_number,
            .finalized = event.raw.block_number
        };
        ASSERT_TRUE(store.applyFinality(CHAIN_ID, heights, now_ms, 2048));
        EXPECT_EQ(projectAll(store, now_ms + 10), 1u);
    }
}

TEST_F(UnitTest, Events_History_HotFeedRead_PrefersHotRows)
{
    const auto paths = makeTempEventsPaths("history_hot_first");
    events::SQLiteHotStore store(paths.hot_db, paths.archive_root, 60 * 60 * 1000, CHAIN_ID);

    const events::DecodedEvent first = makeDecodedEvent(90, 0, 1, 0xE0, 0x20, events::EventType::CONNECTOR_ADDED, events::EventState::OBSERVED, 1'700'000'800);
    const events::DecodedEvent second = makeDecodedEvent(91, 0, 1, 0xE1, 0x21, events::EventType::TRANSFORMATION_ADDED, events::EventState::OBSERVED, 1'700'000'801);
    ASSERT_TRUE(store.ingestBatch(
        CHAIN_ID,
        {first, second},
        {
            makeBlockInfo(90, first.raw.block_hash, hexBytes(0x6F, 32), 1'700'000'800, 1'700'000'800'100),
            makeBlockInfo(91, second.raw.block_hash, hexBytes(0x70, 32), 1'700'000'801, 1'700'000'801'100)
        },
        92,
        1'700'000'801'200));
    EXPECT_EQ(projectAll(store, 1'700'000'801'300), 2u);

    const events::FeedPage page = store.getFeedPage(events::FeedQuery{
        .limit = 10,
        .include_unfinalized = true
    });
    ASSERT_EQ(page.items.size(), 2u);
    EXPECT_EQ(page.items.at(0).block_number, 91);
    EXPECT_EQ(page.items.at(1).block_number, 90);
}

TEST_F(UnitTest, Events_History_DedupesAcrossHotAndArchive_ByFeedId)
{
    const auto paths = makeTempEventsPaths("history_dedupe_hot_archive");
    events::SQLiteHotStore store(paths.hot_db, paths.archive_root, 60 * 60 * 1000, CHAIN_ID);

    const events::DecodedEvent finalized = makeDecodedEvent(92, 0, 1, 0xE2, 0x22, events::EventType::CONDITION_ADDED, events::EventState::OBSERVED, 1'700'000'802);
    ingestProjectFinalize(store, finalized, 1'700'000'802'200);

    ASSERT_TRUE(store.runArchiveCycle(CHAIN_ID, 36500, 1'700'000'900'000));

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
    events::SQLiteHotStore store(paths.hot_db, paths.archive_root, 60 * 60 * 1000, CHAIN_ID);

    const events::DecodedEvent older = makeDecodedEvent(93, 0, 1, 0xE3, 0x23, events::EventType::CONNECTOR_ADDED, events::EventState::OBSERVED, 1'700'000'803);
    ingestProjectFinalize(store, older, 1'700'000'803'200);
    ASSERT_TRUE(store.runArchiveCycle(CHAIN_ID, 0, 1'900'000'000'000));

    events_sql::expectRowCount(paths.hot_db, "feed_items_hot", 0);

    const events::DecodedEvent newer = makeDecodedEvent(120, 0, 1, 0xE4, 0x24, events::EventType::TRANSFORMATION_ADDED, events::EventState::OBSERVED, 1'700'000'900);
    ASSERT_TRUE(store.ingestBatch(
        CHAIN_ID,
        {newer},
        {makeBlockInfo(120, newer.raw.block_hash, hexBytes(0x6E, 32), 1'700'000'900, 1'700'000'900'100)},
        121,
        1'700'000'900'200));
    EXPECT_EQ(projectAll(store, 1'700'000'900'300), 1u);

    const events::FeedPage page = store.getFeedPage(events::FeedQuery{
        .limit = 10,
        .include_unfinalized = true
    });
    ASSERT_EQ(page.items.size(), 2u);
    EXPECT_EQ(page.items.at(0).block_number, 120);
    EXPECT_EQ(page.items.at(1).block_number, 93);
}
