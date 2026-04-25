#include "unit-tests.hpp"

#include "events_sql_assertions.hpp"
#include "events_test_harness.hpp"

using namespace dcn;
using namespace dcn::tests;
using namespace dcn::tests::events_harness;

TEST_F(UnitTest, Events_Projector_TwoStage_IngestDoesNotPublishBeforeProjection)
{
    const auto paths = makeTempEventsPaths("project_two_stage");
    events::SQLiteHotStore store(paths.hot_db, paths.archive_root, 7LL * 24 * 60 * 60 * 1000, CHAIN_ID);

    const events::DecodedEvent event = makeDecodedEvent(60, 0, 1, 0xB0, 0xD0, events::EventType::CONNECTOR_ADDED, events::EventState::OBSERVED, 1'700'000'500);
    const events::ChainBlockInfo block = makeBlockInfo(60, event.raw.block_hash, hexBytes(0x91, 32), 1'700'000'500, 1'700'000'501'000);
    ASSERT_TRUE(store.ingestBatch(CHAIN_ID, {event}, {block}, 61, 1'700'000'501'100));

    events_sql::expectRowCount(paths.hot_db, "projection_jobs", 1);
    events_sql::expectRowCount(paths.hot_db, "feed_items_hot", 0);
    events_sql::expectRowCount(paths.hot_db, "global_outbox", 0);
}

TEST_F(UnitTest, Events_Projector_ProjectBatch_WritesFeedAndOutboxAndClearsJobs)
{
    const auto paths = makeTempEventsPaths("project_batch");
    events::SQLiteHotStore store(paths.hot_db, paths.archive_root, 7LL * 24 * 60 * 60 * 1000, CHAIN_ID);

    const events::DecodedEvent event = makeDecodedEvent(61, 1, 2, 0xB1, 0xD1, events::EventType::TRANSFORMATION_ADDED, events::EventState::OBSERVED, 1'700'000'510);
    const events::ChainBlockInfo block = makeBlockInfo(61, event.raw.block_hash, hexBytes(0x90, 32), 1'700'000'510, 1'700'000'511'000);
    ASSERT_TRUE(store.ingestBatch(CHAIN_ID, {event}, {block}, 62, 1'700'000'511'100));

    EXPECT_EQ(store.projectBatch(128, 1'700'000'511'200), 1u);
    EXPECT_EQ(store.projectBatch(128, 1'700'000'511'300), 0u);

    events_sql::expectRowCount(paths.hot_db, "projection_jobs", 0);
    events_sql::expectRowCount(paths.hot_db, "feed_items_hot", 1);
    events_sql::expectRowCount(paths.hot_db, "global_outbox", 1);

    SqliteReadonly db(paths.hot_db);
    EXPECT_EQ(db.scalarText("SELECT status FROM feed_items_hot LIMIT 1;"), "observed");
    EXPECT_EQ(db.scalarInt64("SELECT visible FROM feed_items_hot LIMIT 1;"), 1);
    EXPECT_EQ(db.scalarText("SELECT op FROM global_outbox LIMIT 1;"), "insert");
    EXPECT_EQ(db.scalarText("SELECT status FROM global_outbox LIMIT 1;"), "observed");
}

TEST_F(UnitTest, Events_Projector_RepeatedRuns_AreIdempotentWithoutNewJobs)
{
    const auto paths = makeTempEventsPaths("project_idempotent");
    events::SQLiteHotStore store(paths.hot_db, paths.archive_root, 7LL * 24 * 60 * 60 * 1000, CHAIN_ID);

    const events::DecodedEvent event = makeDecodedEvent(62, 1, 3, 0xB2, 0xD2, events::EventType::CONDITION_ADDED, events::EventState::OBSERVED, 1'700'000'520);
    const events::ChainBlockInfo block = makeBlockInfo(62, event.raw.block_hash, hexBytes(0x8F, 32), 1'700'000'520, 1'700'000'521'000);
    ASSERT_TRUE(store.ingestBatch(CHAIN_ID, {event}, {block}, 63, 1'700'000'521'100));

    EXPECT_EQ(store.projectBatch(1, 1'700'000'521'200), 1u);
    EXPECT_EQ(store.projectBatch(1, 1'700'000'521'300), 0u);
    EXPECT_EQ(store.projectBatch(1, 1'700'000'521'400), 0u);

    events_sql::expectRowCount(paths.hot_db, "feed_items_hot", 1);
    events_sql::expectRowCount(paths.hot_db, "global_outbox", 1);
}

TEST_F(UnitTest, Events_Projector_RestartAfterDurableIngest_ProjectsPendingJobs)
{
    const auto paths = makeTempEventsPaths("project_restart");

    {
        events::SQLiteHotStore store(paths.hot_db, paths.archive_root, 7LL * 24 * 60 * 60 * 1000, CHAIN_ID);
        const events::DecodedEvent event = makeDecodedEvent(63, 2, 4, 0xB3, 0xD3, events::EventType::CONNECTOR_ADDED, events::EventState::OBSERVED, 1'700'000'530);
        const events::ChainBlockInfo block = makeBlockInfo(63, event.raw.block_hash, hexBytes(0x8E, 32), 1'700'000'530, 1'700'000'531'000);
        ASSERT_TRUE(store.ingestBatch(CHAIN_ID, {event}, {block}, 64, 1'700'000'531'100));
    }

    {
        events::SQLiteHotStore store(paths.hot_db, paths.archive_root, 7LL * 24 * 60 * 60 * 1000, CHAIN_ID);
        EXPECT_EQ(projectAll(store, 1'700'000'532'000), 1u);
    }

    events_sql::expectRowCount(paths.hot_db, "feed_items_hot", 1);
    events_sql::expectRowCount(paths.hot_db, "global_outbox", 1);
    events_sql::expectRowCount(paths.hot_db, "projection_jobs", 0);
}

TEST_F(UnitTest, Events_Projector_FinalityTransition_EmitsUpdateOutboxItem)
{
    const auto paths = makeTempEventsPaths("project_finality_update");
    events::SQLiteHotStore store(paths.hot_db, paths.archive_root, 7LL * 24 * 60 * 60 * 1000, CHAIN_ID);

    const events::DecodedEvent event = makeDecodedEvent(64, 0, 1, 0xB4, 0xD4, events::EventType::TRANSFORMATION_ADDED, events::EventState::OBSERVED, 1'700'000'540);
    const events::ChainBlockInfo block = makeBlockInfo(64, event.raw.block_hash, hexBytes(0x8D, 32), 1'700'000'540, 1'700'000'541'000);
    ASSERT_TRUE(store.ingestBatch(CHAIN_ID, {event}, {block}, 65, 1'700'000'541'100));

    EXPECT_EQ(projectAll(store, 1'700'000'541'200), 1u);

    const events::FinalityHeights heights{
        .head = 100,
        .safe = 64,
        .finalized = 64
    };
    ASSERT_TRUE(store.applyFinality(CHAIN_ID, heights, 1'700'000'542'000, 2048));
    EXPECT_EQ(projectAll(store, 1'700'000'542'100), 1u);

    events_sql::expectRowCount(paths.hot_db, "global_outbox", 2);

    SqliteReadonly db(paths.hot_db);
    EXPECT_EQ(db.scalarText("SELECT status FROM feed_items_hot LIMIT 1;"), "finalized");
    EXPECT_EQ(db.scalarText("SELECT op FROM global_outbox WHERE stream_seq=1;"), "insert");
    EXPECT_EQ(db.scalarText("SELECT op FROM global_outbox WHERE stream_seq=2;"), "update");
    EXPECT_EQ(db.scalarText("SELECT status FROM global_outbox WHERE stream_seq=2;"), "finalized");
}
