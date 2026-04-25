#include "unit-tests.hpp"

#include "events_sql_assertions.hpp"
#include "events_test_harness.hpp"

using namespace dcn;
using namespace dcn::tests;
using namespace dcn::tests::events_harness;

namespace
{
    void finalizeAndProject(events::SQLiteHotStore & store, const events::DecodedEvent & event, const std::int64_t now_ms)
    {
        const events::ChainBlockInfo block = makeBlockInfo(
            event.raw.block_number,
            event.raw.block_hash,
            hexBytes(0x60, 32),
            event.raw.block_time.value_or(0),
            now_ms - 20);
        ASSERT_TRUE(store.ingestBatch(CHAIN_ID, {event}, {block}, event.raw.block_number + 1, now_ms - 10));
        EXPECT_EQ(projectAll(store, now_ms), 1u);

        const events::FinalityHeights heights{
            .head = event.raw.block_number + 100,
            .safe = event.raw.block_number,
            .finalized = event.raw.block_number
        };
        ASSERT_TRUE(store.applyFinality(CHAIN_ID, heights, now_ms + 10, 2048));
        EXPECT_EQ(projectAll(store, now_ms + 20), 1u);
    }
}

TEST_F(UnitTest, Events_Archive_ExportsOnlyFinalizedUnexportedRows)
{
    const auto paths = makeTempEventsPaths("archive_export_predicate");
    events::SQLiteHotStore store(paths.hot_db, paths.archive_root, 60 * 60 * 1000, CHAIN_ID);

    const events::DecodedEvent finalized = makeDecodedEvent(100, 0, 1, 0xF0, 0x30, events::EventType::CONNECTOR_ADDED, events::EventState::OBSERVED, 1'700'001'000);
    finalizeAndProject(store, finalized, 1'700'001'000'200);

    const events::DecodedEvent observed = makeDecodedEvent(101, 0, 1, 0xF1, 0x31, events::EventType::CONDITION_ADDED, events::EventState::OBSERVED, 1'700'001'001);
    ASSERT_TRUE(store.ingestBatch(
        CHAIN_ID,
        {observed},
        {makeBlockInfo(101, observed.raw.block_hash, hexBytes(0x5F, 32), 1'700'001'001, 1'700'001'001'100)},
        102,
        1'700'001'001'200));
    EXPECT_EQ(projectAll(store, 1'700'001'001'300), 1u);

    ASSERT_TRUE(store.runArchiveCycle(CHAIN_ID, 36500, 1'700'001'100'000));

    SqliteReadonly db(paths.hot_db);
    EXPECT_EQ(
        db.scalarInt64(std::format(
            "SELECT exported FROM normalized_events_hot WHERE block_hash='{}' AND log_index={};",
            finalized.raw.block_hash,
            finalized.raw.log_index)),
        1);
    EXPECT_EQ(
        db.scalarInt64(std::format(
            "SELECT exported FROM normalized_events_hot WHERE block_hash='{}' AND log_index={};",
            observed.raw.block_hash,
            observed.raw.log_index)),
        0);
}

TEST_F(UnitTest, Events_Archive_ReExportCycle_IsIdempotent)
{
    const auto paths = makeTempEventsPaths("archive_idempotent");
    events::SQLiteHotStore store(paths.hot_db, paths.archive_root, 60 * 60 * 1000, CHAIN_ID);

    const events::DecodedEvent event = makeDecodedEvent(102, 0, 1, 0xF2, 0x32, events::EventType::TRANSFORMATION_ADDED, events::EventState::OBSERVED, 1'700'001'002);
    finalizeAndProject(store, event, 1'700'001'002'200);

    ASSERT_TRUE(store.runArchiveCycle(CHAIN_ID, 36500, 1'700'001'110'000));
    ASSERT_TRUE(store.runArchiveCycle(CHAIN_ID, 36500, 1'700'001'120'000));

    SqliteReadonly db(paths.hot_db);
    EXPECT_EQ(db.scalarInt64("SELECT COUNT(1) FROM shard_catalog WHERE state='READY';"), 1);
    EXPECT_EQ(db.scalarInt64("SELECT exported FROM normalized_events_hot LIMIT 1;"), 1);
}

TEST_F(UnitTest, Events_Archive_PrunesOnlyAfterExportedRowsExist)
{
    const auto paths = makeTempEventsPaths("archive_prune_gate");
    events::SQLiteHotStore store(paths.hot_db, paths.archive_root, 60 * 60 * 1000, CHAIN_ID);

    const events::DecodedEvent event = makeDecodedEvent(103, 0, 1, 0xF3, 0x33, events::EventType::CONNECTOR_ADDED, events::EventState::OBSERVED, 1'600'000'000);
    finalizeAndProject(store, event, 1'700'001'003'200);

    ASSERT_TRUE(store.runArchiveCycle(CHAIN_ID, 36500, 1'700'001'130'000));
    events_sql::expectRowCount(paths.hot_db, "normalized_events_hot", 1);

    ASSERT_TRUE(store.runArchiveCycle(CHAIN_ID, 0, 1'900'000'000'000));
    events_sql::expectRowCount(paths.hot_db, "normalized_events_hot", 0);
    events_sql::expectRowCount(paths.hot_db, "feed_items_hot", 0);
}

TEST_F(UnitTest, Events_Archive_IsNotReplayAuthority_GlobalOutboxRemainsSource)
{
    const auto paths = makeTempEventsPaths("archive_not_replay_authority");
    events::SQLiteHotStore store(paths.hot_db, paths.archive_root, 60 * 60 * 1000, CHAIN_ID);

    const events::DecodedEvent event = makeDecodedEvent(104, 0, 1, 0xF4, 0x34, events::EventType::CONDITION_ADDED, events::EventState::OBSERVED, 1'600'000'001);
    finalizeAndProject(store, event, 1'700'001'004'200);

    ASSERT_TRUE(store.runArchiveCycle(CHAIN_ID, 0, 1'900'000'000'001));

    const events::StreamPage stream = store.getStreamPage(events::StreamQuery{
        .since_seq = 0,
        .limit = 100
    });
    ASSERT_FALSE(stream.deltas.empty());
    EXPECT_EQ(stream.deltas.back().status, "finalized");
}
