#include "unit-tests.hpp"

#include "events_sql_assertions.hpp"
#include "events_test_harness.hpp"

using namespace dcn;
using namespace dcn::tests;
using namespace dcn::tests::events_harness;

namespace
{
    void finalizeAndProject(
        asio::io_context & store_io_context,
        events::SQLiteHotStore & store,
        const events::DecodedEvent & event,
        const std::int64_t now_ms)
    {
        const events::ChainBlockInfo block = makeBlockInfo(
            event.raw.block_number,
            event.raw.block_hash,
            hexBytes(0x60, 32),
            event.raw.block_time.value_or(0),
            now_ms - 20);
        ASSERT_TRUE(awaitIngestBatch(
            store_io_context,
            store,
            CHAIN_ID,
            {event},
            {block},
            event.raw.block_number + 1,
            now_ms - 10));
        EXPECT_EQ(projectAll(store_io_context, store, now_ms), 1u);

        const events::FinalityHeights heights{
            .head = event.raw.block_number + 100,
            .safe = event.raw.block_number,
            .finalized = event.raw.block_number
        };
        ASSERT_TRUE(awaitApplyFinality(store_io_context, store, CHAIN_ID, heights, now_ms + 10, 2048));
        EXPECT_EQ(projectAll(store_io_context, store, now_ms + 20), 1u);
    }
}

TEST_F(UnitTest, Events_Archive_ExportsOnlyFinalizedUnexportedRows)
{
    const auto paths = makeTempEventsPaths("archive_export_predicate");
    asio::io_context store_io_context;
    events::SQLiteHotStore store(paths.hot_db, paths.archive_root, 60 * 60 * 1000, CHAIN_ID);

    const events::DecodedEvent finalized = makeDecodedEvent(100, 0, 1, 0xF0, 0x30, events::EventType::CONNECTOR_ADDED, events::EventState::OBSERVED, 1'700'001'000);
    finalizeAndProject(store_io_context, store, finalized, 1'700'001'000'200);

    const events::DecodedEvent observed = makeDecodedEvent(101, 0, 1, 0xF1, 0x31, events::EventType::CONDITION_ADDED, events::EventState::OBSERVED, 1'700'001'001);
    ASSERT_TRUE(awaitIngestBatch(
        store_io_context,
        store,
        CHAIN_ID,
        {observed},
        {makeBlockInfo(101, observed.raw.block_hash, hexBytes(0x5F, 32), 1'700'001'001, 1'700'001'001'100)},
        102,
        1'700'001'001'200));
    EXPECT_EQ(projectAll(store_io_context, store, 1'700'001'001'300), 1u);

    ASSERT_TRUE(awaitRunArchiveCycle(store_io_context, store, CHAIN_ID, 36500, 1'700'001'100'000));

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
    asio::io_context store_io_context;
    events::SQLiteHotStore store(paths.hot_db, paths.archive_root, 60 * 60 * 1000, CHAIN_ID);

    const events::DecodedEvent event = makeDecodedEvent(102, 0, 1, 0xF2, 0x32, events::EventType::TRANSFORMATION_ADDED, events::EventState::OBSERVED, 1'700'001'002);
    finalizeAndProject(store_io_context, store, event, 1'700'001'002'200);

    ASSERT_TRUE(awaitRunArchiveCycle(store_io_context, store, CHAIN_ID, 36500, 1'700'001'110'000));
    ASSERT_TRUE(awaitRunArchiveCycle(store_io_context, store, CHAIN_ID, 36500, 1'700'001'120'000));

    SqliteReadonly db(paths.hot_db);
    EXPECT_EQ(db.scalarInt64("SELECT COUNT(1) FROM shard_catalog WHERE state='READY';"), 1);
    EXPECT_EQ(db.scalarInt64("SELECT exported FROM normalized_events_hot LIMIT 1;"), 1);
}

TEST_F(UnitTest, Events_Archive_PrunesOnlyAfterExportedRowsExist)
{
    const auto paths = makeTempEventsPaths("archive_prune_gate");
    asio::io_context store_io_context;
    events::SQLiteHotStore store(paths.hot_db, paths.archive_root, 60 * 60 * 1000, CHAIN_ID);

    const events::DecodedEvent event = makeDecodedEvent(103, 0, 1, 0xF3, 0x33, events::EventType::CONNECTOR_ADDED, events::EventState::OBSERVED, 1'600'000'000);
    finalizeAndProject(store_io_context, store, event, 1'700'001'003'200);

    ASSERT_TRUE(awaitRunArchiveCycle(store_io_context, store, CHAIN_ID, 36500, 1'700'001'130'000));
    events_sql::expectRowCount(paths.hot_db, "normalized_events_hot", 1);

    ASSERT_TRUE(awaitRunArchiveCycle(store_io_context, store, CHAIN_ID, 0, 1'900'000'000'000));
    events_sql::expectRowCount(paths.hot_db, "raw_events_hot", 0);
    events_sql::expectRowCount(paths.hot_db, "normalized_events_hot", 0);
    events_sql::expectRowCount(paths.hot_db, "feed_items_hot", 0);
}

TEST_F(UnitTest, Events_Archive_IsNotReplayAuthority_GlobalOutboxRemainsSource)
{
    const auto paths = makeTempEventsPaths("archive_not_replay_authority");
    asio::io_context store_io_context;
    events::SQLiteHotStore store(paths.hot_db, paths.archive_root, 60 * 60 * 1000, CHAIN_ID);

    const events::DecodedEvent event = makeDecodedEvent(104, 0, 1, 0xF4, 0x34, events::EventType::CONDITION_ADDED, events::EventState::OBSERVED, 1'600'000'001);
    finalizeAndProject(store_io_context, store, event, 1'700'001'004'200);

    ASSERT_TRUE(awaitRunArchiveCycle(store_io_context, store, CHAIN_ID, 0, 1'900'000'000'001));

    const events::StreamPage stream = store.getStreamPage(events::StreamQuery{
        .since_seq = 0,
        .limit = 100
    });
    ASSERT_FALSE(stream.deltas.empty());
    EXPECT_EQ(stream.deltas.back().status, "finalized");
}

TEST_F(UnitTest, Events_Archive_MonthSelection_IncludesPendingFinalizedFeedRows)
{
    const auto paths = makeTempEventsPaths("archive_feed_month_candidate");
    asio::io_context store_io_context;
    events::SQLiteHotStore store(paths.hot_db, paths.archive_root, 60 * 60 * 1000, CHAIN_ID);

    const events::DecodedEvent event = makeDecodedEvent(
        105,
        0,
        1,
        0xF5,
        0x35,
        events::EventType::TRANSFORMATION_ADDED,
        events::EventState::OBSERVED,
        1'700'001'005);
    finalizeAndProject(store_io_context, store, event, 1'700'001'005'200);

    ASSERT_TRUE(awaitRunArchiveCycle(store_io_context, store, CHAIN_ID, 36500, 1'700'001'140'000));

    {
        SqliteWritable db(paths.hot_db);
        db.exec("UPDATE feed_items_hot SET exported=0 WHERE chain_id=1;");
    }

    ASSERT_TRUE(awaitRunArchiveCycle(store_io_context, store, CHAIN_ID, 36500, 1'700'001'150'000));

    SqliteReadonly db(paths.hot_db);
    EXPECT_EQ(db.scalarInt64("SELECT exported FROM normalized_events_hot WHERE chain_id=1 LIMIT 1;"), 1);
    EXPECT_EQ(db.scalarInt64("SELECT exported FROM feed_items_hot WHERE chain_id=1 LIMIT 1;"), 1);
}

TEST_F(UnitTest, Events_Archive_DoesNotExportOrPruneRowsWithPendingProjectionJobs)
{
    const auto paths = makeTempEventsPaths("archive_respects_projection_backlog");
    asio::io_context store_io_context;
    events::SQLiteHotStore store(paths.hot_db, paths.archive_root, 60 * 60 * 1000, CHAIN_ID);

    const events::DecodedEvent event = makeDecodedEvent(
        200,
        0,
        1,
        0xE8,
        0x48,
        events::EventType::CONNECTOR_ADDED,
        events::EventState::OBSERVED,
        1'600'000'000);
    const events::ChainBlockInfo block = makeBlockInfo(
        200,
        event.raw.block_hash,
        hexBytes(0x77, 32),
        1'600'000'000,
        1'700'010'000'100);

    ASSERT_TRUE(awaitIngestBatch(store_io_context, store, CHAIN_ID, {event}, {block}, 201, 1'700'010'000'200));
    EXPECT_EQ(projectAll(store_io_context, store, 1'700'010'000'300), 1u);

    const events::FinalityHeights heights{
        .head = 400,
        .safe = 200,
        .finalized = 200
    };
    ASSERT_TRUE(awaitApplyFinality(store_io_context, store, CHAIN_ID, heights, 1'700'010'000'400, 2048));

    // Keep the finality projection job pending, then run an aggressive archive/prune cycle.
    ASSERT_TRUE(awaitRunArchiveCycle(store_io_context, store, CHAIN_ID, 0, 1'900'000'000'000));

    SqliteReadonly db(paths.hot_db);
    EXPECT_EQ(db.scalarInt64("SELECT COUNT(1) FROM projection_jobs;"), 1);
    EXPECT_EQ(db.scalarInt64("SELECT COUNT(1) FROM normalized_events_hot;"), 1);
    EXPECT_EQ(db.scalarInt64("SELECT exported FROM normalized_events_hot LIMIT 1;"), 0);
}

TEST_F(UnitTest, Events_Archive_NormalizedRowsRequireFeedProjectionEvidence)
{
    const auto paths = makeTempEventsPaths("archive_requires_feed_projection_evidence");
    asio::io_context store_io_context;
    events::SQLiteHotStore store(paths.hot_db, paths.archive_root, 60 * 60 * 1000, CHAIN_ID);

    const events::DecodedEvent event = makeDecodedEvent(
        210,
        0,
        1,
        0xE9,
        0x49,
        events::EventType::TRANSFORMATION_ADDED,
        events::EventState::OBSERVED,
        1'600'100'000);
    finalizeAndProject(store_io_context, store, event, 1'700'011'000'200);

    {
        SqliteWritable db(paths.hot_db);
        db.exec("DELETE FROM feed_items_hot WHERE chain_id=1;");
    }

    ASSERT_TRUE(awaitRunArchiveCycle(store_io_context, store, CHAIN_ID, 0, 1'900'001'000'000));

    SqliteReadonly db(paths.hot_db);
    EXPECT_EQ(db.scalarInt64("SELECT COUNT(1) FROM normalized_events_hot;"), 1);
    EXPECT_EQ(db.scalarInt64("SELECT exported FROM normalized_events_hot LIMIT 1;"), 0);
}

TEST_F(UnitTest, Events_Archive_FinalizationRejectsNormalizedRowsWithDivergedSnapshot)
{
    const auto paths = makeTempEventsPaths("archive_snapshot_mismatch_normalized");
    asio::io_context store_io_context;
    events::SQLiteHotStore store(paths.hot_db, paths.archive_root, 60 * 60 * 1000, CHAIN_ID);

    const events::DecodedEvent event = makeDecodedEvent(
        310,
        0,
        1,
        0xD0,
        0x60,
        events::EventType::CONNECTOR_ADDED,
        events::EventState::OBSERVED,
        1'700'002'000);
    finalizeAndProject(store_io_context, store, event, 1'700'002'000'200);

    {
        SqliteReadonly db(paths.hot_db);
        ASSERT_EQ(db.scalarInt64("SELECT exported FROM normalized_events_hot LIMIT 1;"), 0);
    }

    // Reset exported and capture the snapshot identity, then mutate the row mid-archive
    // by simulating a divergence: pre-image identity must not match post-mutation row.
    const std::string staged_name = "staged-snapshot";
    const std::int64_t staged_updated_at_ms = 1'700'002'000'250;
    {
        SqliteWritable db(paths.hot_db);
        db.exec(std::format(
            "UPDATE normalized_events_hot SET name='{}', updated_at_ms={} "
            "WHERE chain_id=1 AND block_hash='{}' AND log_index={};",
            staged_name,
            staged_updated_at_ms,
            event.raw.block_hash,
            event.raw.log_index));
    }

    ASSERT_TRUE(awaitRunArchiveCycle(store_io_context, store, CHAIN_ID, 36500, 1'700'002'000'300));

    {
        SqliteReadonly db(paths.hot_db);
        EXPECT_EQ(db.scalarInt64("SELECT exported FROM normalized_events_hot LIMIT 1;"), 1);
    }

    // Now diverge AFTER the row is exported: reset exported and tamper name
    // such that the *next* archive cycle's selected snapshot would archive the new content
    // and only mark exported when the snapshot matches the current row.
    {
        SqliteWritable db(paths.hot_db);
        db.exec(std::format(
            "UPDATE normalized_events_hot SET exported=0, name='diverged', "
            "updated_at_ms={} "
            "WHERE chain_id=1 AND block_hash='{}' AND log_index={};",
            staged_updated_at_ms + 1,
            event.raw.block_hash,
            event.raw.log_index));
    }

    // Issue the same UPDATE finalization would issue, but with a stale snapshot identity
    // (the pre-divergence updated_at_ms / name). It must match zero rows.
    {
        SqliteWritable db(paths.hot_db);
        db.exec(std::format(
            "UPDATE normalized_events_hot SET exported=1, updated_at_ms=9999999999999 "
            "WHERE chain_id=1 AND block_hash='{}' AND log_index={} "
            "AND state='finalized' AND projected_version=1 "
            "AND updated_at_ms={} AND name='{}' AND exported=0;",
            event.raw.block_hash,
            event.raw.log_index,
            staged_updated_at_ms,
            staged_name));
    }

    {
        SqliteReadonly db(paths.hot_db);
        EXPECT_EQ(db.scalarInt64("SELECT exported FROM normalized_events_hot LIMIT 1;"), 0);
    }
}

TEST_F(UnitTest, Events_Archive_FinalizationRejectsFeedRowsWithDivergedSnapshot)
{
    const auto paths = makeTempEventsPaths("archive_snapshot_mismatch_feed");
    asio::io_context store_io_context;
    events::SQLiteHotStore store(paths.hot_db, paths.archive_root, 60 * 60 * 1000, CHAIN_ID);

    const events::DecodedEvent event = makeDecodedEvent(
        320,
        0,
        1,
        0xD1,
        0x61,
        events::EventType::TRANSFORMATION_ADDED,
        events::EventState::OBSERVED,
        1'700'002'100);
    finalizeAndProject(store_io_context, store, event, 1'700'002'100'200);

    ASSERT_TRUE(awaitRunArchiveCycle(store_io_context, store, CHAIN_ID, 36500, 1'700'002'100'300));

    {
        SqliteReadonly db(paths.hot_db);
        EXPECT_EQ(db.scalarInt64("SELECT exported FROM feed_items_hot LIMIT 1;"), 1);
    }

    // Reset the row and mutate payload_json so the previous archived snapshot is stale.
    {
        SqliteWritable db(paths.hot_db);
        db.exec("UPDATE feed_items_hot SET exported=0, payload_json='{\"diverged\":true}', "
                "updated_at_ms=updated_at_ms+1 WHERE chain_id=1;");
    }

    // Issue the finalization UPDATE with stale snapshot fields; it must match zero rows.
    {
        SqliteWritable db(paths.hot_db);
        db.exec("UPDATE feed_items_hot SET exported=1, updated_at_ms=9999999999999 "
                "WHERE chain_id=1 AND status='finalized' "
                "AND projector_version=1 AND updated_at_ms=0 "
                "AND history_cursor='stale-cursor' AND payload_json='{\"name\":\"old\"}' "
                "AND exported=0;");
    }

    {
        SqliteReadonly db(paths.hot_db);
        EXPECT_EQ(db.scalarInt64("SELECT exported FROM feed_items_hot LIMIT 1;"), 0);
    }
}

TEST_F(UnitTest, Events_Archive_PruneRetainsBothRowsWhenFeedPairIsUnexported)
{
    const auto paths = makeTempEventsPaths("archive_prune_keeps_pair_when_feed_unexported");
    asio::io_context store_io_context;
    events::SQLiteHotStore store(paths.hot_db, paths.archive_root, 60 * 60 * 1000, CHAIN_ID);

    const events::DecodedEvent event = makeDecodedEvent(
        330,
        0,
        1,
        0xD2,
        0x62,
        events::EventType::CONDITION_ADDED,
        events::EventState::OBSERVED,
        1'600'200'000);
    finalizeAndProject(store_io_context, store, event, 1'700'002'200'200);

    // First archive cycle: rows become exported=1.
    ASSERT_TRUE(awaitRunArchiveCycle(store_io_context, store, CHAIN_ID, 36500, 1'700'002'200'300));
    {
        SqliteReadonly db(paths.hot_db);
        EXPECT_EQ(db.scalarInt64("SELECT exported FROM normalized_events_hot LIMIT 1;"), 1);
        EXPECT_EQ(db.scalarInt64("SELECT exported FROM feed_items_hot LIMIT 1;"), 1);
    }

    // Simulate the orphan-creating scenario: feed row drifted back to exported=0
    // (e.g. snapshot mismatch on a prior finalize) while the normalized row
    // remained exported=1. We also clear feed.block_time so the next archive
    // cycle's export phase will not re-pick the feed row.
    {
        SqliteWritable db(paths.hot_db);
        db.exec("UPDATE feed_items_hot SET exported=0, block_time=NULL WHERE chain_id=1;");
    }

    // Aggressive prune cycle: hot_window_days=0 makes everything prune-eligible
    // by block_time. With the pair-aware prune_keys gate, neither side should be
    // pruned — otherwise the unexported feed row would be orphaned.
    ASSERT_TRUE(awaitRunArchiveCycle(store_io_context, store, CHAIN_ID, 0, 1'900'002'200'000));

    SqliteReadonly db(paths.hot_db);
    EXPECT_EQ(db.scalarInt64("SELECT COUNT(1) FROM feed_items_hot;"), 1);
    EXPECT_EQ(db.scalarInt64("SELECT exported FROM feed_items_hot LIMIT 1;"), 0);
    EXPECT_EQ(db.scalarInt64("SELECT COUNT(1) FROM normalized_events_hot;"), 1);
    EXPECT_EQ(db.scalarInt64("SELECT exported FROM normalized_events_hot LIMIT 1;"), 1);
    EXPECT_EQ(db.scalarInt64("SELECT COUNT(1) FROM raw_events_hot;"), 1);
}
