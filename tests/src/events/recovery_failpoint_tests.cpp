#include "unit-tests.hpp"

#include "events_sql_assertions.hpp"
#include "events_test_harness.hpp"

using namespace dcn;
using namespace dcn::tests;
using namespace dcn::tests::events_harness;

TEST_F(UnitTest, Events_Recovery_CrashBeforeIngestCommit_LeavesNoPartialState)
{
    const auto paths = makeTempEventsPaths("recovery_pre_commit");

    {
        events::SQLiteHotStore store(paths.hot_db, paths.archive_root, 60 * 60 * 1000, CHAIN_ID);

        events::DecodedEvent valid = makeDecodedEvent(110, 0, 1, 0xA8, 0x38, events::EventType::CONNECTOR_ADDED, events::EventState::OBSERVED, 1'700'002'000);
        events::DecodedEvent invalid = makeDecodedEvent(110, 0, 2, 0xA8, 0x39, events::EventType::CONNECTOR_ADDED, events::EventState::OBSERVED, 1'700'002'000);
        invalid.raw.topics[0] = std::nullopt;

        const events::ChainBlockInfo block = makeBlockInfo(110, valid.raw.block_hash, hexBytes(0x50, 32), 1'700'002'000, 1'700'002'000'100);
        EXPECT_FALSE(store.ingestBatch(CHAIN_ID, {valid, invalid}, {block}, 111, 1'700'002'000'200));
    }

    {
        events::SQLiteHotStore restarted(paths.hot_db, paths.archive_root, 60 * 60 * 1000, CHAIN_ID);
        EXPECT_FALSE(restarted.loadNextFromBlock(CHAIN_ID).has_value());
    }

    events_sql::expectRowCount(paths.hot_db, "raw_events_hot", 0);
    events_sql::expectRowCount(paths.hot_db, "normalized_events_hot", 0);
    events_sql::expectRowCount(paths.hot_db, "projection_jobs", 0);
}

TEST_F(UnitTest, Events_Recovery_CrashAfterIngestBeforeProjection_ProjectsAfterRestart)
{
    const auto paths = makeTempEventsPaths("recovery_post_ingest_pre_projection");

    events::DecodedEvent event;
    {
        events::SQLiteHotStore store(paths.hot_db, paths.archive_root, 60 * 60 * 1000, CHAIN_ID);
        event = makeDecodedEvent(111, 0, 1, 0xA9, 0x3A, events::EventType::TRANSFORMATION_ADDED, events::EventState::OBSERVED, 1'700'002'001);
        const events::ChainBlockInfo block = makeBlockInfo(111, event.raw.block_hash, hexBytes(0x4F, 32), 1'700'002'001, 1'700'002'001'100);
        ASSERT_TRUE(store.ingestBatch(CHAIN_ID, {event}, {block}, 112, 1'700'002'001'200));
    }

    {
        events::SQLiteHotStore restarted(paths.hot_db, paths.archive_root, 60 * 60 * 1000, CHAIN_ID);
        EXPECT_EQ(projectAll(restarted, 1'700'002'001'500), 1u);
    }

    events_sql::expectRowCount(paths.hot_db, "feed_items_hot", 1);
    events_sql::expectRowCount(paths.hot_db, "global_outbox", 1);
    events_sql::expectRowCount(paths.hot_db, "projection_jobs", 0);
}

TEST_F(UnitTest, Events_Recovery_CrashAfterProjectionCommit_StreamReplayRemainsConsistent)
{
    const auto paths = makeTempEventsPaths("recovery_post_projection");

    {
        events::SQLiteHotStore store(paths.hot_db, paths.archive_root, 60 * 60 * 1000, CHAIN_ID);
        const events::DecodedEvent first = makeDecodedEvent(112, 0, 1, 0xAA, 0x3B, events::EventType::CONDITION_ADDED, events::EventState::OBSERVED, 1'700'002'002);
        const events::DecodedEvent second = makeDecodedEvent(113, 0, 1, 0xAB, 0x3C, events::EventType::CONDITION_ADDED, events::EventState::OBSERVED, 1'700'002'003);
        ASSERT_TRUE(store.ingestBatch(
            CHAIN_ID,
            {first, second},
            {
                makeBlockInfo(112, first.raw.block_hash, hexBytes(0x4E, 32), 1'700'002'002, 1'700'002'002'100),
                makeBlockInfo(113, second.raw.block_hash, hexBytes(0x4D, 32), 1'700'002'003, 1'700'002'003'100)
            },
            114,
            1'700'002'003'200));
        EXPECT_EQ(projectAll(store, 1'700'002'003'300), 2u);
    }

    {
        events::SQLiteHotStore restarted(paths.hot_db, paths.archive_root, 60 * 60 * 1000, CHAIN_ID);
        const events::StreamPage replay = restarted.getStreamPage(events::StreamQuery{
            .since_seq = 1,
            .limit = 100
        });
        ASSERT_EQ(replay.deltas.size(), 1u);
        EXPECT_EQ(replay.deltas.front().stream_seq, 2);
    }
}

TEST_F(UnitTest, Events_Recovery_RpcDisconnectEquivalent_NoCorruptionOnNoopCycle)
{
    const auto paths = makeTempEventsPaths("recovery_noop_cycles");
    events::SQLiteHotStore store(paths.hot_db, paths.archive_root, 60 * 60 * 1000, CHAIN_ID);

    // Simulate repeated no-op polling cycles by only applying advancing finality.
    for(std::int64_t i = 0; i < 5; ++i)
    {
        const events::FinalityHeights heights{
            .head = 200 + i,
            .safe = 190 + i,
            .finalized = 180 + i
        };
        ASSERT_TRUE(store.applyFinality(CHAIN_ID, heights, 1'700'002'100'000 + i, 2048));
    }

    events_sql::expectRowCount(paths.hot_db, "raw_events_hot", 0);
    events_sql::expectRowCount(paths.hot_db, "normalized_events_hot", 0);
    events_sql::expectRowCount(paths.hot_db, "feed_items_hot", 0);
    {
        SqliteReadonly db(paths.hot_db);
        EXPECT_EQ(db.scalarInt64("SELECT head_block FROM finality_state WHERE chain_id=1;"), 204);
    }
}
