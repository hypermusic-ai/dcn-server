#include "unit-tests.hpp"

#include "events_sql_assertions.hpp"
#include "events_test_harness.hpp"

using namespace dcn;
using namespace dcn::tests;
using namespace dcn::tests::events_harness;

TEST_F(UnitTest, Events_Projector_TwoStage_IngestDoesNotPublishBeforeProjection)
{
    const auto paths = makeTempEventsPaths("project_two_stage");
    asio::io_context store_io_context;
    events::SQLiteHotStore store(
        paths.hot_db,
        paths.archive_root,
        7LL * 24 * 60 * 60 * 1000,
        CHAIN_ID);

    const events::DecodedEvent event = makeDecodedEvent(60, 0, 1, 0xB0, 0xD0, events::EventType::CONNECTOR_ADDED, events::EventState::OBSERVED, 1'700'000'500);
    const events::ChainBlockInfo block = makeBlockInfo(60, event.raw.block_hash, hexBytes(0x91, 32), 1'700'000'500, 1'700'000'501'000);
    ASSERT_TRUE(awaitIngestBatch(store_io_context, store, CHAIN_ID, {event}, {block}, 61, 1'700'000'501'100));

    events_sql::expectRowCount(paths.hot_db, "projection_jobs", 1);
    events_sql::expectRowCount(paths.hot_db, "feed_items_hot", 0);
    events_sql::expectRowCount(paths.hot_db, "global_outbox", 0);
}

TEST_F(UnitTest, Events_Projector_ProjectBatch_WritesFeedAndOutboxAndClearsJobs)
{
    const auto paths = makeTempEventsPaths("project_batch");
    asio::io_context store_io_context;
    events::SQLiteHotStore store(
        paths.hot_db,
        paths.archive_root,
        7LL * 24 * 60 * 60 * 1000,
        CHAIN_ID);

    const events::DecodedEvent event = makeDecodedEvent(61, 1, 2, 0xB1, 0xD1, events::EventType::TRANSFORMATION_ADDED, events::EventState::OBSERVED, 1'700'000'510);
    const events::ChainBlockInfo block = makeBlockInfo(61, event.raw.block_hash, hexBytes(0x90, 32), 1'700'000'510, 1'700'000'511'000);
    ASSERT_TRUE(awaitIngestBatch(store_io_context, store, CHAIN_ID, {event}, {block}, 62, 1'700'000'511'100));

    EXPECT_EQ(awaitProjectBatch(store_io_context, store, 128, 1'700'000'511'200), 1u);
    EXPECT_EQ(awaitProjectBatch(store_io_context, store, 128, 1'700'000'511'300), 0u);

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
    asio::io_context store_io_context;
    events::SQLiteHotStore store(
        paths.hot_db,
        paths.archive_root,
        7LL * 24 * 60 * 60 * 1000,
        CHAIN_ID);

    const events::DecodedEvent event = makeDecodedEvent(62, 1, 3, 0xB2, 0xD2, events::EventType::CONDITION_ADDED, events::EventState::OBSERVED, 1'700'000'520);
    const events::ChainBlockInfo block = makeBlockInfo(62, event.raw.block_hash, hexBytes(0x8F, 32), 1'700'000'520, 1'700'000'521'000);
    ASSERT_TRUE(awaitIngestBatch(store_io_context, store, CHAIN_ID, {event}, {block}, 63, 1'700'000'521'100));

    EXPECT_EQ(awaitProjectBatch(store_io_context, store, 1, 1'700'000'521'200), 1u);
    EXPECT_EQ(awaitProjectBatch(store_io_context, store, 1, 1'700'000'521'300), 0u);
    EXPECT_EQ(awaitProjectBatch(store_io_context, store, 1, 1'700'000'521'400), 0u);

    events_sql::expectRowCount(paths.hot_db, "feed_items_hot", 1);
    events_sql::expectRowCount(paths.hot_db, "global_outbox", 1);
}

TEST_F(UnitTest, Events_Projector_RequeuedNoopJobs_AreFullyDrainedAcrossBatches)
{
    const auto paths = makeTempEventsPaths("project_requeued_noop_jobs");
    asio::io_context store_io_context;
    events::SQLiteHotStore store(
        paths.hot_db,
        paths.archive_root,
        7LL * 24 * 60 * 60 * 1000,
        CHAIN_ID);

    std::vector<events::DecodedEvent> events_batch;
    std::vector<events::ChainBlockInfo> blocks_batch;
    events_batch.reserve(520);
    blocks_batch.reserve(520);
    for (int i = 0; i < 520; ++i)
    {
        const std::int64_t block_number = 1'000 + i;
        auto event = makeDecodedEvent(
            block_number,
            0,
            static_cast<std::int64_t>(i + 1),
            static_cast<std::uint8_t>(0x20 + (i % 200)),
            static_cast<std::uint8_t>(0x80 + (i % 120)),
            events::EventType::TRANSFORMATION_ADDED,
            events::EventState::OBSERVED,
            1'700'030'000 + i);
        events_batch.push_back(event);
        blocks_batch.push_back(makeBlockInfo(
            block_number,
            event.raw.block_hash,
            hexBytes(static_cast<std::uint8_t>(0x40 + (i % 180)), 32),
            1'700'030'000 + i,
            1'700'030'000'100 + i));
    }

    ASSERT_TRUE(awaitIngestBatch(
        store_io_context,
        store,
        CHAIN_ID,
        events_batch,
        blocks_batch,
        2'000,
        1'700'030'001'000));
    EXPECT_EQ(projectAll(store_io_context, store, 1'700'030'002'000), 520u);
    events_sql::expectRowCount(paths.hot_db, "projection_jobs", 0);

    {
        SqliteWritable db(paths.hot_db);
        db.exec(
            "INSERT OR IGNORE INTO projection_jobs(chain_id, block_hash, log_index, created_at_ms) "
            "SELECT chain_id, block_hash, log_index, 1700030003000 "
            "FROM normalized_events_hot;");
    }
    events_sql::expectRowCount(paths.hot_db, "projection_jobs", 520);

    EXPECT_EQ(projectAll(store_io_context, store, 1'700'030'004'000), 520u);
    events_sql::expectRowCount(paths.hot_db, "projection_jobs", 0);
}

TEST_F(UnitTest, Events_Projector_RestartAfterDurableIngest_ProjectsPendingJobs)
{
    const auto paths = makeTempEventsPaths("project_restart");

    {
        asio::io_context store_io_context;
        events::SQLiteHotStore store(
            paths.hot_db,
            paths.archive_root,
            7LL * 24 * 60 * 60 * 1000,
            CHAIN_ID);
        const events::DecodedEvent event = makeDecodedEvent(63, 2, 4, 0xB3, 0xD3, events::EventType::CONNECTOR_ADDED, events::EventState::OBSERVED, 1'700'000'530);
        const events::ChainBlockInfo block = makeBlockInfo(63, event.raw.block_hash, hexBytes(0x8E, 32), 1'700'000'530, 1'700'000'531'000);
        ASSERT_TRUE(awaitIngestBatch(store_io_context, store, CHAIN_ID, {event}, {block}, 64, 1'700'000'531'100));
    }

    {
        asio::io_context store_io_context;
        events::SQLiteHotStore store(
            paths.hot_db,
            paths.archive_root,
            7LL * 24 * 60 * 60 * 1000,
            CHAIN_ID);
        EXPECT_EQ(projectAll(store_io_context, store, 1'700'000'532'000), 1u);
    }

    events_sql::expectRowCount(paths.hot_db, "feed_items_hot", 1);
    events_sql::expectRowCount(paths.hot_db, "global_outbox", 1);
    events_sql::expectRowCount(paths.hot_db, "projection_jobs", 0);
}

TEST_F(UnitTest, Events_Projector_FinalityTransition_EmitsUpdateOutboxItem)
{
    const auto paths = makeTempEventsPaths("project_finality_update");
    asio::io_context store_io_context;
    events::SQLiteHotStore store(
        paths.hot_db,
        paths.archive_root,
        7LL * 24 * 60 * 60 * 1000,
        CHAIN_ID);

    const events::DecodedEvent event = makeDecodedEvent(64, 0, 1, 0xB4, 0xD4, events::EventType::TRANSFORMATION_ADDED, events::EventState::OBSERVED, 1'700'000'540);
    const events::ChainBlockInfo block = makeBlockInfo(64, event.raw.block_hash, hexBytes(0x8D, 32), 1'700'000'540, 1'700'000'541'000);
    ASSERT_TRUE(awaitIngestBatch(store_io_context, store, CHAIN_ID, {event}, {block}, 65, 1'700'000'541'100));

    EXPECT_EQ(projectAll(store_io_context, store, 1'700'000'541'200), 1u);

    const events::FinalityHeights heights{
        .head = 100,
        .safe = 64,
        .finalized = 64
    };
    ASSERT_TRUE(awaitApplyFinality(store_io_context, store, CHAIN_ID, heights, 1'700'000'542'000, 2048));
    EXPECT_EQ(projectAll(store_io_context, store, 1'700'000'542'100), 1u);

    events_sql::expectRowCount(paths.hot_db, "global_outbox", 2);

    SqliteReadonly db(paths.hot_db);
    EXPECT_EQ(db.scalarText("SELECT status FROM feed_items_hot LIMIT 1;"), "finalized");
    EXPECT_EQ(db.scalarInt64("SELECT projected_version FROM normalized_events_hot LIMIT 1;"), 1);
    EXPECT_EQ(db.scalarText("SELECT op FROM global_outbox WHERE stream_seq=1;"), "insert");
    EXPECT_EQ(db.scalarText("SELECT op FROM global_outbox WHERE stream_seq=2;"), "update");
    EXPECT_EQ(db.scalarText("SELECT status FROM global_outbox WHERE stream_seq=2;"), "finalized");
}

TEST_F(UnitTest, Events_Projector_DuplicateJob_DoesNotResetFeedExportedFlag)
{
    const auto paths = makeTempEventsPaths("project_duplicate_job_preserves_exported");
    asio::io_context store_io_context;
    events::SQLiteHotStore store(
        paths.hot_db,
        paths.archive_root,
        7LL * 24 * 60 * 60 * 1000,
        CHAIN_ID);

    const events::DecodedEvent event = makeDecodedEvent(
        140,
        0,
        1,
        0xB7,
        0xD7,
        events::EventType::CONNECTOR_ADDED,
        events::EventState::OBSERVED,
        1'700'020'000);
    const events::ChainBlockInfo block = makeBlockInfo(
        140,
        event.raw.block_hash,
        hexBytes(0x8A, 32),
        1'700'020'000,
        1'700'020'000'100);
    ASSERT_TRUE(awaitIngestBatch(store_io_context, store, CHAIN_ID, {event}, {block}, 141, 1'700'020'000'200));
    EXPECT_EQ(projectAll(store_io_context, store, 1'700'020'000'300), 1u);

    const events::FinalityHeights heights{
        .head = 200,
        .safe = 140,
        .finalized = 140
    };
    ASSERT_TRUE(awaitApplyFinality(store_io_context, store, CHAIN_ID, heights, 1'700'020'000'400, 2048));
    EXPECT_EQ(projectAll(store_io_context, store, 1'700'020'000'500), 1u);

    ASSERT_TRUE(awaitRunArchiveCycle(store_io_context, store, CHAIN_ID, 36500, 1'700'020'010'000));

    {
        SqliteWritable db(paths.hot_db);
        db.exec(std::format(
            "INSERT OR IGNORE INTO projection_jobs(chain_id, block_hash, log_index, created_at_ms) "
            "VALUES(1, '{}', {}, 1700020015000);",
            event.raw.block_hash,
            event.raw.log_index));
    }

    EXPECT_EQ(awaitProjectBatch(store_io_context, store, 32, 1'700'020'020'000), 1u);

    SqliteReadonly db(paths.hot_db);
    EXPECT_EQ(db.scalarInt64("SELECT exported FROM feed_items_hot LIMIT 1;"), 1);
    EXPECT_EQ(db.scalarInt64("SELECT COUNT(1) FROM global_outbox;"), 2);
}

TEST_F(UnitTest, Events_Projector_StreamSeq_RemainsMonotonicAcrossPruneAndRestart)
{
    const auto paths = makeTempEventsPaths("project_stream_seq_monotonic_prune_restart");

    {
        asio::io_context store_io_context;
        events::SQLiteHotStore store(paths.hot_db, paths.archive_root, 10, CHAIN_ID);
        const events::DecodedEvent first =
            makeDecodedEvent(90, 0, 1, 0xC8, 0xE8, events::EventType::CONNECTOR_ADDED, events::EventState::OBSERVED, 1'700'000'900);
        const events::ChainBlockInfo first_block =
            makeBlockInfo(90, first.raw.block_hash, hexBytes(0x70, 32), 1'700'000'900, 1'700'000'900'100);
        ASSERT_TRUE(awaitIngestBatch(store_io_context, store, CHAIN_ID, {first}, {first_block}, 91, 1'700'000'900'200));
        EXPECT_EQ(projectAll(store_io_context, store, 1'700'000'900'300), 1u);

        // Trigger retention prune so the outbox can become empty.
        EXPECT_EQ(awaitProjectBatch(store_io_context, store, 16, 1'700'000'901'000), 0u);
        events_sql::expectRowCount(paths.hot_db, "global_outbox", 0);
    }

    {
        asio::io_context restarted_io_context;
        events::SQLiteHotStore restarted(
            paths.hot_db,
            paths.archive_root,
            10,
            CHAIN_ID);
        const events::DecodedEvent second = makeDecodedEvent(
            91,
            0,
            1,
            0xC9,
            0xE9,
            events::EventType::TRANSFORMATION_ADDED,
            events::EventState::OBSERVED,
            1'700'000'901);
        const events::ChainBlockInfo second_block =
            makeBlockInfo(91, second.raw.block_hash, hexBytes(0x71, 32), 1'700'000'901, 1'700'000'901'100);
        ASSERT_TRUE(awaitIngestBatch(
            restarted_io_context,
            restarted,
            CHAIN_ID,
            {second},
            {second_block},
            92,
            1'700'000'901'200));
        EXPECT_EQ(projectAll(restarted_io_context, restarted, 1'700'000'901'300), 1u);
    }

    SqliteReadonly db(paths.hot_db);
    EXPECT_EQ(db.scalarInt64("SELECT stream_seq FROM global_outbox LIMIT 1;"), 2);
}

TEST_F(UnitTest, Events_Projector_ReplayedLocalEmission_DeduplicatesByTypeNameOwner)
{
    const auto paths = makeTempEventsPaths("project_replayed_local_dedup");
    asio::io_context store_io_context;
    events::SQLiteHotStore store(
        paths.hot_db,
        paths.archive_root,
        7LL * 24 * 60 * 60 * 1000,
        CHAIN_ID);

    events::DecodedEvent first = makeDecodedEvent(
        300,
        0,
        1,
        0xD1,
        0xE1,
        events::EventType::CONNECTOR_ADDED,
        events::EventState::FINALIZED,
        1'700'031'000);
    first.name = "shared_entity";
    first.owner = hexAddress(0x66);

    const events::ChainBlockInfo first_block = makeBlockInfo(
        300,
        first.raw.block_hash,
        hexBytes(0x20, 32),
        1'700'031'000,
        1'700'031'000'100);
    ASSERT_TRUE(awaitIngestBatch(
        store_io_context,
        store,
        CHAIN_ID,
        {first},
        {first_block},
        301,
        1'700'031'000'200));
    EXPECT_EQ(projectAll(store_io_context, store, 1'700'031'000'300), 1u);

    events::DecodedEvent replay = makeDecodedEvent(
        301,
        2,
        7,
        0xD2,
        0xE2,
        events::EventType::CONNECTOR_ADDED,
        events::EventState::FINALIZED,
        1'700'031'100);
    replay.name = first.name;
    replay.owner = first.owner;

    const events::ChainBlockInfo replay_block = makeBlockInfo(
        301,
        replay.raw.block_hash,
        hexBytes(0x21, 32),
        1'700'031'100,
        1'700'031'100'100);
    ASSERT_TRUE(awaitIngestBatch(
        store_io_context,
        store,
        CHAIN_ID,
        {replay},
        {replay_block},
        302,
        1'700'031'100'200));
    EXPECT_EQ(projectAll(store_io_context, store, 1'700'031'100'300), 1u);

    events_sql::expectRowCount(paths.hot_db, "feed_items_hot", 1);
    events_sql::expectRowCount(paths.hot_db, "global_outbox", 1);

    SqliteReadonly db(paths.hot_db);
    EXPECT_EQ(db.scalarText("SELECT tx_hash FROM feed_items_hot LIMIT 1;"), replay.raw.tx_hash);
    EXPECT_EQ(db.scalarInt64("SELECT log_index FROM feed_items_hot LIMIT 1;"), replay.raw.log_index);
    EXPECT_EQ(db.scalarText("SELECT feed_id FROM feed_items_hot LIMIT 1;").rfind("eth:1:", 0), 0u);
}

TEST_F(UnitTest, Events_Projector_OrphanJobs_AreCleanedBeforeProjection)
{
    const auto paths = makeTempEventsPaths("project_orphan_cleanup");
    asio::io_context store_io_context;
    events::SQLiteHotStore store(paths.hot_db, paths.archive_root, 60 * 60 * 1000, CHAIN_ID);

    {
        SqliteWritable db(paths.hot_db);
        db.exec(
            "INSERT INTO projection_jobs(chain_id, block_hash, log_index, created_at_ms) "
            "VALUES(1, '0xdeadbeef', 7, 1700000000000);");
    }

    EXPECT_EQ(awaitProjectBatch(store_io_context, store, 64, 1'700'000'000'500), 0u);
    events_sql::expectRowCount(paths.hot_db, "projection_jobs", 0);
}
