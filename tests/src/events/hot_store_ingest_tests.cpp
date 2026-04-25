#include "unit-tests.hpp"

#include "events_sql_assertions.hpp"
#include "events_test_harness.hpp"

using namespace dcn;
using namespace dcn::tests;
using namespace dcn::tests::events_harness;

TEST_F(UnitTest, Events_HotStore_IngestSingleEvent_WritesDurableRows)
{
    const auto paths = makeTempEventsPaths("ingest_single");
    asio::io_context store_io_context;
    events::SQLiteHotStore store(
        paths.hot_db,
        paths.archive_root,
        7LL * 24 * 60 * 60 * 1000,
        CHAIN_ID);

    events::DecodedEvent event = makeDecodedEvent(11, 1, 2, 0xA0, 0xB0, events::EventType::CONNECTOR_ADDED, events::EventState::OBSERVED, 1'700'000'000);
    const events::ChainBlockInfo block_info = makeBlockInfo(11, event.raw.block_hash, hexBytes(0x99, 32), 1'700'000'000, 1'700'000'001'000);

    ASSERT_TRUE(awaitIngestBatch(store_io_context, store, CHAIN_ID, {event}, {block_info}, 12, 1'700'000'001'100));

    events_sql::expectRowCount(paths.hot_db, "raw_events_hot", 1);
    events_sql::expectRowCount(paths.hot_db, "normalized_events_hot", 1);
    events_sql::expectRowCount(paths.hot_db, "projection_jobs", 1);
    events_sql::expectRowCount(paths.hot_db, "reorg_window", 1);
    events_sql::expectRowCount(paths.hot_db, "ingest_resume_state", 1);

    SqliteReadonly db(paths.hot_db);
    EXPECT_EQ(db.scalarInt64("SELECT next_from_block FROM ingest_resume_state WHERE chain_id=1;"), 12);
    EXPECT_EQ(db.scalarText("SELECT state FROM raw_events_hot LIMIT 1;"), "observed");
    EXPECT_EQ(db.scalarText("SELECT state FROM normalized_events_hot LIMIT 1;"), "observed");
}

TEST_F(UnitTest, Events_HotStore_UndecodableRawLog_IsDurableAndCursorAdvances)
{
    const auto paths = makeTempEventsPaths("ingest_decode_failure_durable");
    asio::io_context store_io_context;
    events::SQLiteHotStore store(
        paths.hot_db,
        paths.archive_root,
        7LL * 24 * 60 * 60 * 1000,
        CHAIN_ID);

    events::RawChainLog raw = makeRawLog(
        21,
        0,
        2,
        hexBytes(0xA1, 32),
        hexBytes(0xB2, 32),
        "0x12345678",
        "0x01",
        false,
        1'700'000'100,
        1'700'000'101'000);
    raw.topics[1] = hexBytes(0x22, 32);

    const events::ChainBlockInfo block_info = makeBlockInfo(21, raw.block_hash, hexBytes(0x98, 32), 1'700'000'100, 1'700'000'101'000);

    ASSERT_TRUE(awaitIngestBatch(store_io_context, store, CHAIN_ID, {raw}, {}, {block_info}, 22, 1'700'000'101'500));

    events_sql::expectRowCount(paths.hot_db, "raw_events_hot", 1);
    events_sql::expectRowCount(paths.hot_db, "normalized_events_hot", 0);
    events_sql::expectRowCount(paths.hot_db, "projection_jobs", 0);
    events_sql::expectRowCount(paths.hot_db, "decode_failures_hot", 1);

    SqliteReadonly db(paths.hot_db);
    EXPECT_EQ(db.scalarInt64("SELECT next_from_block FROM ingest_resume_state WHERE chain_id=1;"), 22);
    EXPECT_EQ(db.scalarInt64("SELECT attempts FROM decode_failures_hot WHERE chain_id=1;"), 1);

    events::DecodedEvent recovered = makeDecodedEvent(
        21,
        0,
        2,
        0xA1,
        0xB2,
        events::EventType::TRANSFORMATION_ADDED,
        events::EventState::OBSERVED,
        1'700'000'100,
        1'700'000'102'000);
    recovered.raw.block_hash = raw.block_hash;
    recovered.raw.tx_hash = raw.tx_hash;
    recovered.raw.log_index = raw.log_index;
    recovered.raw.block_number = raw.block_number;
    recovered.raw.tx_index = raw.tx_index;
    recovered.raw.address = raw.address;
    recovered.raw.data_hex = raw.data_hex;
    recovered.raw.topics = raw.topics;
    recovered.raw.block_time = raw.block_time;
    recovered.raw.parent_hash = raw.parent_hash;
    recovered.raw.removed = false;

    ASSERT_TRUE(awaitIngestBatch(store_io_context, store, CHAIN_ID, {raw}, {recovered}, {block_info}, 23, 1'700'000'102'100));
    events_sql::expectRowCount(paths.hot_db, "decode_failures_hot", 0);
    events_sql::expectRowCount(paths.hot_db, "normalized_events_hot", 1);
}

TEST_F(UnitTest, Events_HotStore_DuplicateIngest_IsIdempotent)
{
    const auto paths = makeTempEventsPaths("ingest_idempotent");
    asio::io_context store_io_context;
    events::SQLiteHotStore store(
        paths.hot_db,
        paths.archive_root,
        7LL * 24 * 60 * 60 * 1000,
        CHAIN_ID);

    const events::DecodedEvent event = makeDecodedEvent(31, 3, 4, 0xA2, 0xB3, events::EventType::TRANSFORMATION_ADDED, events::EventState::OBSERVED, 1'700'000'200);
    const events::ChainBlockInfo block_info = makeBlockInfo(31, event.raw.block_hash, hexBytes(0x97, 32), 1'700'000'200, 1'700'000'201'000);

    ASSERT_TRUE(awaitIngestBatch(store_io_context, store, CHAIN_ID, {event}, {block_info}, 32, 1'700'000'201'100));
    ASSERT_TRUE(awaitIngestBatch(store_io_context, store, CHAIN_ID, {event}, {block_info}, 32, 1'700'000'201'200));

    events_sql::expectRowCount(paths.hot_db, "raw_events_hot", 1);
    events_sql::expectRowCount(paths.hot_db, "normalized_events_hot", 1);
    events_sql::expectRowCount(paths.hot_db, "projection_jobs", 1);
}

TEST_F(UnitTest, Events_HotStore_ResumeStateAndLocalSeq_PersistAcrossRestart)
{
    const auto paths = makeTempEventsPaths("ingest_resume_local_seq");

    {
        asio::io_context store_io_context;
        events::SQLiteHotStore store(
            paths.hot_db,
            paths.archive_root,
            7LL * 24 * 60 * 60 * 1000,
            CHAIN_ID);
        const events::DecodedEvent event = makeDecodedEvent(41, 0, 1, 0xA4, 0xB4, events::EventType::CONDITION_ADDED, events::EventState::OBSERVED, 1'700'000'300);
        const events::ChainBlockInfo block_info = makeBlockInfo(41, event.raw.block_hash, hexBytes(0x96, 32), 1'700'000'300, 1'700'000'301'000);
        ASSERT_TRUE(awaitIngestBatch(store_io_context, store, CHAIN_ID, {event}, {block_info}, 42, 1'700'000'301'200));
        ASSERT_TRUE(awaitSaveNextLocalSeq(store_io_context, store, CHAIN_ID, 555, 1'700'000'301'300));
    }

    {
        asio::io_context store_io_context;
        events::SQLiteHotStore store(
            paths.hot_db,
            paths.archive_root,
            7LL * 24 * 60 * 60 * 1000,
            CHAIN_ID);
        const auto next_block = awaitLoadNextFromBlock(store_io_context, store, CHAIN_ID);
        ASSERT_TRUE(next_block.has_value());
        EXPECT_EQ(*next_block, 42);

        const auto next_seq = awaitLoadNextLocalSeq(store_io_context, store, CHAIN_ID);
        ASSERT_TRUE(next_seq.has_value());
        EXPECT_EQ(*next_seq, 555u);
    }
}

TEST_F(UnitTest, Events_HotStore_LocalResumeState_CommitsWithDurableIngest)
{
    const auto paths = makeTempEventsPaths("ingest_local_seq_txn");

    {
        asio::io_context store_io_context;
        events::SQLiteHotStore store(
            paths.hot_db,
            paths.archive_root,
            7LL * 24 * 60 * 60 * 1000,
            CHAIN_ID);
        const events::DecodedEvent event = makeDecodedEvent(
            42,
            0,
            1,
            0xA5,
            0xB5,
            events::EventType::CONNECTOR_ADDED,
            events::EventState::OBSERVED,
            1'700'000'320);
        const events::ChainBlockInfo block_info =
            makeBlockInfo(42, event.raw.block_hash, hexBytes(0x95, 32), 1'700'000'320, 1'700'000'321'000);
        ASSERT_TRUE(awaitIngestBatch(
            store_io_context,
            store,
            CHAIN_ID,
            {event.raw},
            {event},
            {block_info},
            43,
            1'700'000'321'200,
            777));
    }

    {
        asio::io_context restarted_io_context;
        events::SQLiteHotStore restarted(
            paths.hot_db,
            paths.archive_root,
            7LL * 24 * 60 * 60 * 1000,
            CHAIN_ID);
        const auto next_block = awaitLoadNextFromBlock(restarted_io_context, restarted, CHAIN_ID);
        ASSERT_TRUE(next_block.has_value());
        EXPECT_EQ(*next_block, 43);

        const auto next_seq = awaitLoadNextLocalSeq(restarted_io_context, restarted, CHAIN_ID);
        ASSERT_TRUE(next_seq.has_value());
        EXPECT_EQ(*next_seq, 777u);
    }
}

TEST_F(UnitTest, Events_HotStore_ReorgReplacement_MarksOldRowsRemovedAndQueuesJobs)
{
    const auto paths = makeTempEventsPaths("ingest_reorg_replace");
    asio::io_context store_io_context;
    events::SQLiteHotStore store(
        paths.hot_db,
        paths.archive_root,
        7LL * 24 * 60 * 60 * 1000,
        CHAIN_ID);

    events::DecodedEvent old_event = makeDecodedEvent(50, 0, 1, 0xAA, 0xC1, events::EventType::CONNECTOR_ADDED, events::EventState::OBSERVED, 1'700'000'400);
    const events::ChainBlockInfo old_block = makeBlockInfo(50, old_event.raw.block_hash, hexBytes(0x95, 32), 1'700'000'400, 1'700'000'401'000);
    ASSERT_TRUE(awaitIngestBatch(store_io_context, store, CHAIN_ID, {old_event}, {old_block}, 51, 1'700'000'401'100));
    EXPECT_EQ(projectAll(store_io_context, store, 1'700'000'401'200), 1u);

    events::DecodedEvent replacement = makeDecodedEvent(50, 1, 2, 0xAB, 0xC2, events::EventType::CONNECTOR_ADDED, events::EventState::OBSERVED, 1'700'000'401);
    const events::ChainBlockInfo replacement_block = makeBlockInfo(50, replacement.raw.block_hash, hexBytes(0x94, 32), 1'700'000'401, 1'700'000'402'000);
    ASSERT_TRUE(awaitIngestBatch(
        store_io_context,
        store,
        CHAIN_ID,
        {replacement},
        {replacement_block},
        51,
        1'700'000'402'100));

    SqliteReadonly db(paths.hot_db);
    EXPECT_EQ(
        db.scalarText(std::format(
            "SELECT state FROM normalized_events_hot WHERE chain_id=1 AND block_hash='{}' AND log_index={};",
            old_event.raw.block_hash,
            old_event.raw.log_index)),
        "removed");
    EXPECT_EQ(
        db.scalarInt64(std::format(
            "SELECT removed FROM raw_events_hot WHERE chain_id=1 AND block_hash='{}' AND log_index={};",
            old_event.raw.block_hash,
            old_event.raw.log_index)),
        1);
    EXPECT_EQ(db.scalarInt64("SELECT COUNT(1) FROM projection_jobs;"), 2);
    EXPECT_EQ(
        db.scalarText("SELECT block_hash FROM reorg_window WHERE chain_id=1 AND block_number=50;"),
        replacement.raw.block_hash);
}

TEST_F(UnitTest, Events_HotStore_RemovedRawWithoutDecodedEvent_StillInvalidatesNormalizedFeedState)
{
    const auto paths = makeTempEventsPaths("ingest_removed_raw_without_decoded");
    asio::io_context store_io_context;
    events::SQLiteHotStore store(
        paths.hot_db,
        paths.archive_root,
        7LL * 24 * 60 * 60 * 1000,
        CHAIN_ID);

    const events::DecodedEvent inserted = makeDecodedEvent(
        75,
        0,
        1,
        0xBC,
        0xCD,
        events::EventType::TRANSFORMATION_ADDED,
        events::EventState::OBSERVED,
        1'700'003'100);
    const events::ChainBlockInfo block_info = makeBlockInfo(
        75,
        inserted.raw.block_hash,
        hexBytes(0x66, 32),
        1'700'003'100,
        1'700'003'100'100);

    ASSERT_TRUE(awaitIngestBatch(store_io_context, store, CHAIN_ID, {inserted}, {block_info}, 76, 1'700'003'100'200));
    EXPECT_EQ(projectAll(store_io_context, store, 1'700'003'100'300), 1u);

    events::RawChainLog removed_raw = inserted.raw;
    removed_raw.removed = true;
    removed_raw.seen_at_ms = 1'700'003'100'400;

    ASSERT_TRUE(awaitIngestBatch(
        store_io_context,
        store,
        CHAIN_ID,
        {removed_raw},
        {},
        {block_info},
        76,
        1'700'003'100'500));

    EXPECT_EQ(projectAll(store_io_context, store, 1'700'003'100'600), 1u);

    SqliteReadonly db(paths.hot_db);
    EXPECT_EQ(db.scalarText("SELECT state FROM raw_events_hot LIMIT 1;"), "removed");
    EXPECT_EQ(db.scalarText("SELECT state FROM normalized_events_hot LIMIT 1;"), "removed");
    EXPECT_EQ(db.scalarText("SELECT status FROM feed_items_hot LIMIT 1;"), "removed");
    EXPECT_EQ(db.scalarInt64("SELECT visible FROM feed_items_hot LIMIT 1;"), 0);
}

TEST_F(UnitTest, Events_HotStore_RemovedRawWithDecodedObserved_StillPinsRawStateToRemoved)
{
    const auto paths = makeTempEventsPaths("ingest_removed_raw_with_decoded_observed");
    asio::io_context store_io_context;
    events::SQLiteHotStore store(
        paths.hot_db,
        paths.archive_root,
        7LL * 24 * 60 * 60 * 1000,
        CHAIN_ID);

    const events::DecodedEvent inserted = makeDecodedEvent(
        88,
        0,
        1,
        0xAD,
        0xBD,
        events::EventType::CONNECTOR_ADDED,
        events::EventState::OBSERVED,
        1'700'004'000);
    const events::ChainBlockInfo block_info = makeBlockInfo(
        88,
        inserted.raw.block_hash,
        hexBytes(0x65, 32),
        1'700'004'000,
        1'700'004'000'100);

    ASSERT_TRUE(awaitIngestBatch(store_io_context, store, CHAIN_ID, {inserted}, {block_info}, 89, 1'700'004'000'200));
    EXPECT_EQ(projectAll(store_io_context, store, 1'700'004'000'300), 1u);

    events::RawChainLog removed_raw = inserted.raw;
    removed_raw.removed = true;
    removed_raw.seen_at_ms = 1'700'004'000'400;

    events::DecodedEvent decoded_observed = inserted;
    decoded_observed.raw = removed_raw;
    decoded_observed.state = events::EventState::OBSERVED;

    ASSERT_TRUE(awaitIngestBatch(
        store_io_context,
        store,
        CHAIN_ID,
        {removed_raw},
        {decoded_observed},
        {block_info},
        89,
        1'700'004'000'500));

    const events::FinalityHeights heights{
        .head = 200,
        .safe = 88,
        .finalized = 88
    };
    ASSERT_TRUE(awaitApplyFinality(store_io_context, store, CHAIN_ID, heights, 1'700'004'000'600, 2048));

    SqliteReadonly db(paths.hot_db);
    EXPECT_EQ(db.scalarText("SELECT state FROM raw_events_hot LIMIT 1;"), "removed");
    EXPECT_EQ(db.scalarText("SELECT state FROM normalized_events_hot LIMIT 1;"), "removed");
}
