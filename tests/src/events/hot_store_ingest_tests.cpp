#include "unit-tests.hpp"

#include "events_sql_assertions.hpp"
#include "events_test_harness.hpp"

using namespace dcn;
using namespace dcn::tests;
using namespace dcn::tests::events_harness;

TEST_F(UnitTest, Events_HotStore_IngestSingleEvent_WritesDurableRows)
{
    const auto paths = makeTempEventsPaths("ingest_single");
    events::SQLiteHotStore store(paths.hot_db, paths.archive_root, 7LL * 24 * 60 * 60 * 1000, CHAIN_ID);

    events::DecodedEvent event = makeDecodedEvent(11, 1, 2, 0xA0, 0xB0, events::EventType::CONNECTOR_ADDED, events::EventState::OBSERVED, 1'700'000'000);
    const events::ChainBlockInfo block_info = makeBlockInfo(11, event.raw.block_hash, hexBytes(0x99, 32), 1'700'000'000, 1'700'000'001'000);

    ASSERT_TRUE(store.ingestBatch(CHAIN_ID, {event}, {block_info}, 12, 1'700'000'001'100));

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

TEST_F(UnitTest, Events_HotStore_IngestBatch_RollsBackAtomicallyOnFailure)
{
    const auto paths = makeTempEventsPaths("ingest_atomic_rollback");
    events::SQLiteHotStore store(paths.hot_db, paths.archive_root, 7LL * 24 * 60 * 60 * 1000, CHAIN_ID);

    events::DecodedEvent valid = makeDecodedEvent(21, 0, 1, 0xA1, 0xB1, events::EventType::CONNECTOR_ADDED, events::EventState::OBSERVED, 1'700'000'100);
    events::DecodedEvent invalid = makeDecodedEvent(21, 0, 2, 0xA1, 0xB2, events::EventType::TRANSFORMATION_ADDED, events::EventState::OBSERVED, 1'700'000'100);
    invalid.raw.topics[0] = std::nullopt;

    const events::ChainBlockInfo block_info = makeBlockInfo(21, valid.raw.block_hash, hexBytes(0x98, 32), 1'700'000'100, 1'700'000'101'000);

    EXPECT_FALSE(store.ingestBatch(CHAIN_ID, {valid, invalid}, {block_info}, 22, 1'700'000'101'500));

    events_sql::expectRowCount(paths.hot_db, "raw_events_hot", 0);
    events_sql::expectRowCount(paths.hot_db, "normalized_events_hot", 0);
    events_sql::expectRowCount(paths.hot_db, "projection_jobs", 0);
    events_sql::expectRowCount(paths.hot_db, "ingest_resume_state", 0);
    events_sql::expectRowCount(paths.hot_db, "reorg_window", 0);
}

TEST_F(UnitTest, Events_HotStore_DuplicateIngest_IsIdempotent)
{
    const auto paths = makeTempEventsPaths("ingest_idempotent");
    events::SQLiteHotStore store(paths.hot_db, paths.archive_root, 7LL * 24 * 60 * 60 * 1000, CHAIN_ID);

    const events::DecodedEvent event = makeDecodedEvent(31, 3, 4, 0xA2, 0xB3, events::EventType::TRANSFORMATION_ADDED, events::EventState::OBSERVED, 1'700'000'200);
    const events::ChainBlockInfo block_info = makeBlockInfo(31, event.raw.block_hash, hexBytes(0x97, 32), 1'700'000'200, 1'700'000'201'000);

    ASSERT_TRUE(store.ingestBatch(CHAIN_ID, {event}, {block_info}, 32, 1'700'000'201'100));
    ASSERT_TRUE(store.ingestBatch(CHAIN_ID, {event}, {block_info}, 32, 1'700'000'201'200));

    events_sql::expectRowCount(paths.hot_db, "raw_events_hot", 1);
    events_sql::expectRowCount(paths.hot_db, "normalized_events_hot", 1);
    events_sql::expectRowCount(paths.hot_db, "projection_jobs", 1);
}

TEST_F(UnitTest, Events_HotStore_ResumeStateAndLocalSeq_PersistAcrossRestart)
{
    const auto paths = makeTempEventsPaths("ingest_resume_local_seq");

    {
        events::SQLiteHotStore store(paths.hot_db, paths.archive_root, 7LL * 24 * 60 * 60 * 1000, CHAIN_ID);
        const events::DecodedEvent event = makeDecodedEvent(41, 0, 1, 0xA4, 0xB4, events::EventType::CONDITION_ADDED, events::EventState::OBSERVED, 1'700'000'300);
        const events::ChainBlockInfo block_info = makeBlockInfo(41, event.raw.block_hash, hexBytes(0x96, 32), 1'700'000'300, 1'700'000'301'000);
        ASSERT_TRUE(store.ingestBatch(CHAIN_ID, {event}, {block_info}, 42, 1'700'000'301'200));
        ASSERT_TRUE(store.saveNextLocalSeq(CHAIN_ID, 555, 1'700'000'301'300));
    }

    {
        events::SQLiteHotStore store(paths.hot_db, paths.archive_root, 7LL * 24 * 60 * 60 * 1000, CHAIN_ID);
        const auto next_block = store.loadNextFromBlock(CHAIN_ID);
        ASSERT_TRUE(next_block.has_value());
        EXPECT_EQ(*next_block, 42);

        const auto next_seq = store.loadNextLocalSeq(CHAIN_ID);
        ASSERT_TRUE(next_seq.has_value());
        EXPECT_EQ(*next_seq, 555u);
    }
}

TEST_F(UnitTest, Events_HotStore_ReorgReplacement_MarksOldRowsRemovedAndQueuesJobs)
{
    const auto paths = makeTempEventsPaths("ingest_reorg_replace");
    events::SQLiteHotStore store(paths.hot_db, paths.archive_root, 7LL * 24 * 60 * 60 * 1000, CHAIN_ID);

    events::DecodedEvent old_event = makeDecodedEvent(50, 0, 1, 0xAA, 0xC1, events::EventType::CONNECTOR_ADDED, events::EventState::OBSERVED, 1'700'000'400);
    const events::ChainBlockInfo old_block = makeBlockInfo(50, old_event.raw.block_hash, hexBytes(0x95, 32), 1'700'000'400, 1'700'000'401'000);
    ASSERT_TRUE(store.ingestBatch(CHAIN_ID, {old_event}, {old_block}, 51, 1'700'000'401'100));
    EXPECT_EQ(projectAll(store, 1'700'000'401'200), 1u);

    events::DecodedEvent replacement = makeDecodedEvent(50, 1, 2, 0xAB, 0xC2, events::EventType::CONNECTOR_ADDED, events::EventState::OBSERVED, 1'700'000'401);
    const events::ChainBlockInfo replacement_block = makeBlockInfo(50, replacement.raw.block_hash, hexBytes(0x94, 32), 1'700'000'401, 1'700'000'402'000);
    ASSERT_TRUE(store.ingestBatch(CHAIN_ID, {replacement}, {replacement_block}, 51, 1'700'000'402'100));

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
