#include "unit-tests.hpp"

#include "events_test_harness.hpp"

#include <sqlite3.h>

using namespace dcn;
using namespace dcn::tests;
using namespace dcn::tests::events_harness;

namespace
{
    std::string sqlitePragmaText(const std::filesystem::path & db_path, const std::string & pragma_name)
    {
        sqlite3 * db = nullptr;
        const int open_rc = sqlite3_open_v2(
            db_path.string().c_str(),
            &db,
            SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_FULLMUTEX,
            nullptr);
        if(open_rc != SQLITE_OK)
        {
            if(db != nullptr)
            {
                sqlite3_close(db);
            }
            return {};
        }

        sqlite3_stmt * stmt = nullptr;
        const std::string sql = std::format("PRAGMA {};", pragma_name);
        if(sqlite3_prepare_v2(db, sql.c_str(), -1, &stmt, nullptr) != SQLITE_OK)
        {
            sqlite3_close(db);
            return {};
        }

        std::string value;
        if(sqlite3_step(stmt) == SQLITE_ROW)
        {
            const unsigned char * txt = sqlite3_column_text(stmt, 0);
            if(txt != nullptr)
            {
                value = reinterpret_cast<const char *>(txt);
            }
        }

        sqlite3_finalize(stmt);
        sqlite3_close(db);
        return value;
    }
}

TEST_F(UnitTest, Events_Maintenance_ConfiguresWalJournalMode)
{
    const auto paths = makeTempEventsPaths("maintenance_wal_mode");
    asio::io_context store_io_context;
    events::SQLiteHotStore store(paths.hot_db, paths.archive_root, 60 * 60 * 1000, CHAIN_ID);

    const std::string journal_mode = sqlitePragmaText(paths.hot_db, "journal_mode");
    EXPECT_EQ(journal_mode, "wal");
}

TEST_F(UnitTest, Events_Maintenance_RejectsLikelyNetworkFilesystemPath)
{
    const auto local_paths = makeTempEventsPaths("maintenance_network_reject");
    const std::filesystem::path archive_root = local_paths.archive_root / "archive";

    EXPECT_THROW(
        {
            asio::io_context store_io_context;
            events::SQLiteHotStore store(
                "//server/share/events_hot.sqlite",
                archive_root,
                1000,
                CHAIN_ID);
        },
        std::runtime_error);
}

TEST_F(UnitTest, Events_Maintenance_WalCheckpointRunsAndReportsStats)
{
    const auto paths = makeTempEventsPaths("maintenance_wal_checkpoint");
    asio::io_context store_io_context;
    events::SQLiteHotStore store(paths.hot_db, paths.archive_root, 60 * 60 * 1000, CHAIN_ID);

    const events::DecodedEvent event =
        makeDecodedEvent(201, 0, 1, 0xAA, 0xBA, events::EventType::CONNECTOR_ADDED, events::EventState::OBSERVED, 1'700'010'000);
    const events::ChainBlockInfo block =
        makeBlockInfo(201, event.raw.block_hash, hexBytes(0xA9, 32), 1'700'010'000, 1'700'010'000'100);
    ASSERT_TRUE(awaitIngestBatch(store_io_context, store, CHAIN_ID, {event}, {block}, 202, 1'700'010'000'200));
    EXPECT_EQ(projectAll(store_io_context, store, 1'700'010'000'300), 1u);

    const storage::sqlite::WalCheckpointStats passive = awaitCheckpointWalPassive(store_io_context, store);
    EXPECT_TRUE(passive.ok);
    EXPECT_GE(passive.log_frames, 0);
    EXPECT_GE(passive.checkpointed_frames, 0);

    const storage::sqlite::WalCheckpointStats truncate = awaitCheckpointWalTruncate(store_io_context, store);
    EXPECT_TRUE(truncate.ok);
    EXPECT_GE(truncate.wal_bytes, 0u);
}
