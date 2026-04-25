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
            events::SQLiteHotStore store("//server/share/events_hot.sqlite", archive_root, 1000, CHAIN_ID);
        },
        std::runtime_error);
}
