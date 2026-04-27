#include <string>

#include <spdlog/spdlog.h>

#include "sqlite/exec.hpp"

namespace dcn::storage::sqlite
{
    bool exec(sqlite3 * db, const char * sql)
    {
        char * err = nullptr;
        const int rc = sqlite3_exec(db, sql, nullptr, nullptr, &err);
        if(rc == SQLITE_OK)
        {
            return true;
        }

        const std::string err_msg = (err == nullptr) ? std::string(sqlite3_errmsg(db)) : std::string(err);
        if(err != nullptr)
        {
            sqlite3_free(err);
        }

        spdlog::error("SQLite exec failed: {} | sql={}", err_msg, sql);
        return false;
    }
}