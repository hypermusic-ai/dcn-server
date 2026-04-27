#include <format>
#include <stdexcept>

#include "sqlite/statement.hpp"

namespace dcn::storage::sqlite
{
    Statement::Statement(sqlite3 * db, const char * sql)
    {
        if(sqlite3_prepare_v2(db, sql, -1, &_stmt, nullptr) != SQLITE_OK)
        {
            throw std::runtime_error(std::format("sqlite prepare failed: {}", sqlite3_errmsg(db)));
        }
    }

    Statement::~Statement()
    {
        if(_stmt != nullptr)
        {
            sqlite3_finalize(_stmt);
            _stmt = nullptr;
        }
    }

    sqlite3_stmt * Statement::get() const
    {
        return _stmt;
    }

    int Statement::step() const
    {
        return sqlite3_step(_stmt);
    }

    void Statement::reset() const
    {
        sqlite3_reset(_stmt);
        sqlite3_clear_bindings(_stmt);
    }
}