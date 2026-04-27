#pragma once

#include <sqlite3.h>

namespace dcn::storage::sqlite
{
    bool exec(sqlite3 * db, const char * sql);
}