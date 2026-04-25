#pragma once

#include <sqlite3.h>

namespace dcn::storage::sqlite
{
    class Statement final
    {
        public:

            Statement(sqlite3 * db, const char * sql);
            Statement(const Statement &) = delete;
            Statement & operator=(const Statement &) = delete;

            ~Statement();

            sqlite3_stmt * get() const;
            
            int step() const;

            void reset() const;

        private:
            sqlite3_stmt * _stmt = nullptr;
    };
}