#pragma once

#include <cstdint>
#include <filesystem>
#include <string>

#include <gtest/gtest.h>

#include "events_test_harness.hpp"

namespace dcn::tests::events_sql
{
    inline void expectRowCount(
        const std::filesystem::path & db_path,
        const std::string & table_name,
        const std::int64_t expected)
    {
        EXPECT_EQ(events_harness::rowCount(db_path, table_name), expected) << table_name;
    }
}
