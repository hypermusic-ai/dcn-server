#pragma once

#include <cstdint>
#include <string>

namespace dcn::storage::sqlite
{
    enum class WalCheckpointMode : std::uint8_t
    {
        PASSIVE,
        FULL,
        TRUNCATE
    };

    struct WalCheckpointStats
    {
        bool ok = false;
        int busy = 0;
        int log_frames = 0;
        int checkpointed_frames = 0;
        std::uintmax_t wal_bytes = 0;
    };

    std::string checkpointModeToString(const WalCheckpointMode mode);

    int toSqliteCheckpointMode(const WalCheckpointMode mode);
}