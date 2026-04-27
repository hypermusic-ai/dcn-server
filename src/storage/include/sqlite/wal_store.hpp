#pragma once

#include "async.hpp"

#include "wal.hpp"

namespace dcn::storage::sqlite
{
    class IWalStore
    {
    public:
        virtual ~IWalStore() = default;

        virtual asio::awaitable<WalCheckpointStats> checkpointWal(WalCheckpointMode mode) const = 0;
    };
}