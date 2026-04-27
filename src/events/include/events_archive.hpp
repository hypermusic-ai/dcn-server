#pragma once

#include <cstddef>
#include <cstdint>

namespace dcn::events
{
    class IArchiveManager
    {
        public:
            virtual ~IArchiveManager() = default;
            virtual bool runCycle(int chain_id, std::size_t hot_window_days, std::int64_t now_ms) = 0;
    };
}
