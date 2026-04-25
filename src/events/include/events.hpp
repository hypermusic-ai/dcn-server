#pragma once

#include <cstdint>
#include <filesystem>
#include <memory>
#include <optional>
#include <string>

#include "native.h"
#include <asio.hpp>

#include "events_shard.hpp"
#include "events_archive.hpp"
#include "events_feed.hpp"
#include "events_ingest.hpp"
#include "events_store.hpp"
#include "events_hot_store.hpp"

namespace dcn::evm
{
    class EVM;
}

namespace dcn::events
{
    constexpr std::size_t DEFAULT_PROJECT_BATCH_SIZE = 256;


    struct EventRuntimeConfig
    {
        std::filesystem::path hot_db_path;
        std::filesystem::path archive_root;

        int chain_id = 1;
        bool ingestion_enabled = false;
        bool use_local_evm_source = false;
        evm::EVM * local_evm = nullptr;
        std::string rpc_url;
        std::string registry_address;
        std::optional<std::int64_t> start_block = std::nullopt;
        unsigned int poll_interval_ms = 5000;
        unsigned int confirmations = 12;
        unsigned int block_batch_size = 500;

        std::size_t hot_window_days = 90;
        std::size_t reorg_window_blocks = 2048;
        std::int64_t outbox_retention_ms = 7LL * 24 * 60 * 60 * 1000;

        unsigned int projector_interval_ms = 200;
        unsigned int archive_interval_ms = 30 * 1000;
    };

    class EventRuntime final : public IFeedRepository
    {
        public:
            EventRuntime(asio::io_context & io_context, EventRuntimeConfig config);
            ~EventRuntime();

            EventRuntime(const EventRuntime &) = delete;
            EventRuntime & operator=(const EventRuntime &) = delete;

            EventRuntime(EventRuntime &&) = delete;
            EventRuntime & operator=(EventRuntime &&) = delete;

            void start();
            void requestStop();
            bool running() const;
            bool ingestionEnabled() const;

            FeedPage getFeedPage(const FeedQuery & query) const override;
            StreamPage getStreamPage(const StreamQuery & query) const override;
            std::int64_t minAvailableStreamSeq() const override;

        private:
            class Impl;
            std::unique_ptr<Impl> _impl;
    };
}
