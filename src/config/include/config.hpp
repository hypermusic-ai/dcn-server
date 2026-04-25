#pragma once
#include <cstdint>
#include <filesystem>
#include <optional>
#include <string>

namespace dcn::config
{
    struct IngestionConfig
    {
        bool enabled = false;
        bool use_local_evm_source = false;

        std::string rpc_url;
        std::string registry_address;
        std::optional<std::uint64_t> start_block = std::nullopt;

        unsigned int poll_interval_ms;
        unsigned int confirmations;
        unsigned int block_batch_size;
    };

    struct Config
    {
        std::filesystem::path bin_path;
        std::filesystem::path logs_path;
        std::filesystem::path resources_path;
        std::filesystem::path storage_path;

        bool verbose;

        std::uint32_t port;

        unsigned int loader_batch_connectors;
        unsigned int loader_batch_transformations;
        unsigned int loader_batch_conditions;

        IngestionConfig chain_ingestion;

        unsigned int registry_wal_sync_ms;
        std::filesystem::path registry_db;

        std::filesystem::path events_db;
        std::filesystem::path events_archive_root;
        unsigned int events_chain_id = 1;
        unsigned int events_hot_window_days = 90;
        unsigned int events_projector_interval_ms = 200;
        unsigned int events_archive_interval_ms = 30000;
        unsigned int events_reorg_window_blocks = 2048;
        unsigned int events_outbox_retention_days = 7;
    };
}
