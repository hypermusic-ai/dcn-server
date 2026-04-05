#pragma once
#include <filesystem>
#include <cstdint>
#include <string>

namespace dcn::config
{
    struct IngestionConfig
    {
        bool enabled = false;

        std::string rpc_url;
        std::string registry_address;

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
    };
}