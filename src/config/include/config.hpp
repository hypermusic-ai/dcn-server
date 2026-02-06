#pragma once
#include <filesystem>

namespace dcn::config
{
    struct Config
    {
        std::filesystem::path bin_path;
        std::filesystem::path logs_path;
        std::filesystem::path resources_path;
        std::filesystem::path storage_path;
    };
}