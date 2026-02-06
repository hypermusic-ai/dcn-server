#pragma once

#include <fstream>
#include <filesystem>
#include <optional>

#include <spdlog/spdlog.h>

namespace dcn::file
{
    std::optional<std::string> loadTextFile(std::filesystem::path path);

    std::optional<std::vector<std::byte>> loadBinaryFile(std::filesystem::path path);
}