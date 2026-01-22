#include "file.hpp"

namespace dcn::file
{
    std::optional<std::string> loadTextFile(std::filesystem::path path)
    {
        if(std::filesystem::exists(path) == false)
        {
            spdlog::error(std::format("Cannot find {} in the resources directory.", path.string()));
            return std::nullopt;
        }
        
        std::ifstream file(path, std::ios::in);

        if(file.good() == false)
        {  
            spdlog::error(std::format("Failed to open file {}", path.string()));
            return std::nullopt;
        }

        const std::string file_content = std::string((std::istreambuf_iterator<char>(file)), std::istreambuf_iterator<char>());

        file.close();

        return file_content;
    }

    std::optional<std::vector<std::byte>> loadBinaryFile(std::filesystem::path path)
    {    
        if (!std::filesystem::exists(path)) {
            spdlog::error("Cannot find {} file.", path.string());
            return std::nullopt;
        }
    
        std::ifstream file(path, std::ios::binary | std::ios::ate);
        if (!file) {
            spdlog::error("Failed to open file {}", path.string());
            return std::nullopt;
        }
    
        const std::streamsize size = file.tellg();
        if (size <= 0) {
            spdlog::error("File {} is empty or unreadable.", path.string());
            return std::nullopt;
        }
    
        std::vector<std::byte> buffer(static_cast<std::size_t>(size));
        file.seekg(0);
        if (!file.read(reinterpret_cast<char*>(buffer.data()), size)) {
            spdlog::error("Failed to read file {}", path.string());
            return std::nullopt;
        }
    
        return buffer;
    }
}