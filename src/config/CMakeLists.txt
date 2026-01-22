include("${PROJECT_SOURCE_DIR}/cmake/add_module.cmake")

add_module(NAME "Config"
    DEPENDENCIES
        absl::hash
        spdlog::spdlog
        absl::flat_hash_map
)