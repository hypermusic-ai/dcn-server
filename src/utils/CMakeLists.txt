include("${PROJECT_SOURCE_DIR}/cmake/add_module.cmake")

add_module(NAME "Utils"
    DEPENDENCIES
        spdlog::spdlog
        absl::hash
        absl::flat_hash_map 
)