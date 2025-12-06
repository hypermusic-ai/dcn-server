include("${PROJECT_SOURCE_DIR}/cmake/add_module.cmake")

add_module(NAME "Server"
    DEPENDENCIES
        "${PROJECT_PREFIX}::Native"
        "${PROJECT_PREFIX}::Utils"
        "${PROJECT_PREFIX}::HTTP"
        "${PROJECT_PREFIX}::Parser"
        absl::hash
        absl::flat_hash_map
        spdlog::spdlog
        asio
)

