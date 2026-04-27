include("${PROJECT_SOURCE_DIR}/cmake/add_module.cmake")

add_module(NAME "Storage"
    DEPENDENCIES
        "${PROJECT_PREFIX}::Native"
        "${PROJECT_PREFIX}::Async"

        spdlog::spdlog
        asio
        absl::hash
        absl::flat_hash_map
        sqlite3
)
