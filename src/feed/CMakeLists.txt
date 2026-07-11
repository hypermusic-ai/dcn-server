include("${PROJECT_SOURCE_DIR}/cmake/add_module.cmake")

add_module(NAME "Feed"
    DEPENDENCIES
        "${PROJECT_PREFIX}::Native"
        "${PROJECT_PREFIX}::Utils"
        "${PROJECT_PREFIX}::Async"
        "${PROJECT_PREFIX}::Chain"
        "${PROJECT_PREFIX}::PT"
        "${PROJECT_PREFIX}::Parser"
        "${PROJECT_PREFIX}::Storage"
        "${PROJECT_PREFIX}::Events"

        spdlog::spdlog
        asio
        evmc
        sqlite3
        nlohmann_json
)
