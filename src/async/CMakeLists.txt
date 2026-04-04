include("${PROJECT_SOURCE_DIR}/cmake/add_module.cmake")

add_module(NAME "Async"
    DEPENDENCIES
        "${PROJECT_PREFIX}::Native"
        asio

        spdlog::spdlog
)