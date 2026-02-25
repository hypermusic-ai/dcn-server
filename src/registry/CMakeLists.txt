include("${PROJECT_SOURCE_DIR}/cmake/add_module.cmake")

add_module(NAME "Registry"
    DEPENDENCIES
        "${PROJECT_PREFIX}::Native"
        "${PROJECT_PREFIX}::Utils"
        "${PROJECT_PREFIX}::Chain"
        "${PROJECT_PREFIX}::PT"
        "${PROJECT_PREFIX}::File"
        "${PROJECT_PREFIX}::Parser"
        "${PROJECT_PREFIX}::Crypto"
        "${PROJECT_PREFIX}::Chain"
        
        spdlog::spdlog
        asio
        absl::hash
        absl::flat_hash_map
        evmc
        nlohmann_json
)