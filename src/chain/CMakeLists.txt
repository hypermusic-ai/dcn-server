include("${PROJECT_SOURCE_DIR}/cmake/add_module.cmake")

add_module(NAME "Chain"
    DEPENDENCIES
        "${PROJECT_PREFIX}::Native"
        "${PROJECT_PREFIX}::Parser"
        "${PROJECT_PREFIX}::Utils"
        "${PROJECT_PREFIX}::File"
        "${PROJECT_PREFIX}::Crypto"

        absl::hash
        absl::flat_hash_map
        protobuf::libprotobuf
        spdlog::spdlog
        asio
        nlohmann_json
        evmc
)
