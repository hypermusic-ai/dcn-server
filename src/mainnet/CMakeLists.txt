include("${PROJECT_SOURCE_DIR}/cmake/add_module.cmake")

add_module(NAME "Mainnet"
    DEPENDENCIES
        "${PROJECT_PREFIX}::Native"
        "${PROJECT_PREFIX}::Registry"
        "${PROJECT_PREFIX}::Parser"
        "${PROJECT_PREFIX}::EVM"
        "${PROJECT_PREFIX}::Utils"
        "${PROJECT_PREFIX}::File"
        "${PROJECT_PREFIX}::Crypto"
        "${PROJECT_PREFIX}::PT"

        absl::hash
        absl::flat_hash_map
        spdlog::spdlog
        asio
        nlohmann_json
        evmc
        evmone
)
