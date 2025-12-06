add_module(NAME "EVM"
    DEPENDENCIES
        "${PROJECT_PREFIX}::Utils"
        "${PROJECT_PREFIX}::Native"
        "${PROJECT_PREFIX}::Crypto"
        "${PROJECT_PREFIX}::File"
        "${PROJECT_PREFIX}::PT"
        "${PROJECT_PREFIX}::Parser"

        absl::hash
        absl::flat_hash_map
        spdlog::spdlog
        asio
        evmc
        evmone
        nlohmann_json
)

add_dependencies("EVM" solc_imported evmone)
target_compile_definitions("EVM" PUBLIC Solidity_SOLC_EXECUTABLE=${Solidity_SOLC_EXECUTABLE})
