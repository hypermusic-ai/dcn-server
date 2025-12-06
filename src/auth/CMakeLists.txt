add_module(NAME "Auth"
    DEPENDENCIES
        "${PROJECT_PREFIX}::Utils"
        "${PROJECT_PREFIX}::Native"
        "${PROJECT_PREFIX}::Crypto"
        "${PROJECT_PREFIX}::HTTP"
        "${PROJECT_PREFIX}::Parser"
        "${PROJECT_PREFIX}::EVM"
        "${PROJECT_PREFIX}::File"
        "${PROJECT_PREFIX}::PT"

        spdlog::spdlog
        asio
        absl::hash
        secp256k1
        jwt-cpp
        evmc
        evmone
        nlohmann_json
)

