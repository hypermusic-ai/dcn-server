add_module(NAME "API"
    DEPENDENCIES
        "${PROJECT_PREFIX}::Utils"
        "${PROJECT_PREFIX}::Native"
        "${PROJECT_PREFIX}::Version"
        "${PROJECT_PREFIX}::Config"
        "${PROJECT_PREFIX}::File"
        "${PROJECT_PREFIX}::PT"
        "${PROJECT_PREFIX}::HTTP"
        "${PROJECT_PREFIX}::Server"
        "${PROJECT_PREFIX}::Parser"
        "${PROJECT_PREFIX}::Auth"
        "${PROJECT_PREFIX}::EVM"
        "${PROJECT_PREFIX}::Chain"
        "${PROJECT_PREFIX}::Loader"
        "${PROJECT_PREFIX}::Crypto"
        "${PROJECT_PREFIX}::Registry"

        spdlog::spdlog
        asio
        secp256k1
        jwt-cpp
        evmc
        evmone
        nlohmann_json
)

