add_module(NAME "Auth"
    DEPENDENCIES
        "${PROJECT_PREFIX}::Native"
        "${PROJECT_PREFIX}::Async"
        "${PROJECT_PREFIX}::Crypto"
        "${PROJECT_PREFIX}::Chain"

        spdlog::spdlog
        asio
        absl::hash
        secp256k1
        jwt-cpp
        evmc
        nlohmann_json
)

