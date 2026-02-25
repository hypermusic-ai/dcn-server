include("${PROJECT_SOURCE_DIR}/cmake/add_module.cmake")

add_module(NAME "Sepolia"
    DEPENDENCIES
        "${PROJECT_PREFIX}::Native"
        "${PROJECT_PREFIX}::Crypto"
        "${PROJECT_PREFIX}::Chain"

        asio
        nlohmann_json
        secp256k1
        evmc
)
