# Decentralised Art server

[![Docs](https://img.shields.io/badge/docs-online-blue)](https://hypermusic-ai.github.io/dcn-server/)

[View docs](https://hypermusic-ai.github.io/dcn-server/)

---

## Dependencies

This project requires the following libraries:

### Installed on the machine

- [**OpenSSL**](https://www.openssl.org/)
- [**npm**](https://www.npmjs.com/)

### Fetched automatically

- [**Asio**](https://think-async.com/Asio/)
- [**spdlog**](https://github.com/gabime/spdlog)
- [**cURL**](https://curl.se/)
- [**abseil**](https://github.com/abseil/abseil-cpp)
- [**Protobuf**](https://protobuf.dev/)
- [**jwt-cpp**](https://github.com/Thalhammer/jwt-cpp)
- [**secp256k1**](https://github.com/bitcoin-core/secp256k1)
- [**solc**](https://github.com/ethereum/solidity)
- [**evmc**](https://github.com/ethereum/evmc)
- [**evmone**](https://github.com/ethereum/evmone)

#### Testing

- [**GTest**](https://github.com/google/googletest)

---

## Configuring

To configure the project using CMake, run the following command:

```sh
cmake -S . -B build -DCMAKE_INSTALL_PREFIX=install -DDECENTRALIZED_ART_BUILD_TESTS=ON
```

- `-S .`: Specifies the source directory.
- `-B build`: Specifies the build directory.
- `-DCMAKE_INSTALL_PREFIX=install`: Specifies `<INSTALL_DIRECTORY>`.
- `-DDECENTRALIZED_ART_BUILD_TESTS=ON`: Enables tests.
- `-DDECENTRALIZED_ART_BUILD_STRESS_TESTS=ON`: Enables stress tests.
- `-DDECENTRALISED_ART_USE_SUBMODULE_PT=ON`: Enables use of local PT submodule.

---

## Building

|type|command|
|---|---|
|Debug|`cmake --build build --config Debug`|
|Release|`cmake --build build --config Release`|
|RelWithDebInfo|`cmake --build build --config RelWithDebInfo`|

## Installing

To install project in **Debug Mode**, use:

|type|command|
|---|---|
|Debug|`cmake --build build --config Debug --target install`|
|Release|`cmake --build build --config Release --target install`|
|RelWithDebInfo|`cmake --build build --config RelWithDebInfo --target install`|

---

## Start the server

```sh
<INSTALL_DIRECTORY>/bin/DecentralisedArtServer.exe
```

---
📜 **License**:

👨‍💻 **Contributors**: [Sawyer](https://github.com/MisterSawyer)

💡 **Additional Notes**:
