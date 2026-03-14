# AGENTS.md

## Scope

Instructions in this file apply to the repository root (`dcn-server-cpp`) and all subdirectories.

## Build Directory Policy

Agents must use a build directory for all work:

- `build-agent` for CMake build files
- `install-agent` for install output (`-DCMAKE_INSTALL_PREFIX=install-agent`)

## Build/Test Flags

When building tests, always configure CMake with these options:

- `-DDECENTRALISED_ART_BUILD_TESTS=ON`
- `-DDECENTRALISED_ART_USE_SUBMODULE_PT=ON`

The unit test build target is:

- `DecentralisedArtServerTests`
