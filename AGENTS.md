# AGENTS.md

## Scope

Instructions in this file apply to the repository root (`dcn-server-cpp`) and all subdirectories.

## Repository Structure

Key directories in this repository:

- `src/` - C++ implementation of the DCN server
- `include/` - public/internal header files
- `tests/` - unit/integration tests
- `cmake/` - CMake helper modules
- `resources/` - runtime resources and supporting data
- `submodule/pt/` - PT protocol Solidity repository (git submodule)

## Multi-Repository Context

This workspace contains two primary repositories with different ownership:

- `submodule/pt` is the Solidity PT protocol and is the **source of truth** for protocol behavior.
- `dcn-server-cpp` (this repository) is a C++ REST API server that interacts with the PT protocol.

When protocol behavior is unclear, agents should treat `submodule/pt` as authoritative and ensure server-side behavior aligns with it.

## Build Directory Policy

Agents must use a build directory for all work:

- `build-agent` for CMake build files
- `install-agent` for install output (`-DCMAKE_INSTALL_PREFIX=install-agent`)

## Build/Test Flags

When building tests, always configure CMake with these options:

- `-DDECENTRALISED_ART_BUILD_TESTS=ON`
- `-DDECENTRALISED_ART_USE_SUBMODULE_PT=ON`

Use build configuration Debug or RelWithDebInfo.

The unit test build target is:

- `DecentralisedArtServerTests`
