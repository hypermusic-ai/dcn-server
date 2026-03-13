# Changelog

All notable changes to this project are documented in this file.

---

## [Unreleased]

### ⚠️ Breaking Changes

#### Feature API Removed

The `/feature` endpoint and all associated entity lifecycle infrastructure have been removed:

- **Removed routes**: `GET /feature/<name>/<ver?>`, `POST /feature`, `OPTIONS /feature`
- **Removed source files**: `src/api/src/api_feature.cpp`, `src/pt/include/feature.hpp`, `src/pt/src/feature.cpp`, `src/pt/proto/feature.proto`, `tests/src/pt/feature.cpp`
- **Migration**: Use the `/connector` endpoint and `Connector` entity type instead. See the updated [API Documentation](README.md#-api-documentation) in the README.

#### Constructor-Based Contract Deployment (No Proxy)

Contract deployment has switched from a proxy-upgrade pattern to direct constructor-based deployment:

- The proxy upgrade mechanism has been removed from the deployment pipeline.
- Deployed contracts are no longer upgradeable via proxy; each deployment creates a new contract instance.
- **Migration**: Any tooling or scripts that relied on proxy contract addresses must be updated to use the directly deployed contract addresses returned by the new deployment flow.

### ✨ New Features

#### Connector Bindings

Connectors now support *bindings*: named slot assignments that map open dimensional slots of a connector to specific target addresses on-chain.

- Bindings are specified as a JSON object in the `POST /connector` request body under the `"bindings"` key.
- Each entry maps a slot index (as a string key) to a target connector address.
- The server validates binding targets at ingestion time: empty targets and out-of-range slot indices are rejected.
- Example:
  ```json
  {
    "name": "melody_connector",
    "dimensions": [...],
    "bindings": {
      "0": "0xabc...",
      "1": "0xdef..."
    }
  }
  ```

### 🔄 Changes

- **Connector API** (`/connector`) replaces **Feature API** (`/feature`) as the primary entity endpoint.
- Registry internals refactored: dependency resolution, cycle detection, open-slot computation, and binding parsing are now handled during `addConnector`.
- `GET /connector/<name>[/<address>]` now accepts an optional on-chain address (instead of a version string); omitting the address returns the newest connector with that name.
- Loader updated to handle the new connector storage layout.
- EVM compilation flags updated (`--via-ir`).
- `GET /account/<address>` response fields updated: `owned_connectors` replaces `owned_features`.
