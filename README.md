# 🚀 Decentralised Art server

## 📚 Documentation

</br>

[![Docs](https://img.shields.io/badge/docs-online-blue)](https://hypermusic-ai.github.io/dcn-server/)

[View Doxygen API Docs](https://hypermusic-ai.github.io/dcn-server/)

[View Changelog](CHANGELOG.md)

</br>

---

## 📦 Dependencies

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

</br>

---

## ⚙️ Configuring the Project

To configure the project using CMake, run the following command:

```sh
cmake -S . -B build -DCMAKE_INSTALL_PREFIX=install -DDECENTRALIZED_ART_BUILD_TESTS=ON
```

- `-S .`: Specifies the source directory.
- `-B build`: Specifies the build directory.
- `-DCMAKE_INSTALL_PREFIX=install`: Specifies install directory.
- `-DDECENTRALIZED_ART_BUILD_TESTS=ON`: Enables tests.

</br>

---

## 🛠️ Building the Project (Debug Mode)

To build the project in **Debug Mode**, use:

```sh
cmake --build build --config Debug
```

This will compile the project with debugging enabled.

## 🛠️ Installing the Project (Debug Mode)

To install project in **Debug Mode**, use:

```sh
cmake --build build --config Debug --target install
```

This will install the project with debugging enabled.

</br>

---

## 🖥️ Start the server

```sh
./build/Debug/DecentralisedArtServer.exe
```

</br>

---

## 📚 API Documentation

This section describes the available API endpoints for the server backend.

> ⚠️ All protected routes require authentication via a secure `access_token` cookie. After login, this cookie is automatically included in requests by the browser.

---

### 📄 Interface

#### `OPTIONS /`

Returns CORS headers for the simple HTML interface endpoint.

- **Authentication**: ❌ Public

---

#### `GET /`

Returns the simple HTML interface to interact with the server.

- **Response Type**: `text/html`  
- **Authentication**: ❌ Public

---

### 🔐 Authentication

#### `GET /nonce/<address>`

Returns a nonce for the specified Ethereum address to use in the login message.

- **Params**:
  - `address`: Ethereum address (e.g., `0x123...`)
- **Response**:

```json
{ "nonce": "string" }
```

- **Authentication**: ❌ Public

#### `POST /auth`

Verifies the signed nonce and sets an authentication cookie (access_token).

- **Request Body(JSON)**:
  
```json
{
  "address": "0x...",
  "message": "Login nonce: ...",
  "signature": "0x..."
}
```

- **Response**: `200 OK` : `Authentication successful`

- **Authentication**:
  - **Header**: Set-Cookie: `access_token`=...; HttpOnly; Secure; SameSite=Strict; Path=/
  - **Header**: Set-Cookie: `refresh_token`=...; HttpOnly; Secure; SameSite=Strict; Path=/refresh"

- **Authentication**: ❌ Public

#### `POST /refresh`

Verifies `refresh_token` and `access_token` and generates new `access_token`.

- **Response**: `200 OK` : `Authentication successful`

- **Authentication**:
  - **Header**: Set-Cookie: `access_token`=...; HttpOnly; Secure; SameSite=Strict; Path=/
  - **Header**: Set-Cookie: `refresh_token`=...; HttpOnly; Secure; SameSite=Strict; Path=/refresh"

- **Authentication**: ✅ Required

---

### 🧩 Connectors

#### `OPTIONS /connector`

Returns CORS headers for the connector endpoints.

#### `GET /connector/<name>/<address?>`

Fetches a connector by name and optional address.
If address is not provided, returns the newest connector of that name.

- **Params**:
  - `name`: String identifier
  - `address?`: Optional connector address

- **Response**: Connector data `(JSON)`

  ```json
  {
    "name": "melody_connector",
    "dimensions": [
      {
        "composite": "bass_connector",
        "transformations": [
          { "name": "add", "args": [1] }
        ]
      }
    ],
    "condition_name": "is_active",
    "condition_args": [1]
  }
  ```

- **Authentication**: ❌ Public

#### `POST /connector`

Creates a new connector record.

- **Request Headers**: `Cookie` must include `access_token`
- **Request Body**: `JSON` payload describing the connector with inline `dimensions`
- **Response**: `201 Created` or `error`
- **Authentication**: ✅ Required

#### `GET /account/<address>?limit=<n>&page=<n>`

Returns paged ownership info.

- **Response fields**:
  - `owned_connectors`
  - `owned_transformations`
  - `owned_conditions`
  - `total_connectors`
  - `total_transformations`
  - `total_conditions`

---

### 🔄 Transformation

#### `OPTIONS /transformation`

Returns CORS headers for the transformation endpoints.

#### `GET /transformation/<name>/<ver?>`

Fetches a transformation by name and version.
If version is not given, returns newest transformation of given name.

- **Params**:
  - `name`: String identifier
  - `ver`: Optional String identifier

- **Response**: Transformation data `(JSON)`

  ```json
  {
    "name": "...",
    "sol_src": "..."
  }
  ```

- **Authentication**: ❌ Public

#### `POST /transformation`

Creates a new transformation record.

- **Request Headers**: `Cookie` must include `access_token`

- **Request Body**: `JSON` payload describing the transformation

  ```json
  {
    "name": "...",
    "sol_src": "..."
  }
  ```

- **Response**: `201 Created` or `error`
- **Response Body**:

  ```json
  {
    "name": "...",
    "version": "1823..."
  }
  ```

- **Authentication**: ✅ Required

---
📜 **License**:

👨‍💻 **Contributors**: [Sawyer](https://github.com/MisterSawyer)

💡 **Additional Notes**:
