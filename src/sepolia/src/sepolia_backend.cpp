#include "sepolia_backend.hpp"

#include <algorithm>
#include <array>
#include <chrono>
#include <cstring>
#include <format>
#include <limits>
#include <ranges>
#include <thread>
#include <utility>

#include <nlohmann/json.hpp>
#include <secp256k1.h>
#include <secp256k1_recovery.h>

#include "keccak256.hpp"
#include "native.h"

namespace dcn::sepolia
{
    using json = nlohmann::json;

    namespace
    {
        using Bytes = std::vector<std::uint8_t>;

        std::string _stripHexPrefix(const std::string & value)
        {
            if(value.rfind("0x", 0) == 0 || value.rfind("0X", 0) == 0)
            {
                return value.substr(2);
            }
            return value;
        }

        std::optional<Bytes> _hexToBytes(const std::string & value)
        {
            const std::string hex = _stripHexPrefix(value);
            if(hex.empty() || (hex.size() % 2) != 0)
            {
                return std::nullopt;
            }

            static auto hex_value = [](const char c) -> int
            {
                if(c >= '0' && c <= '9')
                {
                    return c - '0';
                }
                if(c >= 'a' && c <= 'f')
                {
                    return 10 + (c - 'a');
                }
                if(c >= 'A' && c <= 'F')
                {
                    return 10 + (c - 'A');
                }
                return -1;
            };

            Bytes out;
            out.reserve(hex.size() / 2);

            for(std::size_t i = 0; i < hex.size(); i += 2)
            {
                const int hi = hex_value(hex[i]);
                const int lo = hex_value(hex[i + 1]);
                if(hi < 0 || lo < 0)
                {
                    return std::nullopt;
                }

                out.push_back(static_cast<std::uint8_t>((hi << 4) | lo));
            }

            return out;
        }

        std::string _bytesToHex(const std::uint8_t* bytes, const std::size_t size, const bool with_prefix = true)
        {
            static constexpr char HEX[] = "0123456789abcdef";
            std::string out;
            out.reserve(size * 2 + (with_prefix ? 2 : 0));
            if(with_prefix)
            {
                out += "0x";
            }

            for(std::size_t i = 0; i < size; ++i)
            {
                const std::uint8_t b = bytes[i];
                out.push_back(HEX[(b >> 4) & 0x0F]);
                out.push_back(HEX[b & 0x0F]);
            }

            return out;
        }

        std::string _bytesToHex(const Bytes & bytes, const bool with_prefix = true)
        {
            return _bytesToHex(bytes.data(), bytes.size(), with_prefix);
        }

        std::string _addressToHex(const evmc::address & address, const bool with_prefix = true)
        {
            return _bytesToHex(address.bytes, sizeof(address.bytes), with_prefix);
        }

        std::optional<std::uint64_t> _parseQuantity(const std::string & value)
        {
            try
            {
                if(value.empty())
                {
                    return std::nullopt;
                }

                const std::string stripped = _stripHexPrefix(value);
                if(stripped.empty())
                {
                    return 0;
                }

                if(stripped.size() > 16)
                {
                    return std::nullopt;
                }

                return static_cast<std::uint64_t>(std::stoull(stripped, nullptr, 16));
            }
            catch(...)
            {
                return std::nullopt;
            }
        }

        std::string _toQuantity(const std::uint64_t value)
        {
            return std::format("0x{:x}", value);
        }

        std::array<std::uint8_t, 32> _keccak(const std::uint8_t* bytes, const std::size_t size)
        {
            std::array<std::uint8_t, 32> hash{};
            crypto::Keccak256::getHash(bytes, size, hash.data());
            return hash;
        }

        Bytes _minimalBigEndian(std::uint64_t value)
        {
            if(value == 0)
            {
                return {};
            }

            Bytes out;
            while(value > 0)
            {
                out.push_back(static_cast<std::uint8_t>(value & 0xFFu));
                value >>= 8;
            }

            std::ranges::reverse(out);
            return out;
        }

        Bytes _trimLeadingZeros(const std::uint8_t* bytes, const std::size_t size)
        {
            std::size_t first = 0;
            while(first < size && bytes[first] == 0)
            {
                ++first;
            }

            if(first == size)
            {
                return {};
            }

            return Bytes(bytes + first, bytes + size);
        }

        Bytes _rlpEncodeBytes(const std::uint8_t* bytes, const std::size_t size)
        {
            if(size == 1 && bytes[0] < 0x80)
            {
                return Bytes{bytes[0]};
            }

            Bytes out;
            if(size <= 55)
            {
                out.reserve(1 + size);
                out.push_back(static_cast<std::uint8_t>(0x80 + size));
            }
            else
            {
                const Bytes len_be = _minimalBigEndian(static_cast<std::uint64_t>(size));
                out.reserve(1 + len_be.size() + size);
                out.push_back(static_cast<std::uint8_t>(0xB7 + len_be.size()));
                out.insert(out.end(), len_be.begin(), len_be.end());
            }

            out.insert(out.end(), bytes, bytes + size);
            return out;
        }

        Bytes _rlpEncodeBytes(const Bytes & bytes)
        {
            return _rlpEncodeBytes(bytes.data(), bytes.size());
        }

        Bytes _rlpEncodeUint64(const std::uint64_t value)
        {
            const Bytes value_be = _minimalBigEndian(value);
            return _rlpEncodeBytes(value_be);
        }

        Bytes _rlpEncodeBigInteger(const std::uint8_t* bytes, const std::size_t size)
        {
            const Bytes trimmed = _trimLeadingZeros(bytes, size);
            return _rlpEncodeBytes(trimmed);
        }

        Bytes _rlpEncodeList(const std::vector<Bytes> & encoded_items)
        {
            std::size_t payload_size = 0;
            for(const Bytes & item : encoded_items)
            {
                payload_size += item.size();
            }

            Bytes payload;
            payload.reserve(payload_size);
            for(const Bytes & item : encoded_items)
            {
                payload.insert(payload.end(), item.begin(), item.end());
            }

            Bytes out;
            if(payload.size() <= 55)
            {
                out.reserve(1 + payload.size());
                out.push_back(static_cast<std::uint8_t>(0xC0 + payload.size()));
            }
            else
            {
                const Bytes len_be = _minimalBigEndian(static_cast<std::uint64_t>(payload.size()));
                out.reserve(1 + len_be.size() + payload.size());
                out.push_back(static_cast<std::uint8_t>(0xF7 + len_be.size()));
                out.insert(out.end(), len_be.begin(), len_be.end());
            }

            out.insert(out.end(), payload.begin(), payload.end());
            return out;
        }

        std::expected<std::array<std::uint8_t, 32>, chain::DeployError> _parsePrivateKey(const std::string & private_key_hex)
        {
            const auto bytes_res = _hexToBytes(private_key_hex);
            if(!bytes_res || bytes_res->size() != 32)
            {
                return std::unexpected(chain::DeployError{
                    .kind = chain::DeployError::Kind::INVALID_CONFIG,
                    .message = "private_key_hex must be 32-byte hex value"
                });
            }

            std::array<std::uint8_t, 32> key{};
            std::copy(bytes_res->begin(), bytes_res->end(), key.begin());
            return key;
        }

        std::expected<evmc::address, chain::DeployError> _deriveAddress(const std::array<std::uint8_t, 32> & private_key)
        {
            secp256k1_context* ctx = secp256k1_context_create(SECP256K1_CONTEXT_SIGN | SECP256K1_CONTEXT_VERIFY);
            if(ctx == nullptr)
            {
                return std::unexpected(chain::DeployError{
                    .kind = chain::DeployError::Kind::SIGNING_ERROR,
                    .message = "Failed to create secp256k1 context"
                });
            }

            secp256k1_pubkey pubkey{};
            const int pubkey_ok = secp256k1_ec_pubkey_create(ctx, &pubkey, private_key.data());
            if(pubkey_ok != 1)
            {
                secp256k1_context_destroy(ctx);
                return std::unexpected(chain::DeployError{
                    .kind = chain::DeployError::Kind::SIGNING_ERROR,
                    .message = "Invalid private key"
                });
            }

            std::array<std::uint8_t, 65> serialized_pubkey{};
            std::size_t pubkey_size = serialized_pubkey.size();
            if(secp256k1_ec_pubkey_serialize(
                ctx,
                serialized_pubkey.data(),
                &pubkey_size,
                &pubkey,
                SECP256K1_EC_UNCOMPRESSED) != 1)
            {
                secp256k1_context_destroy(ctx);
                return std::unexpected(chain::DeployError{
                    .kind = chain::DeployError::Kind::SIGNING_ERROR,
                    .message = "Failed to serialize public key"
                });
            }

            secp256k1_context_destroy(ctx);

            const auto hash = _keccak(serialized_pubkey.data() + 1, pubkey_size - 1);
            evmc::address out{};
            std::memcpy(out.bytes, hash.data() + 12, 20);
            return out;
        }

        struct SignedTransaction
        {
            Bytes raw_bytes;
        };

        std::expected<SignedTransaction, chain::DeployError> _signCreateTx(
            const std::array<std::uint8_t, 32> & private_key,
            const std::uint64_t chain_id,
            const std::uint64_t nonce,
            const std::uint64_t max_priority_fee_per_gas,
            const std::uint64_t max_fee_per_gas,
            const std::uint64_t gas_limit,
            const std::uint64_t value_wei,
            const Bytes & init_code)
        {
            const Bytes empty_to{};
            const Bytes empty_access_list = _rlpEncodeList({});

            std::vector<Bytes> unsigned_fields{
                _rlpEncodeUint64(chain_id),
                _rlpEncodeUint64(nonce),
                _rlpEncodeUint64(max_priority_fee_per_gas),
                _rlpEncodeUint64(max_fee_per_gas),
                _rlpEncodeUint64(gas_limit),
                _rlpEncodeBytes(empty_to),
                _rlpEncodeUint64(value_wei),
                _rlpEncodeBytes(init_code),
                empty_access_list
            };

            const Bytes unsigned_payload = _rlpEncodeList(unsigned_fields);

            Bytes signing_blob;
            signing_blob.reserve(1 + unsigned_payload.size());
            signing_blob.push_back(0x02);
            signing_blob.insert(signing_blob.end(), unsigned_payload.begin(), unsigned_payload.end());

            const auto sig_hash = _keccak(signing_blob.data(), signing_blob.size());

            secp256k1_context* ctx = secp256k1_context_create(SECP256K1_CONTEXT_SIGN | SECP256K1_CONTEXT_VERIFY);
            if(ctx == nullptr)
            {
                return std::unexpected(chain::DeployError{
                    .kind = chain::DeployError::Kind::SIGNING_ERROR,
                    .message = "Failed to create secp256k1 context"
                });
            }

            secp256k1_ecdsa_recoverable_signature signature{};
            if(secp256k1_ecdsa_sign_recoverable(
                ctx,
                &signature,
                sig_hash.data(),
                private_key.data(),
                nullptr,
                nullptr) != 1)
            {
                secp256k1_context_destroy(ctx);
                return std::unexpected(chain::DeployError{
                    .kind = chain::DeployError::Kind::SIGNING_ERROR,
                    .message = "Failed to sign transaction"
                });
            }

            std::array<std::uint8_t, 64> compact_signature{};
            int recid = 0;
            secp256k1_ecdsa_recoverable_signature_serialize_compact(
                ctx, compact_signature.data(), &recid, &signature);
            secp256k1_context_destroy(ctx);

            const std::uint64_t y_parity = static_cast<std::uint64_t>(recid & 0x01);

            std::vector<Bytes> signed_fields = unsigned_fields;
            signed_fields.push_back(_rlpEncodeUint64(y_parity));
            signed_fields.push_back(_rlpEncodeBigInteger(compact_signature.data(), 32));
            signed_fields.push_back(_rlpEncodeBigInteger(compact_signature.data() + 32, 32));

            const Bytes signed_payload = _rlpEncodeList(signed_fields);

            SignedTransaction out;
            out.raw_bytes.reserve(1 + signed_payload.size());
            out.raw_bytes.push_back(0x02);
            out.raw_bytes.insert(out.raw_bytes.end(), signed_payload.begin(), signed_payload.end());

            return out;
        }
    }

    SepoliaBackend::SepoliaBackend(BackendConfig cfg)
        : _cfg(std::move(cfg))
    {
        if(_cfg.rpc_url.empty())
        {
            _init_error = chain::DeployError{
                .kind = chain::DeployError::Kind::INVALID_CONFIG,
                .message = "rpc_url is required"
            };
            return;
        }

        const auto key_res = _parsePrivateKey(_cfg.private_key_hex);
        if(!key_res)
        {
            _init_error = key_res.error();
            return;
        }
        _private_key = *key_res;

        const auto signer_res = _deriveAddress(_private_key);
        if(!signer_res)
        {
            _init_error = signer_res.error();
            return;
        }
        _signer_address = *signer_res;
    }

    const BackendConfig & SepoliaBackend::config() const noexcept
    {
        return _cfg;
    }

    std::expected<evmc::address, chain::DeployError> SepoliaBackend::signerAddress() const
    {
        if(_init_error)
        {
            return std::unexpected(*_init_error);
        }
        return _signer_address;
    }

    std::expected<std::string, chain::DeployError> SepoliaBackend::sendCreateTransaction(
        const std::vector<std::uint8_t> & init_code,
        const std::optional<std::uint64_t> gas_limit,
        const std::uint64_t value_wei) const
    {
        if(_init_error)
        {
            return std::unexpected(*_init_error);
        }

        if(init_code.empty())
        {
            return std::unexpected(chain::DeployError{
                .kind = chain::DeployError::Kind::INVALID_INPUT,
                .message = "init_code must not be empty"
            });
        }

        const std::string signer_hex = _addressToHex(_signer_address, true);
        const std::string data_hex = _bytesToHex(init_code, true);

        const auto nonce_res = [&]() -> std::expected<std::uint64_t, chain::DeployError>
        {
            const auto rpc_res = rpc("eth_getTransactionCount", json::array({signer_hex, "pending"}));
            if(!rpc_res || !rpc_res->is_string())
            {
                if(!rpc_res)
                {
                    return std::unexpected(rpc_res.error());
                }
                return std::unexpected(chain::DeployError{
                    .kind = chain::DeployError::Kind::RPC_MALFORMED,
                    .message = "eth_getTransactionCount returned non-string result"
                });
            }

            const auto parsed = _parseQuantity(rpc_res->get<std::string>());
            if(!parsed)
            {
                return std::unexpected(chain::DeployError{
                    .kind = chain::DeployError::Kind::RPC_MALFORMED,
                    .message = "Failed to parse nonce quantity"
                });
            }
            return *parsed;
        }();

        if(!nonce_res)
        {
            return std::unexpected(nonce_res.error());
        }

        std::uint64_t max_priority_fee = _cfg.fallback_max_priority_fee_wei;
        if(const auto max_priority_res = rpc("eth_maxPriorityFeePerGas", json::array()))
        {
            if(max_priority_res->is_string())
            {
                if(const auto parsed = _parseQuantity(max_priority_res->get<std::string>()))
                {
                    max_priority_fee = *parsed;
                }
            }
        }

        std::uint64_t base_fee = 0;
        if(const auto block_res = rpc("eth_getBlockByNumber", json::array({"latest", false})))
        {
            if(block_res->is_object() && block_res->contains("baseFeePerGas") && (*block_res)["baseFeePerGas"].is_string())
            {
                if(const auto parsed = _parseQuantity((*block_res)["baseFeePerGas"].get<std::string>()))
                {
                    base_fee = *parsed;
                }
            }
        }

        std::uint64_t max_fee = max_priority_fee;
        if(base_fee <= (std::numeric_limits<std::uint64_t>::max() - max_priority_fee) / 2)
        {
            max_fee = (base_fee * 2) + max_priority_fee;
        }
        else
        {
            max_fee = std::numeric_limits<std::uint64_t>::max();
        }

        std::uint64_t tx_gas_limit = gas_limit.value_or(_cfg.gas_limit_fallback);
        if(!gas_limit.has_value())
        {
            const auto estimate_res = rpc("eth_estimateGas", json::array({json{
                {"from", signer_hex},
                {"data", data_hex},
                {"value", _toQuantity(value_wei)}
            }}));

            if(estimate_res && estimate_res->is_string())
            {
                if(const auto parsed = _parseQuantity(estimate_res->get<std::string>()))
                {
                    tx_gas_limit = *parsed;
                }
            }
        }

        const auto signed_tx_res = _signCreateTx(
            _private_key,
            _cfg.chain_id,
            *nonce_res,
            max_priority_fee,
            max_fee,
            tx_gas_limit,
            value_wei,
            init_code);
        if(!signed_tx_res)
        {
            return std::unexpected(signed_tx_res.error());
        }

        const std::string raw_tx_hex = _bytesToHex(signed_tx_res->raw_bytes, true);

        const auto send_res = rpc("eth_sendRawTransaction", json::array({raw_tx_hex}));
        if(!send_res)
        {
            return std::unexpected(send_res.error());
        }

        if(!send_res->is_string())
        {
            return std::unexpected(chain::DeployError{
                .kind = chain::DeployError::Kind::RPC_MALFORMED,
                .message = "eth_sendRawTransaction returned non-string result"
            });
        }

        return send_res->get<std::string>();
    }

    std::expected<chain::DeployReceipt, chain::DeployError> SepoliaBackend::deployContract(
        const std::vector<std::uint8_t> & init_code,
        const std::optional<std::uint64_t> gas_limit,
        const std::uint64_t value_wei) const
    {
        const auto tx_hash_res = sendCreateTransaction(init_code, gas_limit, value_wei);
        if(!tx_hash_res)
        {
            return std::unexpected(tx_hash_res.error());
        }

        const std::string tx_hash = *tx_hash_res;
        for(std::size_t i = 0; i < _cfg.max_receipt_polls; ++i)
        {
            const auto receipt_res = rpc("eth_getTransactionReceipt", json::array({tx_hash}));
            if(!receipt_res)
            {
                return std::unexpected(receipt_res.error());
            }

            if(receipt_res->is_null())
            {
                std::this_thread::sleep_for(std::chrono::milliseconds(_cfg.receipt_poll_interval_ms));
                continue;
            }

            if(!receipt_res->is_object())
            {
                return std::unexpected(chain::DeployError{
                    .kind = chain::DeployError::Kind::RPC_MALFORMED,
                    .message = "eth_getTransactionReceipt returned non-object result"
                });
            }

            if(receipt_res->contains("status") && (*receipt_res)["status"].is_string())
            {
                const std::string status = (*receipt_res)["status"].get<std::string>();
                if(status == "0x0")
                {
                    return std::unexpected(chain::DeployError{
                        .kind = chain::DeployError::Kind::TRANSACTION_REVERTED,
                        .message = std::format("Deployment transaction reverted ({})", tx_hash)
                    });
                }
            }

            if(!receipt_res->contains("contractAddress") || !(*receipt_res)["contractAddress"].is_string())
            {
                return std::unexpected(chain::DeployError{
                    .kind = chain::DeployError::Kind::RPC_MALFORMED,
                    .message = "Receipt is missing contractAddress"
                });
            }

            const auto contract_address_bytes = _hexToBytes((*receipt_res)["contractAddress"].get<std::string>());
            if(!contract_address_bytes || contract_address_bytes->size() != 20)
            {
                return std::unexpected(chain::DeployError{
                    .kind = chain::DeployError::Kind::RPC_MALFORMED,
                    .message = "Receipt contains invalid contractAddress"
                });
            }

            chain::DeployReceipt deploy_receipt;
            deploy_receipt.tx_hash = tx_hash;
            deploy_receipt.signer_address = _signer_address;
            std::copy(contract_address_bytes->begin(), contract_address_bytes->end(), deploy_receipt.contract_address.bytes);
            deploy_receipt.block_number_hex = receipt_res->value("blockNumber", "0x0");
            deploy_receipt.gas_used_hex = receipt_res->value("gasUsed", "0x0");
            return deploy_receipt;
        }

        return std::unexpected(chain::DeployError{
            .kind = chain::DeployError::Kind::TIMEOUT,
            .message = std::format("Timed out while waiting for receipt ({})", tx_hash)
        });
    }

    std::expected<json, chain::DeployError> SepoliaBackend::rpc(const std::string & method, json params) const
    {
        if(_cfg.rpc_url.empty())
        {
            return std::unexpected(chain::DeployError{
                .kind = chain::DeployError::Kind::INVALID_CONFIG,
                .message = "rpc_url is empty"
            });
        }

        const json request{
            {"jsonrpc", "2.0"},
            {"id", 1},
            {"method", method},
            {"params", std::move(params)}
        };

        std::vector<std::string> args{
            "-sS",
            "-X", "POST",
            _cfg.rpc_url,
            "-H", "Content-Type: application/json",
            "--data", request.dump()
        };

        const auto [exit_code, output] = native::runProcess("curl", std::move(args));
        if(exit_code != 0)
        {
            return std::unexpected(chain::DeployError{
                .kind = chain::DeployError::Kind::RPC_ERROR,
                .message = std::format("curl failed for method '{}' with code {}: {}", method, exit_code, output)
            });
        }

        const json response = json::parse(output, nullptr, false);
        if(response.is_discarded())
        {
            return std::unexpected(chain::DeployError{
                .kind = chain::DeployError::Kind::RPC_MALFORMED,
                .message = std::format("Invalid JSON response for method '{}': {}", method, output)
            });
        }

        if(response.contains("error"))
        {
            return std::unexpected(chain::DeployError{
                .kind = chain::DeployError::Kind::RPC_ERROR,
                .message = std::format("RPC '{}' error: {}", method, response["error"].dump())
            });
        }

        if(!response.contains("result"))
        {
            return std::unexpected(chain::DeployError{
                .kind = chain::DeployError::Kind::RPC_MALFORMED,
                .message = std::format("RPC '{}' response missing result field", method)
            });
        }

        return response["result"];
    }
}
