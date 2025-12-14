#pragma once

#include <array>
#include <string>
#include <cstring>
#include <filesystem>
#include <vector>
#include <expected>
#include <fstream>
#include <istream>

// Undefine the conflicting macro
#ifdef interface
    #undef interface
#endif

#include <evmc/evmc.h>
#include <evmc/evmc.hpp>
#include <evmc/mocked_host.hpp>

#include <evmc/hex.hpp>

#include <evmone/evmone.h>

#ifndef interface
    #define interface __STRUCT__
#endif

#include "native.h"
#include <asio.hpp>
#include <spdlog/spdlog.h>
#include <absl/container/flat_hash_map.h>

#include "utils.hpp"
#include "file.hpp"
#include "keccak256.hpp"
#include "pt.hpp"

#include "evm_storage.hpp"
#include "evm_formatter.hpp"

namespace dcn::evm
{
    using Address = evmc::address;

    /**
     * error FeatureAlreadyRegistered(bytes32 name);
     * error FeatureMissing(bytes32 name);
    
     * error TransformationAlreadyRegistered(bytes32 name);
     * error TransformationArgumentsMismatch(bytes32 name);
     * error TransformationMissing(bytes32 name);
    
     * error RunInstanceAlreadyRegistered(bytes32 featureName, bytes32 runInstanceName);
     * error RunInstanceMissing(bytes32 featureName, bytes32 runInstanceName);
    
     * error RegistryError(uint32 code);
    */
    struct DeployError 
    {
        enum class Kind : std::uint8_t
        {
            UNKNOWN = 0,

            INVALID_BYTECODE,
            INVALID_ADDRESS,
            INVALID_DATA,
            COMPILATION_ERROR,

            FEATURE_ALREADY_REGISTERED,
            FEATURE_MISSING,

            TRANSFORMATION_ALREADY_REGISTERED,
            TRANSFORMATION_ARGUMENTS_MISMATCH,
            TRANSFORMATION_MISSING,

            CONDITION_ALREADY_REGISTERED,
            CONDITION_ARGUMENTS_MISMATCH,
            CONDITION_MISSING,

            REGISTRY_ERROR,


        } kind = Kind::UNKNOWN;

        evmc_bytes32 a{};  // first bytes32 (or zero)
        uint32_t code{};   // for RegistryError
    };

    struct ExecuteError
    {
        enum class Kind : std::uint8_t
        {
            UNKNOWN = 0,
            CONDITION_NOT_MET

        } kind = Kind::UNKNOWN;

        evmc_bytes32 a{};  // first bytes32 (or zero)
    };

    class EVM
    {
    public:
        EVM(asio::io_context & io_context, evmc_revision rev, std::filesystem::path solc_path);
        ~EVM() = default;

        EVM(const EVM&) = delete;
        EVM& operator=(const EVM&) = delete;

        EVM(EVM&&) = delete;
        EVM& operator=(EVM&&) = delete;

        asio::awaitable<bool> addAccount(Address address, std::uint64_t initial_gas) noexcept;
        asio::awaitable<bool> setGas(Address address, std::uint64_t gas) noexcept;

        asio::awaitable<bool> compile(std::filesystem::path code_path,
                std::filesystem::path out_dir,
                std::filesystem::path base_path = {},
                std::filesystem::path includes = {}) const noexcept;

        asio::awaitable<std::expected<Address, DeployError>> deploy(  
                    std::istream & code_stream, 
                    Address sender,
                    std::vector<std::uint8_t> constructor_args,
                    std::uint64_t gas_limit,
                    std::uint64_t value) noexcept;

        asio::awaitable<std::expected<Address, DeployError>> deploy(
                    std::filesystem::path code_path,    
                    Address sender,
                    std::vector<uint8_t> constructor_args,
                    std::uint64_t gas_limit,
                    std::uint64_t value) noexcept;

        asio::awaitable<std::expected<std::vector<std::uint8_t>, ExecuteError>> execute(
                    Address sender,
                    Address recipient, 
                    std::vector<std::uint8_t> input_bytes,
                    std::uint64_t gas_limit,
                    std::uint64_t value) noexcept;

        Address getRegistryAddress() const;
        Address getRunnerAddress() const;

    protected:
        asio::awaitable<bool> loadPT();

    private:
        asio::strand<asio::io_context::executor_type> _strand;
        
        evmc::VM _vm;
        evmc_revision _rev;

        std::filesystem::path _solc_path;

        EVMStorage _storage;
        
        Address _genesis_address;
        Address _console_log_address;

        Address _registry_address;
        Address _runner_address;
    };
    asio::awaitable<std::expected<std::vector<std::uint8_t>, ExecuteError>> fetchOwner(EVM & evm, const Address & address);

    std::vector<std::uint8_t> constructSelector(std::string signature);

    template<class T>
    std::vector<std::uint8_t> encodeAsArg(const T & val);

    template<>
    std::vector<std::uint8_t> encodeAsArg<Address>(const Address & address);

    template<>
    std::vector<std::uint8_t> encodeAsArg<std::uint32_t>(const std::uint32_t & value);

    template<>
    std::vector<std::uint8_t> encodeAsArg<std::vector<std::uint32_t>>(const std::vector<std::uint32_t> & vec);

    template<>
    std::vector<std::uint8_t> encodeAsArg<std::vector<std::tuple<std::uint32_t, std::uint32_t>>>(const std::vector<std::tuple<std::uint32_t, std::uint32_t>>& vec);

    template<>
    std::vector<std::uint8_t> encodeAsArg<std::string>(const std::string& str);


    template<class T>
    T decodeReturnedValue(const std::vector<std::uint8_t> & bytes);


    template<>
    std::vector<std::vector<std::uint32_t>> decodeReturnedValue(const std::vector<std::uint8_t> & bytes);

    template<>
    Address decodeReturnedValue(const std::vector<std::uint8_t> & bytes);

    template<>
    std::vector<Samples> decodeReturnedValue(const std::vector<uint8_t> & bytes);
}

template <>
struct std::formatter<dcn::evm::DeployError::Kind> : std::formatter<std::string> {
    auto format(const dcn::evm::DeployError::Kind & err, format_context& ctx) const {
        switch(err)
        {
            case dcn::evm::DeployError::Kind::INVALID_BYTECODE : return formatter<string>::format("Invalid bytecode", ctx);
            case dcn::evm::DeployError::Kind::INVALID_ADDRESS : return formatter<string>::format("Invalid address", ctx);
            case dcn::evm::DeployError::Kind::INVALID_DATA : return formatter<string>::format("Invalid data", ctx);
            case dcn::evm::DeployError::Kind::COMPILATION_ERROR : return formatter<string>::format("Compilation error", ctx);

            case dcn::evm::DeployError::Kind::FEATURE_ALREADY_REGISTERED : return formatter<string>::format("Feature already registered", ctx);
            case dcn::evm::DeployError::Kind::FEATURE_MISSING : return formatter<string>::format("Feature missing", ctx);

            case dcn::evm::DeployError::Kind::TRANSFORMATION_ALREADY_REGISTERED : return formatter<string>::format("Transformation already registered", ctx);
            case dcn::evm::DeployError::Kind::TRANSFORMATION_ARGUMENTS_MISMATCH : return formatter<string>::format("Transformation arguments mismatch", ctx);
            case dcn::evm::DeployError::Kind::TRANSFORMATION_MISSING : return formatter<string>::format("Transformation missing", ctx);
            
            case dcn::evm::DeployError::Kind::CONDITION_ALREADY_REGISTERED : return formatter<string>::format("Condition already registered", ctx);
            case dcn::evm::DeployError::Kind::CONDITION_ARGUMENTS_MISMATCH : return formatter<string>::format("Condition arguments mismatch", ctx);
            case dcn::evm::DeployError::Kind::CONDITION_MISSING : return formatter<string>::format("Condition missing", ctx);

            default:  return formatter<string>::format("Unknown", ctx);
        }
        return formatter<string>::format("", ctx);
    }
};

template <>
struct std::formatter<dcn::evm::ExecuteError::Kind> : std::formatter<std::string> {
    auto format(const dcn::evm::ExecuteError::Kind & err, format_context& ctx) const {
        switch(err)
        {
            case dcn::evm::ExecuteError::Kind::CONDITION_NOT_MET : return formatter<string>::format("Condition not met", ctx);

            default:  return formatter<string>::format("Unknown", ctx);
        }
        return formatter<string>::format("", ctx);
    }
};