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

namespace dcn
{

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

            RUN_INSTANCE_ALREADY_REGISTERED,
            RUN_INSTANCE_MISSING,

            REGISTRY_ERROR,

        } kind = Kind::UNKNOWN;

        evmc_bytes32 a{};  // first bytes32 (or zero)
        evmc_bytes32 b{};  // second bytes32 (for run instance) or zero
        uint32_t code{};   // for RegistryError
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

        asio::awaitable<bool> addAccount(evmc::address address, std::uint64_t initial_gas) noexcept;
        asio::awaitable<bool> setGas(evmc::address address, std::uint64_t gas) noexcept;

        asio::awaitable<bool> compile(std::filesystem::path code_path,
                std::filesystem::path out_dir,
                std::filesystem::path base_path = {},
                std::filesystem::path includes = {}) const noexcept;

        asio::awaitable<std::expected<evmc::address, DeployError>> deploy(  
                    std::istream & code_stream, 
                    evmc::address sender,
                    std::vector<std::uint8_t> constructor_args,
                    std::uint64_t gas_limit,
                    std::uint64_t value) noexcept;

        asio::awaitable<std::expected<evmc::address, DeployError>> deploy(
                    std::filesystem::path code_path,    
                    evmc::address sender,
                    std::vector<uint8_t> constructor_args,
                    std::uint64_t gas_limit,
                    std::uint64_t value) noexcept;

        asio::awaitable<std::expected<std::vector<std::uint8_t>, evmc_status_code>> execute(
                    evmc::address sender,
                    evmc::address recipient, 
                    std::vector<std::uint8_t> input_bytes,
                    std::uint64_t gas_limit,
                    std::uint64_t value) noexcept;

        evmc::address getRegistryAddress() const;
        evmc::address getRunnerAddress() const;

    protected:
        asio::awaitable<bool> loadPT();

    private:
        asio::strand<asio::io_context::executor_type> _strand;
        
        evmc::VM _vm;
        evmc_revision _rev;

        std::filesystem::path _solc_path;

        EVMStorage _storage;
        
        evmc::address _genesis_address;
        evmc::address _console_log_address;

        evmc::address _registry_address;
        evmc::address _runner_address;
    };
    asio::awaitable<std::expected<std::vector<std::uint8_t>, evmc_status_code>> fetchOwner(EVM & evm, const evmc::address & address);

    std::vector<std::uint8_t> constructSelector(std::string signature);

    template<class T>
    std::vector<std::uint8_t> encodeAsArg(const T & val);

    template<>
    std::vector<std::uint8_t> encodeAsArg<evmc::address>(const evmc::address & address);

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
    evmc::address decodeReturnedValue(const std::vector<std::uint8_t> & bytes);

    template<>
    std::vector<Samples> decodeReturnedValue(const std::vector<uint8_t> & bytes);
}

template <>
struct std::formatter<dcn::DeployError::Kind> : std::formatter<std::string> {
    auto format(const dcn::DeployError::Kind & err, format_context& ctx) const {
        switch(err)
        {
            case dcn::DeployError::Kind::INVALID_BYTECODE : return formatter<string>::format("Invalid bytecode", ctx);
            case dcn::DeployError::Kind::INVALID_ADDRESS : return formatter<string>::format("Invalid address", ctx);
            case dcn::DeployError::Kind::INVALID_DATA : return formatter<string>::format("Invalid data", ctx);
            case dcn::DeployError::Kind::COMPILATION_ERROR : return formatter<string>::format("Compilation error", ctx);

            case dcn::DeployError::Kind::FEATURE_ALREADY_REGISTERED : return formatter<string>::format("Feature already registered", ctx);
            case dcn::DeployError::Kind::FEATURE_MISSING : return formatter<string>::format("Feature missing", ctx);

            case dcn::DeployError::Kind::TRANSFORMATION_ALREADY_REGISTERED : return formatter<string>::format("Transformation already registered", ctx);
            case dcn::DeployError::Kind::TRANSFORMATION_ARGUMENTS_MISMATCH : return formatter<string>::format("Transformation arguments mismatch", ctx);
            case dcn::DeployError::Kind::TRANSFORMATION_MISSING : return formatter<string>::format("Transformation missing", ctx);
            
            case dcn::DeployError::Kind::RUN_INSTANCE_ALREADY_REGISTERED : return formatter<string>::format("Run instance already registered", ctx);
            case dcn::DeployError::Kind::RUN_INSTANCE_MISSING : return formatter<string>::format("Run instance missing", ctx);

            default:  return formatter<string>::format("Unknown", ctx);
        }
        return formatter<string>::format("", ctx);
    }
};