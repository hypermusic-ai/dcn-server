#pragma once

#include <array>
#include <string>
#include <cstring>
#include <filesystem>
#include <vector>
#include <expected>
#include <fstream>
#include <istream>
#include <optional>

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
#include "crypto.hpp"
#include "chain.hpp"

#include "evm_storage.hpp"
#include "evm_formatter.hpp"

namespace dcn::evm
{
    class EVM
    {
    public:
        EVM(asio::io_context & io_context, evmc_revision rev, std::filesystem::path solc_path, std::filesystem::path pt_path);
        ~EVM() = default;

        EVM(const EVM&) = delete;
        EVM& operator=(const EVM&) = delete;

        EVM(EVM&&) = delete;
        EVM& operator=(EVM&&) = delete;

        asio::awaitable<bool> addAccount(chain::Address address, std::uint64_t initial_gas) noexcept;
        asio::awaitable<bool> setGas(chain::Address address, std::uint64_t gas) noexcept;

        asio::awaitable<bool> compile(std::filesystem::path code_path,
                std::filesystem::path out_dir,
                std::filesystem::path base_path = {},
                std::filesystem::path includes = {}) const noexcept;

        asio::awaitable<std::expected<chain::Address, chain::DeployError>> deploy(  
                    std::istream & code_stream, 
                    chain::Address sender,
                    std::vector<std::uint8_t> constructor_args,
                    std::uint64_t gas_limit,
                    std::uint64_t value) noexcept;

        asio::awaitable<std::expected<chain::Address, chain::DeployError>> deploy(
                    std::filesystem::path code_path,    
                    chain::Address sender,
                    std::vector<uint8_t> constructor_args,
                    std::uint64_t gas_limit,
                    std::uint64_t value) noexcept;

        asio::awaitable<std::expected<std::vector<std::uint8_t>, chain::ExecuteError>> execute(
                    chain::Address sender,
                    chain::Address recipient, 
                    std::vector<std::uint8_t> input_bytes,
                    std::uint64_t gas_limit,
                    std::uint64_t value) noexcept;

        chain::Address getRegistryAddress() const;
        chain::Address getRunnerAddress() const;

        const std::filesystem::path & getSolcPath() const;
        const std::filesystem::path & getPTPath() const;

    protected:
        asio::awaitable<bool> loadPT();

    private:
        asio::strand<asio::io_context::executor_type> _strand;
        
        evmc::VM _vm;
        evmc_revision _rev;

        std::filesystem::path _solc_path;
        std::filesystem::path _pt_path;

        EVMStorage _storage;
        
        chain::Address _genesis_address;
        chain::Address _console_log_address;

        chain::Address _registry_address;
        chain::Address _runner_address;
    };

    asio::awaitable<std::expected<std::vector<std::uint8_t>, chain::ExecuteError>> fetchOwner(EVM & evm, const chain::Address & address);

    template<class T>
    std::vector<std::uint8_t> encodeAsArg(const T & val);

    template<>
    std::vector<std::uint8_t> encodeAsArg<chain::Address>(const chain::Address & address);

    template<>
    std::vector<std::uint8_t> encodeAsArg<std::uint32_t>(const std::uint32_t & value);

    template<>
    std::vector<std::uint8_t> encodeAsArg<std::vector<std::uint32_t>>(const std::vector<std::uint32_t> & vec);

    template<>
    std::vector<std::uint8_t> encodeAsArg<std::vector<std::tuple<std::uint32_t, std::uint32_t>>>(const std::vector<std::tuple<std::uint32_t, std::uint32_t>>& vec);

    template<>
    std::vector<std::uint8_t> encodeAsArg<std::string>(const std::string& str);
}
