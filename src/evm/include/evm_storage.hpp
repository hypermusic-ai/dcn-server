#pragma once

#include <string>
#include <cstring>
#include <vector>
#include <format>
#include <stack>

// Undefine the conflicting macro
#ifdef interface
    #undef interface
#endif

#include <evmc/evmc.h>
#include <evmc/evmc.hpp>
#include <evmc/hex.hpp>

#ifndef interface
    #define interface __STRUCT__
#endif

#include "native.h"
#include <asio.hpp>
#include <spdlog/spdlog.h>
#include <absl/container/flat_hash_map.h>

#include "utils.hpp"
#include "keccak256.hpp"

#include "evm_formatter.hpp"

namespace dcn::evm
{
    const std::uint64_t DEFAULT_GAS_LIMIT = 100'000'000;

    class EVMStorage : public evmc::Host
    {
    public:
        struct Account
        {
            evmc_uint256be balance;
            std::vector<std::uint8_t> code;
            absl::flat_hash_map<evmc::bytes32, evmc::bytes32> storage;

            // Deployment metadata
            evmc::address creator{};
            uint64_t nonce = 0;
            uint64_t timestamp = 0;
        };

        EVMStorage(evmc::VM & vm, evmc_revision rev);

        bool add_account(const evmc::address& addr)
        {
            if(account_exists(addr) == true)
            {
                spdlog::error(std::format("add_account: Account {} already exists", addr));
                return false;
            }
            _accounts.emplace(to_key(addr), Account{});
            return true;
        }

        void set_balance(const evmc::address& addr, std::uint64_t x) noexcept
        {   
            if(account_exists(addr) == false)
            {
                spdlog::error(std::format("set_balance : Account {} does not exist", addr));
                return;
            }
            Account & acc = _accounts.at(to_key(addr));

            acc.balance = evmc::uint256be{};
            for (std::size_t i = 0; i < sizeof(x); ++i)
                acc.balance.bytes[sizeof(acc.balance) - 1 - i] = static_cast<uint8_t>(x >> (8 * i));
        }

        bool account_exists(const evmc::address& addr) const noexcept override;

        evmc::bytes32 get_storage(const evmc::address& addr, const evmc::bytes32& key) const noexcept override;

        evmc_storage_status set_storage(const evmc::address& address, const evmc::bytes32& key, const evmc::bytes32& value) noexcept override;

        evmc::uint256be get_balance(const evmc::address& addr) const noexcept override;

        std::size_t get_code_size(const evmc::address& addr) const noexcept override;

        evmc::bytes32 get_code_hash(const evmc::address& addr) const noexcept override;

        std::size_t copy_code(const evmc::address& addr,
                             std::size_t code_offset,
                             std::uint8_t* buffer_data,
                             std::size_t buffer_size) const noexcept override;

        bool selfdestruct(const evmc::address& addr, const evmc::address& beneficiary) noexcept override;

        evmc::Result call(const evmc_message& msg) noexcept override;

        evmc_tx_context get_tx_context() const noexcept override;

        evmc::bytes32 get_block_hash(int64_t block_number) const noexcept override;
        void emit_log(const evmc::address& addr,
                          const std::uint8_t* data,
                          size_t data_size,
                          const evmc::bytes32 topics[],
                          size_t num_topics) noexcept override;

        evmc_access_status access_account(const evmc::address& addr) noexcept override;

        evmc_access_status access_storage(const evmc::address& addr, const evmc::bytes32& key) noexcept override;

        evmc::bytes32 get_transient_storage(const evmc::address& addr,
                                          const evmc::bytes32& key) const noexcept override;

        void set_transient_storage(const evmc::address& addr,
                                       const evmc::bytes32& key,
                                       const evmc::bytes32& value) noexcept override;

    protected:
        std::string to_key(const evmc::address& addr) const;

        evmc_address derive_create_address(const evmc::address& sender, std::uint64_t nonce);

        evmc_address derive_create2_address(const evmc::address& sender, const evmc::bytes32& salt, const std::vector<std::uint8_t>& code);

        void deploy_contract(evmc::address addr, std::vector<std::uint8_t>&& code, evmc_uint256be value, evmc::address creator, std::uint64_t nonce);
    
    private:
        evmc::VM & _vm;
        evmc_revision _revision;

        absl::flat_hash_map<std::string, Account> _accounts;
        absl::flat_hash_map<evmc::address, std::uint64_t> _create_nonce;

        std::stack<evmc::address> _sender_stack;
    };

    template <typename H>
    inline H AbslHashValue(H h, const evmc::address & addr)
    {
        return H::combine(std::move(h), addr.bytes);
    }
}
 
