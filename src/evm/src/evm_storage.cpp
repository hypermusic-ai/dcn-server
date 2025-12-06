#include "evm_storage.hpp"

namespace dcn::evm
{
    EVMStorage::EVMStorage(evmc::VM & vm, evmc_revision rev)
        :   _vm(vm),
            _revision(rev)
    {
    }

    bool EVMStorage::account_exists(const evmc::address& addr) const noexcept
    {
        return _accounts.find(to_key(addr)) != _accounts.end();
    }

    evmc::bytes32 EVMStorage::get_storage(const evmc::address& addr, const evmc::bytes32& key) const noexcept
    {
        if(account_exists(addr) == false) 
        {
            spdlog::error(std::format("get_storage : Account {} does not exist", addr));
            return {};
        }
        if(_accounts.at(to_key(addr)).storage.find(key) == _accounts.at(to_key(addr)).storage.end()) return {};
        return _accounts.at(to_key(addr)).storage.at(key);
    }

    evmc_storage_status EVMStorage::set_storage(const evmc::address& address, const evmc::bytes32& key, const evmc::bytes32& value) noexcept
    {
        _accounts[to_key(address)].storage[key] = value;
        return EVMC_STORAGE_MODIFIED;
    }

    evmc::uint256be EVMStorage::get_balance(const evmc::address& addr) const noexcept
    {
        if(account_exists(addr) == false) 
        {
            spdlog::error(std::format("get_balance : Account {} does not exist", addr));
            return {};
        }
        return evmc::uint256be{_accounts.at(to_key(addr)).balance};
    }

    std::size_t EVMStorage::get_code_size(const evmc::address& addr) const noexcept
    {
        return _accounts.at(to_key(addr)).code.size();
    }

    evmc::bytes32 EVMStorage::get_code_hash(const evmc::address& addr) const noexcept
    {
        if(account_exists(addr) == false) 
        {
            spdlog::error(std::format("get_code_hash : Account {} does not exist", addr));
            return {};
        }

        const auto& code = _accounts.at(to_key(addr)).code;
        evmc::bytes32 hash;
        dcn::Keccak256::getHash((const std::uint8_t*)code.data(), code.size(), hash.bytes);
        return hash;
    }

    std::size_t EVMStorage::copy_code(const evmc::address& addr,
                             std::size_t code_offset,
                             std::uint8_t* buffer_data,
                             std::size_t buffer_size) const noexcept
    {
        spdlog::debug(std::format("copy_code : Account {}, offset {}, buffer_size {}", addr, code_offset, buffer_size));
        const auto& code = _accounts.at(to_key(addr)).code;
        if (code_offset >= code.size())
        {
            spdlog::error(std::format("copy_code : Invalid code offset: {}", code_offset));
            return 0;
        }
        std::size_t copy_size = std::min(buffer_size, code.size() - code_offset);
        std::memcpy(buffer_data, code.data() + code_offset, copy_size);
        return copy_size;
    }

    bool EVMStorage::selfdestruct(const evmc::address& addr, const evmc::address& beneficiary) noexcept
    {
        auto key = to_key(addr);
        auto it = _accounts.find(key);
        if (it == _accounts.end()) return false;

        // Transfer balance
        // implement evmc_uint256be operations
        //_accounts[to_key(beneficiary)].balance += it->second.balance;
        _accounts.erase(it);
        return true;
    }

    evmc::Result EVMStorage::call(const evmc_message& msg) noexcept
    {
        evmc::address actual_sender = msg.sender;

        // Patch: if sender is zero and we are in a nested context
        if (evmc::is_zero(msg.sender) && !_sender_stack.empty()) {
            actual_sender = _sender_stack.top();
        }
        // Push current sender onto the stack
        _sender_stack.push(actual_sender);

        if (msg.kind == EVMC_CALL || msg.kind == EVMC_DELEGATECALL || msg.kind == EVMC_CALLCODE) 
        {
            auto it = _accounts.find(to_key(msg.recipient));

            if (it == _accounts.end())
            {
                spdlog::error(std::format("Account {} does not exist", msg.recipient));
                return evmc::Result{EVMC_FAILURE};
            }

            auto& code = it->second.code;
            if (code.empty())
            {
                spdlog::error(std::format("Account {} has no code", msg.recipient));
                return evmc::Result{EVMC_FAILURE};
            }
            
            evmc_message patched_msg = msg;
            //patched_msg.sender = actual_sender;
            evmc::Result result = _vm.execute(*this, _revision, patched_msg, code.data(), code.size());
            _sender_stack.pop();

            return result;
        }
        else if (msg.kind == EVMC_CREATE || msg.kind == EVMC_CREATE2)
        {
            assert(msg.recipient == evmc::address{});
            evmc_message patched_msg = msg;

            const std::vector<std::uint8_t> init_code(patched_msg.input_data, patched_msg.input_data + patched_msg.input_size);

            // Calculate address
            if(!_create_nonce.contains(actual_sender))
            {
                _create_nonce[actual_sender] = 100;
            }

            evmc_address new_address;
            if (patched_msg.kind == EVMC_CREATE)
            {
                new_address = derive_create_address(actual_sender, _create_nonce[actual_sender]++);
            }
            else // CREATE2
            {
                new_address = derive_create2_address(actual_sender, patched_msg.create2_salt, init_code);
            }

            add_account(new_address);
            set_balance(new_address, DEFAULT_GAS_LIMIT);

            // at this point constructor does not have any address - so any operation - for example store ect 
            // will use default 0x0 address, therefore we use dummy ctor account, 
            // to which constructor code can write its sload, sstore operations ect
            patched_msg.recipient = new_address;

            //patched_msg.sender = actual_sender;
            evmc::Result result = _vm.execute(*this, _revision, patched_msg, init_code.data(), init_code.size());
            _sender_stack.pop();

            if (result.status_code != EVMC_SUCCESS)
            {
                spdlog::error(std::format("Failed to deploy contract: {}", result.status_code));
                return result;
            }

            deploy_contract(new_address, std::vector<std::uint8_t>(result.output_data, result.output_data + result.output_size),
                patched_msg.value, actual_sender, _create_nonce[actual_sender] - 1);

            result.create_address = new_address;
            
            spdlog::debug(std::format("EVMC create call from {} to {} ended", actual_sender, new_address));
            return result;
        }

        spdlog::error(std::format("Unsupported message kind: {}", msg.kind));
        return evmc::Result{EVMC_FAILURE};
    }

    evmc_tx_context EVMStorage::get_tx_context() const noexcept
    {
        evmc_tx_context ctx{};

        // Set a dummy gas price 
        //ctx.tx_gas_price.bytes[31] = 1;

        // Set the origin (e.g. zero address or some predefined test sender)
        ctx.tx_origin = {}; // All zeroes by default

        // Block metadata
        ctx.block_coinbase = {};                   // Miner address (can be zero)
        ctx.block_number = 123456;                 // Arbitrary block height
        ctx.block_timestamp = static_cast<int64_t>(
            std::chrono::duration_cast<std::chrono::seconds>(
                std::chrono::system_clock::now().time_since_epoch()
            ).count()
        );
        ctx.block_gas_limit = DEFAULT_GAS_LIMIT;   // gas limit
        ctx.block_prev_randao = {};                // Randomness (EIP-4399)
        
        ctx.block_base_fee = {};
        //ctx.block_base_fee.bytes[31] = 1;          // Base fee (EIP-1559)

        // Chain ID (mainnet = 1)
        //ctx.chain_id.bytes[31] = 1;

        // Leave EIP-4844 blob fields null
        ctx.blob_hashes = nullptr;
        ctx.blob_hashes_count = 0;

        // Leave EIP-7685 TXCREATE initcodes empty
        ctx.initcodes = nullptr;
        ctx.initcodes_count = 0;

        return ctx;
    }

    evmc::bytes32 EVMStorage::get_block_hash(int64_t block_number) const noexcept
    {
        // implement
        assert(false && "not implemented");
        return {};
    }

    static std::string _decodeString(const std::uint8_t* data, std::size_t data_size, std::size_t offset)
    {
        if (offset + 32 > data_size) return "<out-of-bounds>";

        std::uint64_t len = 0;
        for (int i = 0; i < 32; ++i)
            len = (len << 8) | data[offset + i];

        if (offset + 32 + len > data_size) return "<out-of-bounds>";

        return std::string(reinterpret_cast<const char*>(data + offset + 32), len);
    }


    void EVMStorage::emit_log(const evmc::address& addr,
                              const std::uint8_t* data,
                              size_t data_size,
                              const evmc::bytes32 topics[],
                              size_t num_topics) noexcept
    {
        std::string label = "<n/a>";
        std::string value = "<n/a>";

        if (data_size >= 64) {
            uint64_t offset_label = 0, offset_value = 0;
            for (int i = 0; i < 32; ++i)
                offset_label = (offset_label << 8) | data[i];
            for (int i = 0; i < 32; ++i)
                offset_value = (offset_value << 8) | data[32 + i];

            label = _decodeString(data, data_size, offset_label);
            value = _decodeString(data, data_size, offset_value);
        }

        // Format topics
        std::string topics_str;
        for (size_t i = 0; i < num_topics; ++i)
            topics_str += std::format("Topic[{}]: {}\n", i, topics[i]);

        spdlog::info(std::format("Log from {}:\n  Data: {} bytes\n  Label: {}\n  Value: {}\n  Topics:\n{}",
                                 addr, data_size, label, value, topics_str));
    }

    evmc_access_status EVMStorage::access_account(const evmc::address& addr) noexcept
    {
        if (account_exists(addr))
            return EVMC_ACCESS_WARM;
        return EVMC_ACCESS_COLD;
    }

    evmc_access_status EVMStorage::access_storage(const evmc::address& addr, const evmc::bytes32& key) noexcept
    {
        if (account_exists(addr))
            return EVMC_ACCESS_WARM;
        return EVMC_ACCESS_COLD;
    }

    evmc::bytes32 EVMStorage::get_transient_storage(const evmc::address& addr,
                                          const evmc::bytes32& key) const noexcept
    {
        // implement
        assert(false && "not implemented");

        return {};
    }

    void EVMStorage::set_transient_storage(const evmc::address& addr,
                                       const evmc::bytes32& key,
                                       const evmc::bytes32& value) noexcept
    {
        // implement
        assert(false && "not implemented");
    }


    std::string EVMStorage::to_key(const evmc::address& addr) const
    {
        return std::string(reinterpret_cast<const char*>(addr.bytes), sizeof(addr.bytes));
    }

    evmc_address EVMStorage::derive_create_address(const evmc::address& sender, std::uint64_t nonce)
    {
        std::array<std::uint8_t, 64> data{};
        std::memcpy(data.data(), sender.bytes, 20);
        std::memcpy(data.data() + 20, &nonce, sizeof(nonce));
            
        evmc::bytes32 hash;
        dcn::Keccak256::getHash((const std::uint8_t*)data.data(), 20 + sizeof(nonce), hash.bytes);

        evmc_address addr;
        std::memcpy(addr.bytes, &hash.bytes[12], 20);
        return addr;
    }

    evmc_address EVMStorage::derive_create2_address(const evmc::address& sender, const evmc::bytes32& salt, const std::vector<std::uint8_t>& code)
    {
        std::vector<std::uint8_t> data;
        data.reserve(1 + 20 + 32 + 32);
        data.push_back(0xff);
        data.insert(data.end(), sender.bytes, sender.bytes + 20);
        data.insert(data.end(), salt.bytes, salt.bytes + 32);

        evmc::bytes32 code_hash;
        dcn::Keccak256::getHash((const std::uint8_t*)code.data(), code.size(), code_hash.bytes);

        data.insert(data.end(), code_hash.bytes, code_hash.bytes + 32);

        evmc::bytes32 hash;
        dcn::Keccak256::getHash((const std::uint8_t*)data.data(), data.size(), hash.bytes);

        evmc_address addr;
        std::memcpy(addr.bytes, &hash.bytes[12], 20);
        return addr;
    }

    void EVMStorage::deploy_contract(evmc::address addr,
                    std::vector<std::uint8_t>&& code,
                    evmc_uint256be value,
                    evmc::address creator,
                    std::uint64_t nonce)
    {
        if(account_exists(addr) == false) 
        {
            spdlog::error(std::format("deploy_contract: Account {} does not exists", addr));
            return;
        }

        spdlog::debug(std::format("Deploying contract to {} by {}", addr,creator));
        auto& account = _accounts[to_key(addr)];
        account.code = std::move(code);
        account.balance = value;
        account.creator = creator;
        account.nonce = nonce;
        account.timestamp = static_cast<std::uint64_t>(std::chrono::seconds(std::time(nullptr)).count());
    }
}