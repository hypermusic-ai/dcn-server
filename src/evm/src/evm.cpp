#include "evm.hpp"

namespace dcn
{
    std::vector<std::uint8_t> constructSelector(std::string signature)
    {
        std::uint8_t hash[32];
        Keccak256::getHash(reinterpret_cast<const uint8_t*>(signature.data()), signature.size(), hash);
        return std::vector<std::uint8_t>(hash, hash + 4);
    }

    template<>
    std::vector<std::uint8_t> encodeAsArg<evmc::address>(const evmc::address & address)
    {
        std::vector<std::uint8_t> encoded(32, 0); // Initialize with 32 zero bytes
        std::copy(address.bytes, address.bytes + 20, encoded.begin() + 12); // Right-align in last 20 bytes
        return encoded;
    }

    template<>
    std::vector<std::uint8_t> encodeAsArg<std::uint32_t>(const std::uint32_t & value)
    {
        std::vector<std::uint8_t> encoded(32, 0); // Initialize with 32 zero bytes

        // Encode as big-endian and place in the last 4 bytes (right-aligned)
        encoded[28] = static_cast<std::uint8_t>((value >> 24) & 0xFF);
        encoded[29] = static_cast<std::uint8_t>((value >> 16) & 0xFF);
        encoded[30] = static_cast<std::uint8_t>((value >> 8) & 0xFF);
        encoded[31] = static_cast<std::uint8_t>(value & 0xFF);

        return encoded;
    }

    template<>
    std::vector<std::uint8_t> encodeAsArg<std::vector<std::uint32_t>>(const std::vector<std::uint32_t>& vec)
    {
        std::vector<std::uint8_t> encoded;

        // Step 1: offset to the data section (0x20 = 32)
        encoded.resize(32, 0);
        encoded[31] = 0x20; // offset is 32 bytes

        // Step 2: dynamic data section begins
        std::vector<std::uint8_t> data;

        // 2.1: encode length (number of elements)
        data.resize(32, 0);
        std::uint32_t length = static_cast<std::uint32_t>(vec.size());
        data[28] = static_cast<std::uint8_t>((length >> 24) & 0xFF);
        data[29] = static_cast<std::uint8_t>((length >> 16) & 0xFF);
        data[30] = static_cast<std::uint8_t>((length >> 8) & 0xFF);
        data[31] = static_cast<std::uint8_t>(length & 0xFF);

        // 2.2: encode each element (right-aligned uint32 in 32 bytes)
        for (const std::uint32_t val : vec)
        {
            std::vector<std::uint8_t> element(32, 0);
            element[28] = static_cast<std::uint8_t>((val >> 24) & 0xFF);
            element[29] = static_cast<std::uint8_t>((val >> 16) & 0xFF);
            element[30] = static_cast<std::uint8_t>((val >> 8) & 0xFF);
            element[31] = static_cast<std::uint8_t>(val & 0xFF);
            data.insert(data.end(), element.begin(), element.end());
        }

        // Step 3: append the data section after the 32-byte offset
        encoded.insert(encoded.end(), data.begin(), data.end());

        return encoded;
    }

    template<>
    std::vector<std::uint8_t> encodeAsArg<std::vector<std::tuple<std::uint32_t, std::uint32_t>>>(const std::vector<std::tuple<std::uint32_t, std::uint32_t>>& vec)
    {
        std::vector<std::uint8_t> encoded;

        // Encode length
        std::vector<std::uint8_t> length(32, 0);
        uint64_t len = vec.size();
        for (int i = 0; i < 8; ++i)
            length[31 - i] = static_cast<uint8_t>(len >> (i * 8));
        
        encoded.insert(encoded.end(), length.begin(), length.end());

        // Encode each pair as two int32s in a 32-byte ABI word each
        for (const auto& [a, b] : vec)
        {
            std::vector<std::uint8_t> elem(32, 0);
            for (int i = 0; i < 4; ++i)
                elem[31 - i] = static_cast<uint8_t>(a >> (i * 8));
            
            encoded.insert(encoded.end(), elem.begin(), elem.end());

            std::fill(elem.begin(), elem.end(), 0);
            for (int i = 0; i < 4; ++i)
                elem[31 - i] = static_cast<uint8_t>(b >> (i * 8));
            
            encoded.insert(encoded.end(), elem.begin(), elem.end());
        }

        return encoded;
    }

    template<>
    std::vector<std::uint8_t> encodeAsArg<std::string>(const std::string& str)
    {
        std::vector<std::uint8_t> encoded;

        std::vector<uint8_t> len_enc(32, 0);
        len_enc[31] = static_cast<uint8_t>(str.size());

        encoded.insert(encoded.end(), len_enc.begin(), len_enc.end()); // string length
        encoded.insert(encoded.end(), str.begin(), str.end());             // string bytes

        // pad to multiple of 32 bytes
        size_t pad = (32 - (str.size() % 32)) % 32;
        encoded.insert(encoded.end(), pad, 0);
        
        return encoded;
    }


    static std::uint32_t _readUint32(const std::vector<std::uint8_t> & bytes, std::size_t offset) {
        std::uint32_t value = 0;
        for (std::size_t i = 0; i < 4; ++i) {
            value <<= 8;
            value |= bytes[offset + i];
        }
        return value;
    }

    static std::uint32_t _readUint32Padded(const std::vector<uint8_t>& bytes, std::size_t offset) {
        // Read last 4 bytes of 32-byte ABI word
        assert(offset + 32 <= bytes.size());
        uint32_t value = 0;
        for (int i = 28; i < 32; ++i) {
            value = (value << 8) | bytes[offset + i];
        }
        return value;
    }

    static std::uint64_t _readUint256(const std::vector<std::uint8_t> & bytes, std::size_t offset) {
        std::uint64_t value = 0;
        for (std::size_t i = 0; i < 32; ++i) {
            value <<= 8;
            value |= bytes[offset + i];
        }
        return value;
    }


    static std::uint64_t _readOffset(const std::vector<std::uint8_t> & bytes, std::size_t offset) {
        std::size_t return_offset = 0;
        for (int i = 0; i < 32; ++i)
            return_offset = (return_offset << 8) | bytes[offset + i];
        return_offset += 32;
        return return_offset;
    }

    template<class E>
    static E _decodeError(const evmc::Result& r);

    template<>
    static DeployError _decodeError(const evmc::Result& r)
    {
        std::vector<std::uint8_t> selector;

        selector = constructSelector("FeatureAlreadyRegistered(bytes32)");
        if (std::equal(selector.begin(), selector.end(), r.output_data))
        {
            return DeployError{DeployError::Kind::FEATURE_ALREADY_REGISTERED};
        }

        selector = constructSelector("FeatureMissing(bytes32)");
        if (std::equal(selector.begin(), selector.end(), r.output_data))
        {
            return DeployError{DeployError::Kind::FEATURE_MISSING};
        }

        selector = constructSelector("TransformationAlreadyRegistered(bytes32)");
        if (std::equal(selector.begin(), selector.end(), r.output_data))
        {
            return DeployError{DeployError::Kind::TRANSFORMATION_ALREADY_REGISTERED};
        }

        selector = constructSelector("TransformationArgumentsMismatch(bytes32)");
        if (std::equal(selector.begin(), selector.end(), r.output_data))
        {
            return DeployError{DeployError::Kind::TRANSFORMATION_ARGUMENTS_MISMATCH};
        }

        selector = constructSelector("TransformationMissing(bytes32)");
        if (std::equal(selector.begin(), selector.end(), r.output_data))
        {
            return DeployError{DeployError::Kind::TRANSFORMATION_MISSING};
        }

        selector = constructSelector("RunInstanceAlreadyRegistered(bytes32, bytes32)");
        if (std::equal(selector.begin(), selector.end(), r.output_data))
        {
            return DeployError{DeployError::Kind::RUN_INSTANCE_ALREADY_REGISTERED};
        }

        selector = constructSelector("RunInstanceMissing(bytes32, bytes32)");
        if (std::equal(selector.begin(), selector.end(), r.output_data))
        {
            return DeployError{DeployError::Kind::RUN_INSTANCE_MISSING};
        }

        selector = constructSelector("RegistryError(bytes32)");
        if (std::equal(selector.begin(), selector.end(), r.output_data))
        {
            return DeployError{DeployError::Kind::REGISTRY_ERROR};
        }

        return DeployError{};
    }


    template<>
    std::vector<std::vector<uint32_t>> decodeReturnedValue(const std::vector<std::uint8_t> & bytes)
    {
        assert(bytes.size() % 32 == 0);
        std::vector<std::vector<uint32_t>> result;

        std::size_t base_offset = _readUint256(bytes, 0);  // normally 32
        std::size_t offset = base_offset;

        uint64_t outer_len = _readUint256(bytes, offset);
        offset += 32;

        std::vector<std::size_t> inner_offsets;
        for (uint64_t i = 0; i < outer_len; ++i) {
            std::uint64_t inner_offset = _readUint256(bytes, offset);
            inner_offsets.push_back(inner_offset + base_offset + 32);
            offset += 32;
        }

        for (std::size_t inner_offset : inner_offsets) 
        {
            if (inner_offset + 32 > bytes.size()) {
                throw std::runtime_error("Inner array header out of range");
            }

            std::uint64_t inner_len = _readUint256(bytes, inner_offset);
            inner_offset += 32;

            std::vector<uint32_t> inner_array;
            for (std::uint64_t j = 0; j < inner_len; ++j) {
                std::uint32_t val = _readUint32Padded(bytes, inner_offset + (j * 32));
                inner_array.push_back(val);
            }
            result.push_back(std::move(inner_array));
        }

        return result;
    }

    template<>
    evmc::address decodeReturnedValue(const std::vector<std::uint8_t> & bytes)
    {
        if (bytes.size() < 32)
            throw std::runtime_error("Invalid ABI data: less than 32 bytes");

        evmc::address result;
        // Copy last 20 bytes from the 32-byte word (ABI stores address in the last 20 bytes)
        std::copy(bytes.begin() + 12, bytes.begin() + 32, result.bytes);
        return result;
    }


    template<>
    std::vector<Samples> decodeReturnedValue(const std::vector<std::uint8_t>& bytes)
    {
        std::vector<Samples> result;

        // Step 1: read base offset to array
        std::size_t array_base = _readUint256(bytes, 0);  // should be 32

        // Step 2: read array length
        std::size_t array_len = _readUint256(bytes, array_base);  // at offset 32

        // Step 3: read offsets to structs (relative to array_base)
        std::vector<std::size_t> struct_offsets;
        for (std::size_t i = 0; i < array_len; ++i) {
            std::size_t struct_rel_offset = _readUint256(bytes, (array_base + 32) + (i * 32));
            struct_offsets.push_back((array_base + 32) + struct_rel_offset);
        }

        // Step 4: parse each struct
        for (std::size_t struct_offset : struct_offsets) 
        {
            Samples samples;

            std::size_t feature_path_rel = _readUint256(bytes, struct_offset);
            std::size_t data_rel         = _readUint256(bytes, struct_offset + 32);

            // 4.2: resolve actual offsets
            std::size_t feature_path_offset = struct_offset + feature_path_rel;
            std::size_t data_offset         = struct_offset + data_rel;

            // 4.3: read string length and content
            std::size_t str_len = _readUint256(bytes, feature_path_offset);

            samples.set_feature_path(std::string(
                reinterpret_cast<const char*>(&bytes[feature_path_offset + 32]),
                str_len
            ));

            // 4.4: read data length and entries
            std::size_t data_len = _readUint256(bytes, data_offset);
            std::vector<std::uint32_t> data;
            for (std::size_t j = 0; j < data_len; ++j) {
                std::uint32_t val = _readUint32Padded(bytes, data_offset + 32 + j * 32);
                samples.add_data(val);
            }

            result.emplace_back(std::move(samples));
        }

        return result;
    }

    asio::awaitable<std::expected<std::vector<std::uint8_t>, evmc_status_code>> fetchOwner(EVM & evm, const evmc::address & address)
    {
        spdlog::debug(std::format("Fetching contract owner: {}", address));
        std::vector<uint8_t> input_data;
        const auto selector = constructSelector("getOwner()");
        input_data.insert(input_data.end(), selector.begin(), selector.end());
        co_return  co_await evm.execute(evm.getRegistryAddress(), address, input_data, 1'000'000, 0);
    }

    EVM::EVM(asio::io_context & io_context, evmc_revision rev, std::filesystem::path solc_path)
    :   _vm(evmc_create_evmone()),
        _rev(rev),
        _strand(asio::make_strand(io_context)),
        _solc_path(std::move(solc_path)),
        _storage(_vm, _rev)
    {
        if (!_vm)
        {
            throw std::runtime_error("Failed to create EVM instance");
        }

        _vm.set_option("O", "0"); // disable optimizations

        // Initialize the genesis account
        std::memcpy(_genesis_address.bytes + (20 - 7), "genesis", 7);
        addAccount(_genesis_address, DEFAULT_GAS_LIMIT);
        spdlog::info(std::format("Genesis address: {}", _genesis_address));

        // Initialize console log account
        std::memcpy(_console_log_address.bytes + (20 - 11), "console.log", 11);
        addAccount(_console_log_address, DEFAULT_GAS_LIMIT);

        co_spawn(io_context, loadPT(), [](std::exception_ptr e, bool r){
            if(e || !r)
            {
                spdlog::error("Failed to load PT");
                throw std::runtime_error("Failed to load PT");
            }
        });
    }
    
    evmc::address EVM::getRegistryAddress() const
    {
        return _registry_address;
    }

    evmc::address EVM::getRunnerAddress() const
    {
        return _runner_address;
    }

    asio::awaitable<bool> EVM::addAccount(evmc::address address, std::uint64_t initial_gas) noexcept
    {
        co_await utils::ensureOnStrand(_strand);

        if(_storage.account_exists(address))
        {
            spdlog::warn(std::format("addAccount: Account {} already exists", evmc::hex(address)));
            co_return false;
        }

        if(_storage.add_account(address))
        {
            _storage.set_balance(address, initial_gas);
        }
        else
        {
            co_return false;
        }

        co_return true;
    }

    asio::awaitable<bool> EVM::setGas(evmc::address address, std::uint64_t gas) noexcept
    {
        co_await utils::ensureOnStrand(_strand);

        if(!_storage.account_exists(address))
        {
            spdlog::warn(std::format("addAccount: Account {} does not exist", evmc::hex(address)));
            co_return false;
        }

        _storage.set_balance(address, gas);
        co_return true;
    }

    asio::awaitable<bool> EVM::compile(std::filesystem::path code_path, std::filesystem::path out_dir, std::filesystem::path base_path, std::filesystem::path includes) const noexcept
    {
        co_await utils::ensureOnStrand(_strand);

        if(!std::filesystem::exists(code_path))
        {
            spdlog::error(std::format("File {} does not exist", code_path.string()));
            co_return false;
        }

        std::vector<std::string> args = {
            "--evm-version", "shanghai",
            "--overwrite", "-o", out_dir.string(),
            "--optimize", "--bin",
            "--abi",
            code_path.string()
        };

        if(!includes.empty() && base_path.empty())
        {
            spdlog::error("Base path must be specified if includes are specified");
            co_return false;
        }

        if (!base_path.empty()) 
        {
            args.emplace_back("--base-path");
            args.emplace_back(base_path.string());
        }

        if (!includes.empty()) 
        {
            args.emplace_back("--include-path");
            args.emplace_back(includes.string());
        }

        const auto [exit_code, compile_result] = dcn::native::runProcess(_solc_path.string(), std::move(args));

        spdlog::info("Solc exited with code {},\n{}\n{}", exit_code, code_path.string(), compile_result);

        if(exit_code != 0)
        {
            co_return false;
        }
        
        co_return true;
    }

    asio::awaitable<std::expected<evmc::address, DeployError>> EVM::deploy(
                        std::istream & code_stream,
                        evmc::address sender,
                        std::vector<std::uint8_t> constructor_args, 
                        std::uint64_t gas_limit,
                        std::uint64_t value) noexcept
    {
        const std::string code_hex = std::string(std::istreambuf_iterator<char>(code_stream), std::istreambuf_iterator<char>());
        const std::optional<evmc::bytes> bytecode_result = evmc::from_hex(code_hex);
        if(!bytecode_result)
        {
            spdlog::error("Cannot parse bytecode");
            co_return std::unexpected(DeployError{DeployError::Kind::INVALID_BYTECODE});
        }
        const auto & bytecode = *bytecode_result;

        if(bytecode.size() == 0)
        {
            spdlog::error("Empty bytecode");
            co_return std::unexpected(DeployError{DeployError::Kind::INVALID_BYTECODE});
        }

        if(!constructor_args.empty())
        {
            std::string hex_str;
            for(const std::uint8_t & b : constructor_args)
            {
                hex_str += evmc::hex(b);
            }

            spdlog::debug(std::format("Constructor args: {}", hex_str));
        }

        std::vector<uint8_t> deployment_input;
        deployment_input.reserve(bytecode.size() + constructor_args.size());
        deployment_input.insert(deployment_input.end(), bytecode.begin(), bytecode.end());
        deployment_input.insert(deployment_input.end(), constructor_args.begin(), constructor_args.end());

        evmc_message create_msg{};
        create_msg.kind       = EVMC_CREATE2;
        create_msg.sender     = sender;
        std::memcpy(create_msg.sender.bytes, sender.bytes, 20);
        
        create_msg.gas        = gas_limit;
        create_msg.input_data = deployment_input.data();
        create_msg.input_size = deployment_input.size();

        // fill message salt
        std::string salt_str = "message_salt_42";
        Keccak256::getHash(reinterpret_cast<const uint8_t*>(salt_str.data()), salt_str.size(), create_msg.create2_salt.bytes);

        // fill message value
        evmc_uint256be value256{};
        std::memcpy(&value256.bytes[24], &value, sizeof(value));  // Big endian: last 8 bytes hold the value
        create_msg.value = value256;

        co_await utils::ensureOnStrand(_strand);

        const evmc::Result result = _storage.call(create_msg);        

        if (result.status_code != EVMC_SUCCESS)
        {
            const DeployError error = _decodeError<DeployError>(result);
            spdlog::error(std::format("Failed to deploy contract: {}, error: {}", result.status_code, error.kind));
            co_return std::unexpected(error);
        }

        // Display result
        spdlog::info("EVM deployment status: {}", evmc_status_code_to_string(result.status_code));
        spdlog::info("Gas left: {}", result.gas_left);

        if (result.output_data){
            spdlog::debug("Output size: {}", result.output_size);
        }

        co_return result.create_address;
     }

    asio::awaitable<std::expected<evmc::address, DeployError>> EVM::deploy(
                        std::filesystem::path code_path,
                        evmc::address sender,
                        std::vector<uint8_t> constructor_args,
                        std::uint64_t gas_limit,
                        std::uint64_t value) noexcept
    {
        spdlog::debug(std::format("Deploying contract from file: {}", code_path.string()));
        std::ifstream file(code_path, std::ios::binary);
        co_return co_await deploy(file, std::move(sender), std::move(constructor_args),  gas_limit, value);
    }

    asio::awaitable<std::expected<std::vector<std::uint8_t>, evmc_status_code>> EVM::execute(
                    evmc::address sender,
                    evmc::address recipient,
                    std::vector<std::uint8_t> input_bytes,
                    std::uint64_t gas_limit,
                    std::uint64_t value) noexcept
    {
        if(std::ranges::all_of(recipient.bytes, [](uint8_t b) { return b == 0; }))
        {
            spdlog::error("Cannot create a contract with execute function. Use dedicated deploy method.");
            co_return std::unexpected<evmc_status_code>(EVMC_FAILURE);
        }

        evmc_message msg{};
        msg.gas = gas_limit;
        msg.kind = EVMC_CALL;
        msg.sender = sender;
        msg.recipient = recipient;

        if(!input_bytes.empty())
        {
            msg.input_data = input_bytes.data();
            msg.input_size = input_bytes.size();
        }
        else
        {
            msg.input_data = nullptr;
            msg.input_size = 0;
        }

        evmc_uint256be value256{};
        std::memcpy(&value256.bytes[24], &value, sizeof(value));  // Big endian: last 8 bytes hold the value
        msg.value = value256;

        co_await utils::ensureOnStrand(_strand);
        evmc::Result result = _storage.call(msg);
        
        if (result.status_code != EVMC_SUCCESS)
        {
            spdlog::error("Failed to execute contract: {}", evmc_status_code_to_string(result.status_code));
            co_return std::unexpected<evmc_status_code>(result.status_code);
        }

        // Display result
        spdlog::info("EVM execution status: {}", evmc_status_code_to_string(result.status_code));
        spdlog::info("Gas left: {}", result.gas_left);

        if (result.output_data){
            spdlog::debug("Output size: {}", result.output_size);
        }

        co_return std::vector<std::uint8_t>(result.output_data, result.output_data + result.output_size);
    }

    asio::awaitable<bool> EVM::loadPT()
    { 
        const auto & contracts_dir = getPTPath()    / "contracts";
        const auto & node_modules = getPTPath()     / "node_modules";
        const auto & out_dir = getPTPath()          / "out";

        std::filesystem::create_directories(out_dir);
        
        { // deploy registry
            if(co_await compile(
                    contracts_dir / "registry" / "RegistryBase.sol",
                    out_dir / "registry", 
                    contracts_dir, 
                    node_modules) == false)
            {
                spdlog::error("Failed to compile registry");
                co_return false;
            }

            const auto registry_address_res = co_await deploy(
                    out_dir / "registry" / "RegistryBase.bin", 
                    _genesis_address,
                    {}, 
                    DEFAULT_GAS_LIMIT, 
                    0);

            if(!registry_address_res)
                co_return false;

            _registry_address = registry_address_res.value();
            spdlog::info("Registry address: {}", evmc::hex(_registry_address));
        }

        { // deploy runner
            if(co_await compile(
                    contracts_dir /  "Runner.sol",
                    out_dir, 
                    contracts_dir, 
                    node_modules) == false)
            {
                spdlog::error("Failed to compile runner");
                co_return false;
            }
            
            spdlog::debug("Deploy runner");
            const auto runner_address_res = co_await deploy(
                    out_dir / "Runner.bin", 
                    _genesis_address,
                    encodeAsArg(_registry_address), 
                    DEFAULT_GAS_LIMIT, 
                    0);

            if(!runner_address_res)
                co_return false;
        
            _runner_address = runner_address_res.value();
            spdlog::info("Runner address: {}", evmc::hex(_runner_address));
        }

        co_return true;
    }

}