#include "unit-tests.hpp"

#include <array>
#include <chrono>
#include <cstring>
#include <expected>
#include <filesystem>
#include <format>
#include <future>
#include <optional>
#include <string_view>
#include <tuple>
#include <vector>

#ifndef DECENTRALISED_ART_TEST_BINARY_DIR
    #error "DECENTRALISED_ART_TEST_BINARY_DIR is not defined"
#endif

#ifndef DECENTRALISED_ART_TEST_SOLC_PATH
    #error "DECENTRALISED_ART_TEST_SOLC_PATH is not defined"
#endif

#ifndef DECENTRALISED_ART_TEST_PT_PATH
    #error "DECENTRALISED_ART_TEST_PT_PATH is not defined"
#endif

using namespace dcn;
using namespace dcn::tests;

namespace
{
    constexpr const char * kIdentityTransformationName = "IdentityTx";
    constexpr const char * kIncrementTransformationName = "IncrementTx";
    // Bump cache key whenever generated Solidity or imported PT runtime contracts change.
    constexpr const char * kBindingBuildCacheVersion = "constructors-v3-static-ri-scope";

    const std::array<std::filesystem::path, 3> kBindingBuildDirs{
        std::filesystem::path("connectors") / "build",
        std::filesystem::path("transformations") / "build",
        std::filesystem::path("conditions") / "build",
    };

    template<class AwaitableT>
    auto runAwaitable(asio::io_context & io_context, AwaitableT awaitable)
    {
        auto future = asio::co_spawn(io_context, std::move(awaitable), asio::use_future);
        io_context.restart();
        io_context.run();
        return future.get();
    }

    std::filesystem::path buildPath()
    {
        return std::filesystem::path(DECENTRALISED_ART_TEST_BINARY_DIR);
    }

    std::filesystem::path solcPath()
    {
        return std::filesystem::path(DECENTRALISED_ART_TEST_SOLC_PATH);
    }

    std::filesystem::path ptPath()
    {
        return std::filesystem::path(DECENTRALISED_ART_TEST_PT_PATH);
    }

    chain::Address makeAddressFromSuffix(const char * suffix)
    {
        chain::Address address{};
        const std::size_t suffix_len = std::strlen(suffix);
        const std::size_t copied_len = (suffix_len <= 20) ? suffix_len : 20;
        const std::size_t suffix_offset = suffix_len - copied_len;
        std::memcpy(address.bytes + (20 - copied_len), suffix + suffix_offset, copied_len);
        return address;
    }

    chain::Address makeAddressFromByte(std::uint8_t value)
    {
        chain::Address address{};
        address.bytes[19] = value;
        return address;
    }

    std::filesystem::path makeBindingStoragePath()
    {
        return buildPath() / "tests" / "pt_binding_storage" /
            std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
    }

    std::filesystem::path bindingBuildCachePath()
    {
        return buildPath() / "tests" / "pt_binding_build_cache" / kBindingBuildCacheVersion;
    }

    bool ensureBindingBuildDirectories(const std::filesystem::path & root_path)
    {
        std::error_code ec;
        for(const auto & build_dir : kBindingBuildDirs)
        {
            ec.clear();
            std::filesystem::create_directories(root_path / build_dir, ec);
            if(ec)
            {
                spdlog::error("Failed to create binding build directory '{}': {}", (root_path / build_dir).string(), ec.message());
                return false;
            }
        }

        return true;
    }

    std::uint64_t fnv1a64(std::string_view input)
    {
        std::uint64_t hash = 14695981039346656037ULL;
        for(const unsigned char byte : input)
        {
            hash ^= byte;
            hash *= 1099511628211ULL;
        }
        return hash;
    }

    std::string hashHex(std::uint64_t hash)
    {
        return std::format("{:016x}", hash);
    }

    std::optional<std::string> makeConnectorBuildCacheKey(const Connector & connector)
    {
        const auto solidity_result = constructConnectorSolidityCode(connector);
        if(!solidity_result)
        {
            spdlog::debug(
                "Skipping connector cache key for '{}': parse error kind={} ({})",
                connector.name(),
                static_cast<int>(solidity_result.error().kind),
                solidity_result.error().message);
            return std::nullopt;
        }

        return hashHex(fnv1a64(*solidity_result));
    }

    std::optional<std::string> makeTransformationBuildCacheKey(const Transformation & transformation)
    {
        const auto solidity_result = constructTransformationSolidityCode(transformation);
        if(!solidity_result)
        {
            spdlog::debug(
                "Skipping transformation cache key for '{}': parse error kind={} ({})",
                transformation.name(),
                static_cast<int>(solidity_result.error().kind),
                solidity_result.error().message);
            return std::nullopt;
        }

        return hashHex(fnv1a64(*solidity_result));
    }

    std::filesystem::path makeCachedArtifactPath(
        const std::filesystem::path & cache_root,
        const char * entity_dir,
        const std::string & name,
        const std::string & cache_key,
        const char * extension)
    {
        return cache_root / entity_dir / "build" / std::format("{}__{}{}", name, cache_key, extension);
    }

    void restoreCachedBuildArtifacts(
        const std::filesystem::path & storage_path,
        const char * entity_dir,
        const std::string & name,
        const std::string & cache_key)
    {
        const auto cache_root = bindingBuildCachePath();
        if(!ensureBindingBuildDirectories(cache_root))
        {
            return;
        }

        const auto cached_bin = makeCachedArtifactPath(cache_root, entity_dir, name, cache_key, ".bin");
        const auto cached_abi = makeCachedArtifactPath(cache_root, entity_dir, name, cache_key, ".abi");
        if(!std::filesystem::exists(cached_bin) || !std::filesystem::exists(cached_abi))
        {
            return;
        }

        const auto local_build_dir = storage_path / entity_dir / "build";
        std::error_code ec;

        std::filesystem::copy_file(cached_bin, local_build_dir / (name + ".bin"), std::filesystem::copy_options::overwrite_existing, ec);
        if(ec)
        {
            spdlog::warn("Failed to restore cached bin '{}' -> '{}': {}", cached_bin.string(), (local_build_dir / (name + ".bin")).string(), ec.message());
            return;
        }

        ec.clear();
        std::filesystem::copy_file(cached_abi, local_build_dir / (name + ".abi"), std::filesystem::copy_options::overwrite_existing, ec);
        if(ec)
        {
            spdlog::warn("Failed to restore cached abi '{}' -> '{}': {}", cached_abi.string(), (local_build_dir / (name + ".abi")).string(), ec.message());
            return;
        }
    }

    void storeCachedBuildArtifacts(
        const std::filesystem::path & storage_path,
        const char * entity_dir,
        const std::string & name,
        const std::string & cache_key)
    {
        const auto local_build_dir = storage_path / entity_dir / "build";
        const auto local_bin = local_build_dir / (name + ".bin");
        const auto local_abi = local_build_dir / (name + ".abi");
        if(!std::filesystem::exists(local_bin) || !std::filesystem::exists(local_abi))
        {
            return;
        }

        const auto cache_root = bindingBuildCachePath();
        if(!ensureBindingBuildDirectories(cache_root))
        {
            return;
        }

        const auto cached_bin = makeCachedArtifactPath(cache_root, entity_dir, name, cache_key, ".bin");
        const auto cached_abi = makeCachedArtifactPath(cache_root, entity_dir, name, cache_key, ".abi");
        std::error_code ec;

        std::filesystem::copy_file(local_bin, cached_bin, std::filesystem::copy_options::overwrite_existing, ec);
        if(ec)
        {
            spdlog::warn("Failed to cache bin '{}' -> '{}': {}", local_bin.string(), cached_bin.string(), ec.message());
            return;
        }

        ec.clear();
        std::filesystem::copy_file(local_abi, cached_abi, std::filesystem::copy_options::overwrite_existing, ec);
        if(ec)
        {
            spdlog::warn("Failed to cache abi '{}' -> '{}': {}", local_abi.string(), cached_abi.string(), ec.message());
        }
    }

    struct BindingStorageScope
    {
        explicit BindingStorageScope(std::filesystem::path storage_path_)
            : storage_path(std::move(storage_path_))
        {
        }

        ~BindingStorageScope()
        {
            std::error_code ec;
            std::filesystem::remove_all(storage_path, ec);
            if(ec)
            {
                spdlog::warn("Failed to cleanup binding storage '{}': {}", storage_path.string(), ec.message());
            }
        }

        std::filesystem::path storage_path;
    };

    bool prepareBindingStorageDirectories(const std::filesystem::path & storage_path)
    {
        std::error_code ec;
        std::filesystem::remove_all(storage_path, ec);
        if(ec)
        {
            spdlog::error("Failed to cleanup binding storage '{}': {}", storage_path.string(), ec.message());
            return false;
        }

        for(const auto & build_dir : kBindingBuildDirs)
        {
            ec.clear();
            std::filesystem::create_directories(storage_path / build_dir, ec);
            if(ec)
            {
                spdlog::error("Failed to create binding storage directory '{}': {}", (storage_path / build_dir).string(), ec.message());
                return false;
            }
        }

        return true;
    }

    TransformationRecord makeTransformationRecord(
        const std::string & name,
        const std::string & sol_src,
        const std::string & owner_hex)
    {
        TransformationRecord record;
        record.mutable_transformation()->set_name(name);
        record.mutable_transformation()->set_sol_src(sol_src);
        record.set_owner(owner_hex);
        return record;
    }

    TransformationRecord makeIdentityTransformationRecord(const std::string & owner_hex)
    {
        return makeTransformationRecord(kIdentityTransformationName, "return x;", owner_hex);
    }

    ConnectorRecord makeConnectorRecord(const std::string & name, const std::string & owner_hex)
    {
        ConnectorRecord record;
        record.mutable_connector()->set_name(name);
        record.set_owner(owner_hex);
        return record;
    }

    void addDimensionWithTransformation(
        ConnectorRecord & record,
        const std::string & composite,
        const std::string & transformation_name,
        std::initializer_list<std::pair<std::string, std::string>> bindings = {})
    {
        auto * dimension = record.mutable_connector()->add_dimensions();
        dimension->set_composite(composite);

        auto * transformation = dimension->add_transformations();
        transformation->set_name(transformation_name);

        for(const auto & [slot, binding_target] : bindings)
        {
            (*dimension->mutable_bindings())[slot] = binding_target;
        }
    }

    void addDimension(
        ConnectorRecord & record,
        const std::string & composite,
        std::initializer_list<std::pair<std::string, std::string>> bindings = {})
    {
        addDimensionWithTransformation(record, composite, kIdentityTransformationName, bindings);
    }

    bool addIdentityTransformation(
        asio::io_context & io_context,
        registry::Registry & registry,
        const std::string & owner_hex,
        std::uint8_t address_byte)
    {
        return runAwaitable(
            io_context,
            registry.addTransformation(
                makeAddressFromByte(address_byte),
                makeIdentityTransformationRecord(owner_hex)));
    }

    bool addConnectorRecord(
        asio::io_context & io_context,
        registry::Registry & registry,
        std::uint8_t address_byte,
        ConnectorRecord record)
    {
        return runAwaitable(
            io_context,
            registry.addConnector(makeAddressFromByte(address_byte), std::move(record)));
    }

    bool addScalarConnector(
        asio::io_context & io_context,
        registry::Registry & registry,
        const std::string & name,
        const std::string & owner_hex,
        std::uint8_t address_byte,
        std::uint32_t dimensions_count = 1)
    {
        ConnectorRecord record = makeConnectorRecord(name, owner_hex);
        for(std::uint32_t i = 0; i < dimensions_count; ++i)
        {
            addDimension(record, "");
        }

        return addConnectorRecord(io_context, registry, address_byte, std::move(record));
    }

    std::expected<chain::Address, std::string> deployTransformationRecord(
        asio::io_context & io_context,
        evm::EVM & evm_instance,
        registry::Registry & registry,
        TransformationRecord record,
        const std::filesystem::path & storage_path)
    {
        const std::string transformation_name = record.transformation().name();
        const auto cache_key = makeTransformationBuildCacheKey(record.transformation());
        if(cache_key)
        {
            restoreCachedBuildArtifacts(storage_path, "transformations", record.transformation().name(), *cache_key);
        }

        auto deploy_result = runAwaitable(
            io_context,
            loader::deployTransformation(
                evm_instance,
                registry,
                std::move(record),
                storage_path));

        if(!deploy_result)
        {
            return std::unexpected(std::format("deployTransformation failed: {}", deploy_result.error().kind));
        }

        if(cache_key)
        {
            storeCachedBuildArtifacts(storage_path, "transformations", transformation_name, *cache_key);
        }
        return deploy_result.value();
    }

    std::expected<chain::Address, std::string> deployIdentityTransformation(
        asio::io_context & io_context,
        evm::EVM & evm_instance,
        registry::Registry & registry,
        const std::string & owner_hex,
        const std::filesystem::path & storage_path)
    {
        return deployTransformationRecord(
            io_context,
            evm_instance,
            registry,
            makeIdentityTransformationRecord(owner_hex),
            storage_path);
    }

    std::expected<chain::Address, std::string> deployConnectorRecord(
        asio::io_context & io_context,
        evm::EVM & evm_instance,
        registry::Registry & registry,
        ConnectorRecord record,
        const std::filesystem::path & storage_path)
    {
        const std::string name = record.connector().name();
        const auto cache_key = makeConnectorBuildCacheKey(record.connector());
        if(cache_key)
        {
            restoreCachedBuildArtifacts(storage_path, "connectors", name, *cache_key);
        }

        auto deploy_result = runAwaitable(
            io_context,
            loader::deployConnector(
                evm_instance,
                registry,
                std::move(record),
                storage_path));

        if(!deploy_result)
        {
            return std::unexpected(std::format("deployConnector failed: {}", deploy_result.error().kind));
        }

        if(cache_key)
        {
            storeCachedBuildArtifacts(storage_path, "connectors", name, *cache_key);
        }

        return deploy_result.value();
    }

    std::vector<std::uint8_t> makeRunnerGenInput(
        const std::string & connector_name,
        std::uint32_t particles_count,
        const std::vector<std::tuple<std::uint32_t, std::uint32_t, std::uint32_t>> & dynamic_ri)
    {
        std::vector<std::uint8_t> input_data;

        const auto selector = chain::constructSelector("gen(string,uint32,(uint32,uint32,uint32)[])");
        input_data.insert(input_data.end(), selector.begin(), selector.end());

        std::vector<std::uint8_t> offset_to_string(32, 0);
        offset_to_string[31] = 0x60;
        input_data.insert(input_data.end(), offset_to_string.begin(), offset_to_string.end());

        const auto particles_count_arg = evm::encodeAsArg(particles_count);
        input_data.insert(input_data.end(), particles_count_arg.begin(), particles_count_arg.end());

        const auto connector_name_arg = evm::encodeAsArg(connector_name);

        std::vector<std::uint8_t> offset_to_tuple(32, 0);
        const std::size_t tuple_offset = 0x60 + connector_name_arg.size();
        for(std::size_t i = 0; i < sizeof(std::size_t); ++i)
        {
            offset_to_tuple[31 - i] = static_cast<std::uint8_t>((tuple_offset >> (8 * i)) & 0xFFu);
        }
        input_data.insert(input_data.end(), offset_to_tuple.begin(), offset_to_tuple.end());

        input_data.insert(input_data.end(), connector_name_arg.begin(), connector_name_arg.end());

        const auto dynamic_ri_arg = evm::encodeAsArg(dynamic_ri);
        input_data.insert(input_data.end(), dynamic_ri_arg.begin(), dynamic_ri_arg.end());

        return input_data;
    }

    std::expected<std::vector<Particles>, std::string> runGeneration(
        asio::io_context & io_context,
        evm::EVM & evm_instance,
        const chain::Address & owner,
        const std::string & connector_name,
        std::uint32_t particles_count)
    {
        const auto input_data = makeRunnerGenInput(connector_name, particles_count, {});
        auto execute_result = runAwaitable(
            io_context,
            evm_instance.execute(
                owner,
                evm_instance.getRunnerAddress(),
                input_data,
                evm::DEFAULT_GAS_LIMIT,
                0));

        if(!execute_result)
        {
            return std::unexpected(std::format("runner.gen failed: {}", execute_result.error().kind));
        }

        auto particles_result = parse::decodeBytes<std::vector<Particles>>(execute_result.value());
        if(!particles_result)
        {
            return std::unexpected(std::format("Failed to decode particles: {}", particles_result.error().kind));
        }

        return particles_result.value();
    }

    std::expected<pt::PTExecuteError::Kind, std::string> runGenerationExpectRevert(
        asio::io_context & io_context,
        evm::EVM & evm_instance,
        const chain::Address & owner,
        const std::string & connector_name,
        std::uint32_t particles_count,
        const std::vector<std::tuple<std::uint32_t, std::uint32_t, std::uint32_t>> & dynamic_ri)
    {
        const auto input_data = makeRunnerGenInput(connector_name, particles_count, dynamic_ri);
        auto execute_result = runAwaitable(
            io_context,
            evm_instance.execute(
                owner,
                evm_instance.getRunnerAddress(),
                input_data,
                evm::DEFAULT_GAS_LIMIT,
                0));

        if(execute_result)
        {
            return std::unexpected("runner.gen unexpectedly succeeded");
        }

        if(execute_result.error().kind != chain::ExecuteError::Kind::TRANSACTION_REVERTED)
        {
            return std::unexpected(std::format(
                "runner.gen failed with non-revert status: {}",
                execute_result.error().kind));
        }

        const auto decoded_error = parse::decodeBytes<pt::PTExecuteError>(execute_result.error().result_bytes);
        if(!decoded_error)
        {
            return std::unexpected(std::format("failed to decode PT error: {}", decoded_error.error().kind));
        }

        return decoded_error->kind;
    }

    void expectIdentityData(const Particles & particles, std::uint32_t particles_count, std::uint32_t expected_value = 0)
    {
        ASSERT_EQ(particles.data_size(), static_cast<int>(particles_count));
        for(std::uint32_t i = 0; i < particles_count; ++i)
        {
            EXPECT_EQ(particles.data(static_cast<int>(i)), expected_value);
        }
    }
}

TEST_F(UnitTest, PT_Bindings_RegistryAcceptsDeepBindingToNestedSlot)
{
    asio::io_context io_context;
    registry::Registry registry(io_context);

    const std::string owner_hex = evmc::hex(makeAddressFromSuffix("bindings_owner"));

    ASSERT_TRUE(addIdentityTransformation(io_context, registry, owner_hex, 0x01));

    ASSERT_TRUE(addScalarConnector(io_context, registry, "TIME", owner_hex, 0x11, 1));
    ASSERT_TRUE(addScalarConnector(io_context, registry, "PITCH", owner_hex, 0x12, 2));

    ConnectorRecord rhythm = makeConnectorRecord("RHYTHM", owner_hex);
    addDimension(rhythm, "TIME");
    addDimension(rhythm, "");
    ASSERT_TRUE(addConnectorRecord(io_context, registry, 0x13, std::move(rhythm)));

    ConnectorRecord melody = makeConnectorRecord("MELODY", owner_hex);
    addDimension(melody, "PITCH");
    addDimension(melody, "RHYTHM");
    addDimension(melody, "");
    ASSERT_TRUE(addConnectorRecord(io_context, registry, 0x14, std::move(melody)));

    ConnectorRecord song = makeConnectorRecord("SONG", owner_hex);
    addDimension(song, "MELODY", {{"3", "TIME"}});
    EXPECT_TRUE(addConnectorRecord(io_context, registry, 0x15, std::move(song)));
}

TEST_F(UnitTest, PT_Bindings_RegistryAcceptsBindingToPropagatedStaticChildSlot)
{
    asio::io_context io_context;
    registry::Registry registry(io_context);

    const std::string owner_hex = evmc::hex(makeAddressFromSuffix("bindings_owner"));

    ASSERT_TRUE(addIdentityTransformation(io_context, registry, owner_hex, 0x01));

    ASSERT_TRUE(addScalarConnector(io_context, registry, "BASE2", owner_hex, 0x11, 2));
    ASSERT_TRUE(addScalarConnector(io_context, registry, "TARGET2", owner_hex, 0x12, 2));
    ASSERT_TRUE(addScalarConnector(io_context, registry, "TIME", owner_hex, 0x13, 1));

    ConnectorRecord test = makeConnectorRecord("TEST", owner_hex);
    addDimension(test, "BASE2", {{"0", "TARGET2"}});
    ASSERT_TRUE(addConnectorRecord(io_context, registry, 0x14, std::move(test)));

    ConnectorRecord test2 = makeConnectorRecord("TEST2", owner_hex);
    addDimension(test2, "TEST", {{"2", "TIME"}});
    EXPECT_TRUE(addConnectorRecord(io_context, registry, 0x15, std::move(test2)));
}

TEST_F(UnitTest, PT_Bindings_RegistryComputesNestedPropagatedSlotsAcrossMixedStaticBindings)
{
    asio::io_context io_context;
    registry::Registry registry(io_context);

    const std::string owner_hex = evmc::hex(makeAddressFromSuffix("bindings_owner"));

    ASSERT_TRUE(addIdentityTransformation(io_context, registry, owner_hex, 0x01));

    ASSERT_TRUE(addScalarConnector(io_context, registry, "BASE3", owner_hex, 0x11, 3));
    ASSERT_TRUE(addScalarConnector(io_context, registry, "TIME", owner_hex, 0x12, 1));
    ASSERT_TRUE(addScalarConnector(io_context, registry, "ALT", owner_hex, 0x13, 1));
    ASSERT_TRUE(addScalarConnector(io_context, registry, "PITCH2", owner_hex, 0x14, 2));

    ConnectorRecord child = makeConnectorRecord("CHILD", owner_hex);
    addDimension(child, "BASE3", {{"0", "TIME"}, {"2", "ALT"}});
    ASSERT_TRUE(addConnectorRecord(io_context, registry, 0x15, std::move(child)));

    ConnectorRecord mid = makeConnectorRecord("MID", owner_hex);
    addDimension(mid, "CHILD", {{"1", "PITCH2"}});
    ASSERT_TRUE(addConnectorRecord(io_context, registry, 0x16, std::move(mid)));

    ConnectorRecord root_valid = makeConnectorRecord("ROOT_VALID", owner_hex);
    addDimension(root_valid, "MID", {{"3", "TIME"}});
    EXPECT_TRUE(addConnectorRecord(io_context, registry, 0x17, std::move(root_valid)));

    ConnectorRecord root_invalid = makeConnectorRecord("ROOT_INVALID", owner_hex);
    addDimension(root_invalid, "MID", {{"4", "TIME"}});
    EXPECT_FALSE(addConnectorRecord(io_context, registry, 0x18, std::move(root_invalid)));
}

TEST_F(UnitTest, PT_Bindings_RegistryRejectsBindingsOnScalarDimension)
{
    asio::io_context io_context;
    registry::Registry registry(io_context);

    const std::string owner_hex = evmc::hex(makeAddressFromSuffix("bindings_owner"));

    ASSERT_TRUE(addIdentityTransformation(io_context, registry, owner_hex, 0x01));
    ASSERT_TRUE(addScalarConnector(io_context, registry, "TIME", owner_hex, 0x11, 1));

    ConnectorRecord invalid = makeConnectorRecord("SCALAR_WITH_BINDING", owner_hex);
    addDimension(invalid, "", {{"0", "TIME"}});

    EXPECT_FALSE(addConnectorRecord(io_context, registry, 0x12, std::move(invalid)));
}

TEST_F(UnitTest, PT_Bindings_RegistryRejectsNonNumericSlotId)
{
    asio::io_context io_context;
    registry::Registry registry(io_context);

    const std::string owner_hex = evmc::hex(makeAddressFromSuffix("bindings_owner"));

    ASSERT_TRUE(addIdentityTransformation(io_context, registry, owner_hex, 0x01));
    ASSERT_TRUE(addScalarConnector(io_context, registry, "CHILD", owner_hex, 0x11, 1));
    ASSERT_TRUE(addScalarConnector(io_context, registry, "TIME", owner_hex, 0x12, 1));

    ConnectorRecord invalid = makeConnectorRecord("PARENT", owner_hex);
    addDimension(invalid, "CHILD", {{"dim:0", "TIME"}});

    EXPECT_FALSE(addConnectorRecord(io_context, registry, 0x13, std::move(invalid)));
}

TEST_F(UnitTest, PT_Bindings_RegistryRejectsOutOfRangeSlotId)
{
    asio::io_context io_context;
    registry::Registry registry(io_context);

    const std::string owner_hex = evmc::hex(makeAddressFromSuffix("bindings_owner"));

    ASSERT_TRUE(addIdentityTransformation(io_context, registry, owner_hex, 0x01));
    ASSERT_TRUE(addScalarConnector(io_context, registry, "CHILD", owner_hex, 0x11, 1));
    ASSERT_TRUE(addScalarConnector(io_context, registry, "TIME", owner_hex, 0x12, 1));

    ConnectorRecord invalid = makeConnectorRecord("PARENT", owner_hex);
    addDimension(invalid, "CHILD", {{"1", "TIME"}});

    EXPECT_FALSE(addConnectorRecord(io_context, registry, 0x13, std::move(invalid)));
}

TEST_F(UnitTest, PT_Bindings_RegistryRejectsMissingBindingTarget)
{
    asio::io_context io_context;
    registry::Registry registry(io_context);

    const std::string owner_hex = evmc::hex(makeAddressFromSuffix("bindings_owner"));

    ASSERT_TRUE(addIdentityTransformation(io_context, registry, owner_hex, 0x01));
    ASSERT_TRUE(addScalarConnector(io_context, registry, "CHILD", owner_hex, 0x11, 1));

    ConnectorRecord invalid = makeConnectorRecord("PARENT", owner_hex);
    addDimension(invalid, "CHILD", {{"0", "MISSING_TARGET"}});

    EXPECT_FALSE(addConnectorRecord(io_context, registry, 0x12, std::move(invalid)));
}

TEST_F(UnitTest, PT_Bindings_RegistryRejectsDuplicateCanonicalSlotIds)
{
    asio::io_context io_context;
    registry::Registry registry(io_context);

    const std::string owner_hex = evmc::hex(makeAddressFromSuffix("bindings_owner"));

    ASSERT_TRUE(addIdentityTransformation(io_context, registry, owner_hex, 0x01));
    ASSERT_TRUE(addScalarConnector(io_context, registry, "CHILD", owner_hex, 0x11, 3));
    ASSERT_TRUE(addScalarConnector(io_context, registry, "TIME", owner_hex, 0x12, 1));
    ASSERT_TRUE(addScalarConnector(io_context, registry, "ALT", owner_hex, 0x13, 1));

    ConnectorRecord invalid = makeConnectorRecord("PARENT", owner_hex);
    addDimension(invalid, "CHILD", {{"1", "TIME"}, {"01", "ALT"}});

    EXPECT_FALSE(addConnectorRecord(io_context, registry, 0x14, std::move(invalid)));
}

TEST_F(UnitTest, PT_Bindings_RegistryRejectsCycleThroughBindingTarget)
{
    asio::io_context io_context;
    registry::Registry registry(io_context);

    const std::string owner_hex = evmc::hex(makeAddressFromSuffix("bindings_owner"));

    ASSERT_TRUE(addIdentityTransformation(io_context, registry, owner_hex, 0x01));
    ASSERT_TRUE(addScalarConnector(io_context, registry, "LEAF", owner_hex, 0x11, 1));

    ConnectorRecord a_v1 = makeConnectorRecord("A", owner_hex);
    addDimension(a_v1, "LEAF");
    ASSERT_TRUE(addConnectorRecord(io_context, registry, 0x12, std::move(a_v1)));

    ConnectorRecord a_v2 = makeConnectorRecord("A", owner_hex);
    addDimension(a_v2, "LEAF", {{"0", "A"}});

    EXPECT_FALSE(addConnectorRecord(io_context, registry, 0x13, std::move(a_v2)));
}

TEST_F(UnitTest, PT_Bindings_RegistryAllowsParentOverrideOfChildStaticBinding)
{
    asio::io_context io_context;
    registry::Registry registry(io_context);

    const std::string owner_hex = evmc::hex(makeAddressFromSuffix("bindings_owner"));

    ASSERT_TRUE(addIdentityTransformation(io_context, registry, owner_hex, 0x01));
    ASSERT_TRUE(addScalarConnector(io_context, registry, "LEAF", owner_hex, 0x11, 1));
    ASSERT_TRUE(addScalarConnector(io_context, registry, "TIME", owner_hex, 0x12, 1));

    ConnectorRecord child = makeConnectorRecord("CHILD", owner_hex);
    addDimension(child, "LEAF", {{"0", "TIME"}});
    ASSERT_TRUE(addConnectorRecord(io_context, registry, 0x13, std::move(child)));

    ConnectorRecord parent = makeConnectorRecord("PARENT", owner_hex);
    addDimension(parent, "CHILD", {{"0", "TIME"}});

    EXPECT_TRUE(addConnectorRecord(io_context, registry, 0x14, std::move(parent)));
}

TEST_F(UnitTest, PT_Bindings_RegistryAllowsBindingRemainingUnboundChildSlots)
{
    asio::io_context io_context;
    registry::Registry registry(io_context);

    const std::string owner_hex = evmc::hex(makeAddressFromSuffix("bindings_owner"));

    ASSERT_TRUE(addIdentityTransformation(io_context, registry, owner_hex, 0x01));
    ASSERT_TRUE(addScalarConnector(io_context, registry, "TIME", owner_hex, 0x11, 1));
    ASSERT_TRUE(addScalarConnector(io_context, registry, "BASE", owner_hex, 0x12, 2));

    ConnectorRecord child = makeConnectorRecord("CHILD", owner_hex);
    addDimension(child, "BASE", {{"0", "TIME"}});
    ASSERT_TRUE(addConnectorRecord(io_context, registry, 0x13, std::move(child)));

    ConnectorRecord parent = makeConnectorRecord("PARENT", owner_hex);
    addDimension(parent, "CHILD", {{"0", "TIME"}});

    EXPECT_TRUE(addConnectorRecord(io_context, registry, 0x14, std::move(parent)));
}

TEST_F(UnitTest, PT_Bindings_RegistryAllowsBindingToConnectorWithStaticBindings)
{
    asio::io_context io_context;
    registry::Registry registry(io_context);

    const std::string owner_hex = evmc::hex(makeAddressFromSuffix("bindings_owner"));

    ASSERT_TRUE(addIdentityTransformation(io_context, registry, owner_hex, 0x01));
    ASSERT_TRUE(addScalarConnector(io_context, registry, "BASE", owner_hex, 0x11, 2));
    ASSERT_TRUE(addScalarConnector(io_context, registry, "TIME", owner_hex, 0x12, 1));

    ConnectorRecord target = makeConnectorRecord("TARGET", owner_hex);
    addDimension(target, "BASE", {{"0", "TIME"}});
    ASSERT_TRUE(addConnectorRecord(io_context, registry, 0x13, std::move(target)));

    ConnectorRecord host = makeConnectorRecord("HOST", owner_hex);
    addDimension(host, "BASE");
    ASSERT_TRUE(addConnectorRecord(io_context, registry, 0x14, std::move(host)));

    ConnectorRecord root = makeConnectorRecord("ROOT", owner_hex);
    addDimension(root, "HOST", {{"1", "TARGET"}});
    EXPECT_TRUE(addConnectorRecord(io_context, registry, 0x15, std::move(root)));
}

TEST_F(UnitTest, PT_Bindings_RegistryAllowsCompositeWithStaticBindingsReusedAcrossDimensions)
{
    asio::io_context io_context;
    registry::Registry registry(io_context);

    const std::string owner_hex = evmc::hex(makeAddressFromSuffix("bindings_owner"));

    ASSERT_TRUE(addIdentityTransformation(io_context, registry, owner_hex, 0x01));
    ASSERT_TRUE(addScalarConnector(io_context, registry, "BASE", owner_hex, 0x11, 2));
    ASSERT_TRUE(addScalarConnector(io_context, registry, "TIME", owner_hex, 0x12, 1));
    ASSERT_TRUE(addScalarConnector(io_context, registry, "ALT", owner_hex, 0x13, 1));

    ConnectorRecord child = makeConnectorRecord("CHILD", owner_hex);
    addDimension(child, "BASE", {{"0", "TIME"}});
    ASSERT_TRUE(addConnectorRecord(io_context, registry, 0x14, std::move(child)));

    ConnectorRecord parent = makeConnectorRecord("PARENT", owner_hex);
    addDimension(parent, "CHILD", {{"0", "ALT"}});
    addDimension(parent, "CHILD");
    EXPECT_TRUE(addConnectorRecord(io_context, registry, 0x15, std::move(parent)));
}

TEST_F(UnitTest, PT_Bindings_RegistryAllowsBindingToPropagatedSlotWhenChildHasStaticBindings)
{
    asio::io_context io_context;
    registry::Registry registry(io_context);

    const std::string owner_hex = evmc::hex(makeAddressFromSuffix("bindings_owner"));

    ASSERT_TRUE(addIdentityTransformation(io_context, registry, owner_hex, 0x01));
    ASSERT_TRUE(addScalarConnector(io_context, registry, "BASE", owner_hex, 0x11, 2));
    ASSERT_TRUE(addScalarConnector(io_context, registry, "TIME", owner_hex, 0x12, 1));

    ConnectorRecord child = makeConnectorRecord("CHILD", owner_hex);
    addDimension(child, "BASE", {{"0", "TIME"}});
    ASSERT_TRUE(addConnectorRecord(io_context, registry, 0x13, std::move(child)));

    ConnectorRecord parent = makeConnectorRecord("PARENT", owner_hex);
    addDimension(parent, "CHILD", {{"1", "TIME"}});
    EXPECT_TRUE(addConnectorRecord(io_context, registry, 0x14, std::move(parent)));
}

TEST_F(UnitTest, PT_Bindings_RegistryAllowsBindingTargetWithoutOpenSlots)
{
    asio::io_context io_context;
    registry::Registry registry(io_context);

    const std::string owner_hex = evmc::hex(makeAddressFromSuffix("bindings_owner"));

    ASSERT_TRUE(addIdentityTransformation(io_context, registry, owner_hex, 0x01));
    ASSERT_TRUE(addScalarConnector(io_context, registry, "BASE", owner_hex, 0x11, 2));
    ASSERT_TRUE(addScalarConnector(io_context, registry, "TIME", owner_hex, 0x12, 1));
    ASSERT_TRUE(addScalarConnector(io_context, registry, "ALT", owner_hex, 0x13, 1));

    ConnectorRecord child = makeConnectorRecord("CHILD", owner_hex);
    addDimension(child, "BASE", {{"0", "TIME"}});
    ASSERT_TRUE(addConnectorRecord(io_context, registry, 0x14, std::move(child)));

    ConnectorRecord sealed = makeConnectorRecord("SEALED", owner_hex);
    addDimension(sealed, "CHILD", {{"0", "ALT"}});
    ASSERT_TRUE(addConnectorRecord(io_context, registry, 0x15, std::move(sealed)));

    ConnectorRecord host = makeConnectorRecord("HOST", owner_hex);
    addDimension(host, "BASE");
    ASSERT_TRUE(addConnectorRecord(io_context, registry, 0x16, std::move(host)));

    ConnectorRecord root = makeConnectorRecord("ROOT", owner_hex);
    addDimension(root, "HOST", {{"1", "SEALED"}});
    EXPECT_TRUE(addConnectorRecord(io_context, registry, 0x17, std::move(root)));
}

TEST_F(UnitTest, PT_Bindings_RegistryAllowsBindingAllRemainingSlotsAfterChildStaticBindings)
{
    asio::io_context io_context;
    registry::Registry registry(io_context);

    const std::string owner_hex = evmc::hex(makeAddressFromSuffix("bindings_owner"));

    ASSERT_TRUE(addIdentityTransformation(io_context, registry, owner_hex, 0x01));
    ASSERT_TRUE(addScalarConnector(io_context, registry, "BASE3", owner_hex, 0x11, 3));
    ASSERT_TRUE(addScalarConnector(io_context, registry, "TIME", owner_hex, 0x12, 1));
    ASSERT_TRUE(addScalarConnector(io_context, registry, "ALT", owner_hex, 0x13, 1));

    ConnectorRecord child = makeConnectorRecord("CHILD", owner_hex);
    addDimension(child, "BASE3", {{"1", "TIME"}});
    ASSERT_TRUE(addConnectorRecord(io_context, registry, 0x14, std::move(child)));

    ConnectorRecord parent = makeConnectorRecord("PARENT", owner_hex);
    addDimension(parent, "CHILD", {{"0", "ALT"}, {"1", "TIME"}});
    EXPECT_TRUE(addConnectorRecord(io_context, registry, 0x15, std::move(parent)));
}

TEST_F(UnitTest, PT_Bindings_RegistryAllowsBindingToMiddlePropagatedSlotAfterMultipleChildStaticBindings)
{
    asio::io_context io_context;
    registry::Registry registry(io_context);

    const std::string owner_hex = evmc::hex(makeAddressFromSuffix("bindings_owner"));

    ASSERT_TRUE(addIdentityTransformation(io_context, registry, owner_hex, 0x01));
    ASSERT_TRUE(addScalarConnector(io_context, registry, "BASE3", owner_hex, 0x11, 3));
    ASSERT_TRUE(addScalarConnector(io_context, registry, "TIME", owner_hex, 0x12, 1));
    ASSERT_TRUE(addScalarConnector(io_context, registry, "ALT", owner_hex, 0x13, 1));

    ConnectorRecord child = makeConnectorRecord("CHILD", owner_hex);
    addDimension(child, "BASE3", {{"0", "TIME"}, {"2", "ALT"}});
    ASSERT_TRUE(addConnectorRecord(io_context, registry, 0x14, std::move(child)));

    ConnectorRecord parent = makeConnectorRecord("PARENT", owner_hex);
    addDimension(parent, "CHILD", {{"1", "TIME"}});
    EXPECT_TRUE(addConnectorRecord(io_context, registry, 0x15, std::move(parent)));
}

TEST_F(UnitTest, PT_Bindings_RegistryAllowsSameCompactedSlotIdOnDifferentDimensions)
{
    asio::io_context io_context;
    registry::Registry registry(io_context);

    const std::string owner_hex = evmc::hex(makeAddressFromSuffix("bindings_owner"));

    ASSERT_TRUE(addIdentityTransformation(io_context, registry, owner_hex, 0x01));
    ASSERT_TRUE(addScalarConnector(io_context, registry, "BASE2", owner_hex, 0x11, 2));
    ASSERT_TRUE(addScalarConnector(io_context, registry, "TIME", owner_hex, 0x12, 1));
    ASSERT_TRUE(addScalarConnector(io_context, registry, "ALT", owner_hex, 0x13, 1));

    ConnectorRecord child = makeConnectorRecord("CHILD", owner_hex);
    addDimension(child, "BASE2", {{"0", "TIME"}});
    ASSERT_TRUE(addConnectorRecord(io_context, registry, 0x14, std::move(child)));

    ConnectorRecord parent = makeConnectorRecord("PARENT", owner_hex);
    addDimension(parent, "CHILD", {{"0", "ALT"}});
    addDimension(parent, "CHILD", {{"0", "TIME"}});
    EXPECT_TRUE(addConnectorRecord(io_context, registry, 0x15, std::move(parent)));
}

TEST_F(UnitTest, PT_Bindings_CodegenSortsBindingSlotIdsNumerically)
{
    ConnectorRecord record = makeConnectorRecord("CODEGEN_SORT", "0xabc123");
    addDimension(record, "ROOT_CHILD", {
        {"10", "ZETA"},
        {"2", "BETA"},
        {"1", "ALPHA"}
    });

    auto solidity_result = constructConnectorSolidityCode(record.connector());
    ASSERT_TRUE(solidity_result.has_value());
    const std::string & solidity = *solidity_result;

    const std::size_t slot_1 = solidity.find("bindingSlotIds[0] = uint32(1);");
    const std::size_t slot_2 = solidity.find("bindingSlotIds[1] = uint32(2);");
    const std::size_t slot_10 = solidity.find("bindingSlotIds[2] = uint32(10);");

    ASSERT_NE(slot_1, std::string::npos);
    ASSERT_NE(slot_2, std::string::npos);
    ASSERT_NE(slot_10, std::string::npos);
    EXPECT_LT(slot_1, slot_2);
    EXPECT_LT(slot_2, slot_10);
}

TEST_F(UnitTest, PT_Bindings_CodegenRejectsBindingsOnScalarDimension)
{
    ConnectorRecord record = makeConnectorRecord("CODEGEN_SCALAR_INVALID", "0xabc123");
    addDimension(record, "", {{"0", "TIME"}});

    auto solidity_result = constructConnectorSolidityCode(record.connector());
    EXPECT_FALSE(solidity_result.has_value());
}

TEST_F(UnitTest, PT_Bindings_CodegenRejectsEmptyBindingTarget)
{
    ConnectorRecord record = makeConnectorRecord("CODEGEN_EMPTY_TARGET", "0xabc123");
    addDimension(record, "ROOT_CHILD", {{"0", ""}});

    auto solidity_result = constructConnectorSolidityCode(record.connector());
    EXPECT_FALSE(solidity_result.has_value());
}

TEST_F(UnitTest, PT_Bindings_RunnerResolvesDeepGrandchildBinding)
{
    ASSERT_TRUE(std::filesystem::exists(solcPath())) << std::format("Missing Solidity compiler at '{}'", solcPath().string());
    ASSERT_TRUE(std::filesystem::exists(ptPath() / "contracts")) << std::format("Missing PT contracts at '{}'", (ptPath() / "contracts").string());

    const BindingStorageScope storage_scope(makeBindingStoragePath());
    const std::filesystem::path & storage_path = storage_scope.storage_path;
    ASSERT_TRUE(prepareBindingStorageDirectories(storage_path));

    asio::io_context io_context;
    evm::EVM evm_instance(io_context, EVMC_SHANGHAI, solcPath(), ptPath());
    io_context.run();

    registry::Registry registry(io_context);
    const chain::Address owner = makeAddressFromSuffix("binding_runtime_owner");
    const std::string owner_hex = evmc::hex(owner);

    runAwaitable(io_context, evm_instance.addAccount(owner, evm::DEFAULT_GAS_LIMIT));
    runAwaitable(io_context, evm_instance.setGas(owner, evm::DEFAULT_GAS_LIMIT));

    const auto tx_deploy = deployIdentityTransformation(io_context, evm_instance, registry, owner_hex, storage_path);
    ASSERT_TRUE(tx_deploy.has_value()) << tx_deploy.error();

    ConnectorRecord time = makeConnectorRecord("TIME", owner_hex);
    addDimension(time, "");
    ASSERT_TRUE(deployConnectorRecord(io_context, evm_instance, registry, std::move(time), storage_path).has_value());

    ConnectorRecord pitch = makeConnectorRecord("PITCH", owner_hex);
    addDimension(pitch, "");
    addDimension(pitch, "");
    ASSERT_TRUE(deployConnectorRecord(io_context, evm_instance, registry, std::move(pitch), storage_path).has_value());

    ConnectorRecord melody = makeConnectorRecord("MELODY", owner_hex);
    addDimension(melody, "PITCH");
    addDimension(melody, "");
    ASSERT_TRUE(deployConnectorRecord(io_context, evm_instance, registry, std::move(melody), storage_path).has_value());

    ConnectorRecord root = makeConnectorRecord("ROOT", owner_hex);
    addDimension(root, "MELODY", {{"1", "TIME"}});
    ASSERT_TRUE(deployConnectorRecord(io_context, evm_instance, registry, std::move(root), storage_path).has_value());

    const auto particles_result = runGeneration(io_context, evm_instance, owner, "ROOT", 4);
    ASSERT_TRUE(particles_result.has_value()) << particles_result.error();

    const auto & particles = particles_result.value();
    ASSERT_EQ(particles.size(), 3u);

    EXPECT_EQ(particles[0].path(), "/ROOT:0/MELODY:0/PITCH:0");
    EXPECT_EQ(particles[1].path(), "/ROOT:0/MELODY:0/PITCH:1/TIME:0");
    EXPECT_EQ(particles[2].path(), "/ROOT:0/MELODY:1");

    expectIdentityData(particles[0], 4);
    expectIdentityData(particles[1], 4);
    expectIdentityData(particles[2], 4);
}

TEST_F(UnitTest, PT_Bindings_RunnerResolvesBindingOnPropagatedStaticChildSlot)
{
    ASSERT_TRUE(std::filesystem::exists(solcPath())) << std::format("Missing Solidity compiler at '{}'", solcPath().string());
    ASSERT_TRUE(std::filesystem::exists(ptPath() / "contracts")) << std::format("Missing PT contracts at '{}'", (ptPath() / "contracts").string());

    const BindingStorageScope storage_scope(makeBindingStoragePath());
    const std::filesystem::path & storage_path = storage_scope.storage_path;
    ASSERT_TRUE(prepareBindingStorageDirectories(storage_path));

    asio::io_context io_context;
    evm::EVM evm_instance(io_context, EVMC_SHANGHAI, solcPath(), ptPath());
    io_context.run();

    registry::Registry registry(io_context);
    const chain::Address owner = makeAddressFromSuffix("binding_runtime_owner");
    const std::string owner_hex = evmc::hex(owner);

    runAwaitable(io_context, evm_instance.addAccount(owner, evm::DEFAULT_GAS_LIMIT));
    runAwaitable(io_context, evm_instance.setGas(owner, evm::DEFAULT_GAS_LIMIT));

    const auto tx_deploy = deployIdentityTransformation(io_context, evm_instance, registry, owner_hex, storage_path);
    ASSERT_TRUE(tx_deploy.has_value()) << tx_deploy.error();

    ConnectorRecord base2 = makeConnectorRecord("BASE2", owner_hex);
    addDimension(base2, "");
    addDimension(base2, "");
    ASSERT_TRUE(deployConnectorRecord(io_context, evm_instance, registry, std::move(base2), storage_path).has_value());

    ConnectorRecord target2 = makeConnectorRecord("TARGET2", owner_hex);
    addDimension(target2, "");
    addDimension(target2, "");
    ASSERT_TRUE(deployConnectorRecord(io_context, evm_instance, registry, std::move(target2), storage_path).has_value());

    ConnectorRecord pitch = makeConnectorRecord("PITCH", owner_hex);
    addDimension(pitch, "");
    addDimension(pitch, "");
    ASSERT_TRUE(deployConnectorRecord(io_context, evm_instance, registry, std::move(pitch), storage_path).has_value());

    ConnectorRecord test = makeConnectorRecord("TEST", owner_hex);
    addDimension(test, "BASE2", {{"0", "TARGET2"}});
    ASSERT_TRUE(deployConnectorRecord(io_context, evm_instance, registry, std::move(test), storage_path).has_value());

    ConnectorRecord test2 = makeConnectorRecord("TEST2", owner_hex);
    addDimension(test2, "TEST", {{"2", "PITCH"}});
    ASSERT_TRUE(deployConnectorRecord(io_context, evm_instance, registry, std::move(test2), storage_path).has_value());

    const auto particles_result = runGeneration(io_context, evm_instance, owner, "TEST2", 4);
    ASSERT_TRUE(particles_result.has_value()) << particles_result.error();

    const auto & particles = particles_result.value();
    ASSERT_EQ(particles.size(), 4u);

    EXPECT_EQ(particles[0].path(), "/TEST2:0/TEST:0/BASE2:0/TARGET2:0");
    EXPECT_EQ(particles[1].path(), "/TEST2:0/TEST:0/BASE2:0/TARGET2:1");
    EXPECT_EQ(particles[2].path(), "/TEST2:0/TEST:0/BASE2:1/PITCH:0");
    EXPECT_EQ(particles[3].path(), "/TEST2:0/TEST:0/BASE2:1/PITCH:1");

    expectIdentityData(particles[0], 4);
    expectIdentityData(particles[1], 4);
    expectIdentityData(particles[2], 4);
    expectIdentityData(particles[3], 4);
}

TEST_F(UnitTest, PT_Bindings_RunnerMapsStaticRangeStartToInternalSlot)
{
    ASSERT_TRUE(std::filesystem::exists(solcPath())) << std::format("Missing Solidity compiler at '{}'", solcPath().string());
    ASSERT_TRUE(std::filesystem::exists(ptPath() / "contracts")) << std::format("Missing PT contracts at '{}'", (ptPath() / "contracts").string());

    const BindingStorageScope storage_scope(makeBindingStoragePath());
    const std::filesystem::path & storage_path = storage_scope.storage_path;
    ASSERT_TRUE(prepareBindingStorageDirectories(storage_path));

    asio::io_context io_context;
    evm::EVM evm_instance(io_context, EVMC_SHANGHAI, solcPath(), ptPath());
    io_context.run();

    registry::Registry registry(io_context);
    const chain::Address owner = makeAddressFromSuffix("binding_runtime_owner");
    const std::string owner_hex = evmc::hex(owner);

    runAwaitable(io_context, evm_instance.addAccount(owner, evm::DEFAULT_GAS_LIMIT));
    runAwaitable(io_context, evm_instance.setGas(owner, evm::DEFAULT_GAS_LIMIT));

    const auto tx_deploy = deployIdentityTransformation(io_context, evm_instance, registry, owner_hex, storage_path);
    ASSERT_TRUE(tx_deploy.has_value()) << tx_deploy.error();

    ConnectorRecord base2 = makeConnectorRecord("BASE2", owner_hex);
    addDimension(base2, "");
    addDimension(base2, "");
    ASSERT_TRUE(deployConnectorRecord(io_context, evm_instance, registry, std::move(base2), storage_path).has_value());

    ConnectorRecord target2 = makeConnectorRecord("TARGET2", owner_hex);
    addDimension(target2, "");
    addDimension(target2, "");
    ASSERT_TRUE(deployConnectorRecord(io_context, evm_instance, registry, std::move(target2), storage_path).has_value());

    ConnectorRecord alt = makeConnectorRecord("ALT", owner_hex);
    addDimension(alt, "");
    ASSERT_TRUE(deployConnectorRecord(io_context, evm_instance, registry, std::move(alt), storage_path).has_value());

    ConnectorRecord child = makeConnectorRecord("CHILD", owner_hex);
    addDimension(child, "BASE2", {{"0", "TARGET2"}});
    ASSERT_TRUE(deployConnectorRecord(io_context, evm_instance, registry, std::move(child), storage_path).has_value());

    ConnectorRecord parent = makeConnectorRecord("PARENT", owner_hex);
    addDimension(parent, "CHILD", {{"0", "ALT"}});
    ASSERT_TRUE(deployConnectorRecord(io_context, evm_instance, registry, std::move(parent), storage_path).has_value());

    const auto particles_result = runGeneration(io_context, evm_instance, owner, "PARENT", 4);
    ASSERT_TRUE(particles_result.has_value()) << particles_result.error();

    const auto & particles = particles_result.value();
    ASSERT_EQ(particles.size(), 3u);

    EXPECT_EQ(particles[0].path(), "/PARENT:0/CHILD:0/BASE2:0/TARGET2:0/ALT:0");
    EXPECT_EQ(particles[1].path(), "/PARENT:0/CHILD:0/BASE2:0/TARGET2:1");
    EXPECT_EQ(particles[2].path(), "/PARENT:0/CHILD:0/BASE2:1");

    expectIdentityData(particles[0], 4);
    expectIdentityData(particles[1], 4);
    expectIdentityData(particles[2], 4);
}

TEST_F(UnitTest, PT_Bindings_RunnerMapsStaticRangeOffsetToInternalSlot)
{
    ASSERT_TRUE(std::filesystem::exists(solcPath())) << std::format("Missing Solidity compiler at '{}'", solcPath().string());
    ASSERT_TRUE(std::filesystem::exists(ptPath() / "contracts")) << std::format("Missing PT contracts at '{}'", (ptPath() / "contracts").string());

    const BindingStorageScope storage_scope(makeBindingStoragePath());
    const std::filesystem::path & storage_path = storage_scope.storage_path;
    ASSERT_TRUE(prepareBindingStorageDirectories(storage_path));

    asio::io_context io_context;
    evm::EVM evm_instance(io_context, EVMC_SHANGHAI, solcPath(), ptPath());
    io_context.run();

    registry::Registry registry(io_context);
    const chain::Address owner = makeAddressFromSuffix("binding_runtime_owner");
    const std::string owner_hex = evmc::hex(owner);

    runAwaitable(io_context, evm_instance.addAccount(owner, evm::DEFAULT_GAS_LIMIT));
    runAwaitable(io_context, evm_instance.setGas(owner, evm::DEFAULT_GAS_LIMIT));

    const auto tx_deploy = deployIdentityTransformation(io_context, evm_instance, registry, owner_hex, storage_path);
    ASSERT_TRUE(tx_deploy.has_value()) << tx_deploy.error();

    ConnectorRecord base2 = makeConnectorRecord("BASE2", owner_hex);
    addDimension(base2, "");
    addDimension(base2, "");
    ASSERT_TRUE(deployConnectorRecord(io_context, evm_instance, registry, std::move(base2), storage_path).has_value());

    ConnectorRecord target2 = makeConnectorRecord("TARGET2", owner_hex);
    addDimension(target2, "");
    addDimension(target2, "");
    ASSERT_TRUE(deployConnectorRecord(io_context, evm_instance, registry, std::move(target2), storage_path).has_value());

    ConnectorRecord alt = makeConnectorRecord("ALT", owner_hex);
    addDimension(alt, "");
    ASSERT_TRUE(deployConnectorRecord(io_context, evm_instance, registry, std::move(alt), storage_path).has_value());

    ConnectorRecord child = makeConnectorRecord("CHILD", owner_hex);
    addDimension(child, "BASE2", {{"0", "TARGET2"}});
    ASSERT_TRUE(deployConnectorRecord(io_context, evm_instance, registry, std::move(child), storage_path).has_value());

    ConnectorRecord parent = makeConnectorRecord("PARENT", owner_hex);
    addDimension(parent, "CHILD", {{"1", "ALT"}});
    ASSERT_TRUE(deployConnectorRecord(io_context, evm_instance, registry, std::move(parent), storage_path).has_value());

    const auto particles_result = runGeneration(io_context, evm_instance, owner, "PARENT", 4);
    ASSERT_TRUE(particles_result.has_value()) << particles_result.error();

    const auto & particles = particles_result.value();
    ASSERT_EQ(particles.size(), 3u);

    EXPECT_EQ(particles[0].path(), "/PARENT:0/CHILD:0/BASE2:0/TARGET2:0");
    EXPECT_EQ(particles[1].path(), "/PARENT:0/CHILD:0/BASE2:0/TARGET2:1/ALT:0");
    EXPECT_EQ(particles[2].path(), "/PARENT:0/CHILD:0/BASE2:1");

    expectIdentityData(particles[0], 4);
    expectIdentityData(particles[1], 4);
    expectIdentityData(particles[2], 4);
}

TEST_F(UnitTest, PT_Bindings_RunnerMapsMultipleBindingsIntoSameStaticCompositeOffsets)
{
    ASSERT_TRUE(std::filesystem::exists(solcPath())) << std::format("Missing Solidity compiler at '{}'", solcPath().string());
    ASSERT_TRUE(std::filesystem::exists(ptPath() / "contracts")) << std::format("Missing PT contracts at '{}'", (ptPath() / "contracts").string());

    const BindingStorageScope storage_scope(makeBindingStoragePath());
    const std::filesystem::path & storage_path = storage_scope.storage_path;
    ASSERT_TRUE(prepareBindingStorageDirectories(storage_path));

    asio::io_context io_context;
    evm::EVM evm_instance(io_context, EVMC_SHANGHAI, solcPath(), ptPath());
    io_context.run();

    registry::Registry registry(io_context);
    const chain::Address owner = makeAddressFromSuffix("binding_runtime_owner");
    const std::string owner_hex = evmc::hex(owner);

    runAwaitable(io_context, evm_instance.addAccount(owner, evm::DEFAULT_GAS_LIMIT));
    runAwaitable(io_context, evm_instance.setGas(owner, evm::DEFAULT_GAS_LIMIT));

    const auto tx_deploy = deployIdentityTransformation(io_context, evm_instance, registry, owner_hex, storage_path);
    ASSERT_TRUE(tx_deploy.has_value()) << tx_deploy.error();

    ConnectorRecord base2 = makeConnectorRecord("BASE2", owner_hex);
    addDimension(base2, "");
    addDimension(base2, "");
    ASSERT_TRUE(deployConnectorRecord(io_context, evm_instance, registry, std::move(base2), storage_path).has_value());

    ConnectorRecord target2 = makeConnectorRecord("TARGET2", owner_hex);
    addDimension(target2, "");
    addDimension(target2, "");
    ASSERT_TRUE(deployConnectorRecord(io_context, evm_instance, registry, std::move(target2), storage_path).has_value());

    ConnectorRecord alt = makeConnectorRecord("ALT", owner_hex);
    addDimension(alt, "");
    ASSERT_TRUE(deployConnectorRecord(io_context, evm_instance, registry, std::move(alt), storage_path).has_value());

    ConnectorRecord extra = makeConnectorRecord("EXTRA", owner_hex);
    addDimension(extra, "");
    ASSERT_TRUE(deployConnectorRecord(io_context, evm_instance, registry, std::move(extra), storage_path).has_value());

    ConnectorRecord child = makeConnectorRecord("CHILD", owner_hex);
    addDimension(child, "BASE2", {{"0", "TARGET2"}});
    ASSERT_TRUE(deployConnectorRecord(io_context, evm_instance, registry, std::move(child), storage_path).has_value());

    ConnectorRecord parent = makeConnectorRecord("PARENT", owner_hex);
    addDimension(parent, "CHILD", {{"0", "ALT"}, {"1", "EXTRA"}});
    ASSERT_TRUE(deployConnectorRecord(io_context, evm_instance, registry, std::move(parent), storage_path).has_value());

    const auto particles_result = runGeneration(io_context, evm_instance, owner, "PARENT", 4);
    ASSERT_TRUE(particles_result.has_value()) << particles_result.error();

    const auto & particles = particles_result.value();
    ASSERT_EQ(particles.size(), 3u);

    EXPECT_EQ(particles[0].path(), "/PARENT:0/CHILD:0/BASE2:0/TARGET2:0/ALT:0");
    EXPECT_EQ(particles[1].path(), "/PARENT:0/CHILD:0/BASE2:0/TARGET2:1/EXTRA:0");
    EXPECT_EQ(particles[2].path(), "/PARENT:0/CHILD:0/BASE2:1");

    expectIdentityData(particles[0], 4);
    expectIdentityData(particles[1], 4);
    expectIdentityData(particles[2], 4);
}

TEST_F(UnitTest, PT_Bindings_RunnerResolvesNestedPropagatedBindingAcrossTwoLevels)
{
    ASSERT_TRUE(std::filesystem::exists(solcPath())) << std::format("Missing Solidity compiler at '{}'", solcPath().string());
    ASSERT_TRUE(std::filesystem::exists(ptPath() / "contracts")) << std::format("Missing PT contracts at '{}'", (ptPath() / "contracts").string());

    const BindingStorageScope storage_scope(makeBindingStoragePath());
    const std::filesystem::path & storage_path = storage_scope.storage_path;
    ASSERT_TRUE(prepareBindingStorageDirectories(storage_path));

    asio::io_context io_context;
    evm::EVM evm_instance(io_context, EVMC_SHANGHAI, solcPath(), ptPath());
    io_context.run();

    registry::Registry registry(io_context);
    const chain::Address owner = makeAddressFromSuffix("binding_runtime_owner");
    const std::string owner_hex = evmc::hex(owner);

    runAwaitable(io_context, evm_instance.addAccount(owner, evm::DEFAULT_GAS_LIMIT));
    runAwaitable(io_context, evm_instance.setGas(owner, evm::DEFAULT_GAS_LIMIT));

    const auto tx_deploy = deployIdentityTransformation(io_context, evm_instance, registry, owner_hex, storage_path);
    ASSERT_TRUE(tx_deploy.has_value()) << tx_deploy.error();

    ConnectorRecord time = makeConnectorRecord("TIME", owner_hex);
    addDimension(time, "");
    ASSERT_TRUE(deployConnectorRecord(io_context, evm_instance, registry, std::move(time), storage_path).has_value());

    ConnectorRecord alt = makeConnectorRecord("ALT", owner_hex);
    addDimension(alt, "");
    ASSERT_TRUE(deployConnectorRecord(io_context, evm_instance, registry, std::move(alt), storage_path).has_value());

    ConnectorRecord pitch2 = makeConnectorRecord("PITCH2", owner_hex);
    addDimension(pitch2, "");
    addDimension(pitch2, "");
    ASSERT_TRUE(deployConnectorRecord(io_context, evm_instance, registry, std::move(pitch2), storage_path).has_value());

    ConnectorRecord extra2 = makeConnectorRecord("EXTRA2", owner_hex);
    addDimension(extra2, "");
    addDimension(extra2, "");
    ASSERT_TRUE(deployConnectorRecord(io_context, evm_instance, registry, std::move(extra2), storage_path).has_value());

    ConnectorRecord base3 = makeConnectorRecord("BASE3", owner_hex);
    addDimension(base3, "");
    addDimension(base3, "");
    addDimension(base3, "");
    ASSERT_TRUE(deployConnectorRecord(io_context, evm_instance, registry, std::move(base3), storage_path).has_value());

    ConnectorRecord child = makeConnectorRecord("CHILD", owner_hex);
    addDimension(child, "BASE3", {{"0", "TIME"}, {"2", "ALT"}});
    ASSERT_TRUE(deployConnectorRecord(io_context, evm_instance, registry, std::move(child), storage_path).has_value());

    ConnectorRecord mid = makeConnectorRecord("MID", owner_hex);
    addDimension(mid, "CHILD", {{"1", "PITCH2"}});
    ASSERT_TRUE(deployConnectorRecord(io_context, evm_instance, registry, std::move(mid), storage_path).has_value());

    ConnectorRecord root = makeConnectorRecord("ROOT", owner_hex);
    addDimension(root, "MID", {{"3", "EXTRA2"}});
    ASSERT_TRUE(deployConnectorRecord(io_context, evm_instance, registry, std::move(root), storage_path).has_value());

    const auto particles_result = runGeneration(io_context, evm_instance, owner, "ROOT", 4);
    ASSERT_TRUE(particles_result.has_value()) << particles_result.error();

    const auto & particles = particles_result.value();
    ASSERT_EQ(particles.size(), 5u);

    EXPECT_EQ(particles[0].path(), "/ROOT:0/MID:0/CHILD:0/BASE3:0/TIME:0");
    EXPECT_EQ(particles[1].path(), "/ROOT:0/MID:0/CHILD:0/BASE3:1/PITCH2:0");
    EXPECT_EQ(particles[2].path(), "/ROOT:0/MID:0/CHILD:0/BASE3:1/PITCH2:1");
    EXPECT_EQ(particles[3].path(), "/ROOT:0/MID:0/CHILD:0/BASE3:2/ALT:0/EXTRA2:0");
    EXPECT_EQ(particles[4].path(), "/ROOT:0/MID:0/CHILD:0/BASE3:2/ALT:0/EXTRA2:1");

    expectIdentityData(particles[0], 4);
    expectIdentityData(particles[1], 4);
    expectIdentityData(particles[2], 4);
    expectIdentityData(particles[3], 4);
    expectIdentityData(particles[4], 4);
}

TEST_F(UnitTest, PT_Bindings_RunnerMapsParentBindingIntoChildStaticSlotRange)
{
    ASSERT_TRUE(std::filesystem::exists(solcPath())) << std::format("Missing Solidity compiler at '{}'", solcPath().string());
    ASSERT_TRUE(std::filesystem::exists(ptPath() / "contracts")) << std::format("Missing PT contracts at '{}'", (ptPath() / "contracts").string());

    const BindingStorageScope storage_scope(makeBindingStoragePath());
    const std::filesystem::path & storage_path = storage_scope.storage_path;
    ASSERT_TRUE(prepareBindingStorageDirectories(storage_path));

    asio::io_context io_context;
    evm::EVM evm_instance(io_context, EVMC_SHANGHAI, solcPath(), ptPath());
    io_context.run();

    registry::Registry registry(io_context);
    const chain::Address owner = makeAddressFromSuffix("binding_runtime_owner");
    const std::string owner_hex = evmc::hex(owner);

    runAwaitable(io_context, evm_instance.addAccount(owner, evm::DEFAULT_GAS_LIMIT));
    runAwaitable(io_context, evm_instance.setGas(owner, evm::DEFAULT_GAS_LIMIT));

    const auto tx_deploy = deployIdentityTransformation(io_context, evm_instance, registry, owner_hex, storage_path);
    ASSERT_TRUE(tx_deploy.has_value()) << tx_deploy.error();

    ConnectorRecord time = makeConnectorRecord("TIME", owner_hex);
    addDimension(time, "");
    ASSERT_TRUE(deployConnectorRecord(io_context, evm_instance, registry, std::move(time), storage_path).has_value());

    ConnectorRecord alt = makeConnectorRecord("ALT", owner_hex);
    addDimension(alt, "");
    ASSERT_TRUE(deployConnectorRecord(io_context, evm_instance, registry, std::move(alt), storage_path).has_value());

    ConnectorRecord dual = makeConnectorRecord("DUAL", owner_hex);
    addDimension(dual, "");
    addDimension(dual, "");
    ASSERT_TRUE(deployConnectorRecord(io_context, evm_instance, registry, std::move(dual), storage_path).has_value());

    ConnectorRecord child = makeConnectorRecord("CHILD", owner_hex);
    addDimension(child, "DUAL", {{"0", "TIME"}});
    ASSERT_TRUE(deployConnectorRecord(io_context, evm_instance, registry, std::move(child), storage_path).has_value());

    ConnectorRecord parent = makeConnectorRecord("PARENT", owner_hex);
    addDimension(parent, "CHILD", {{"0", "ALT"}});
    ASSERT_TRUE(deployConnectorRecord(io_context, evm_instance, registry, std::move(parent), storage_path).has_value());

    const auto particles_result = runGeneration(io_context, evm_instance, owner, "PARENT", 4);
    ASSERT_TRUE(particles_result.has_value()) << particles_result.error();

    const auto & particles = particles_result.value();
    ASSERT_EQ(particles.size(), 2u);

    EXPECT_EQ(particles[0].path(), "/PARENT:0/CHILD:0/DUAL:0/TIME:0/ALT:0");
    EXPECT_EQ(particles[1].path(), "/PARENT:0/CHILD:0/DUAL:1");

    expectIdentityData(particles[0], 4);
    expectIdentityData(particles[1], 4);
}

TEST_F(UnitTest, PT_Bindings_RunnerAllowsBindingToConnectorWithStaticBindings)
{
    ASSERT_TRUE(std::filesystem::exists(solcPath())) << std::format("Missing Solidity compiler at '{}'", solcPath().string());
    ASSERT_TRUE(std::filesystem::exists(ptPath() / "contracts")) << std::format("Missing PT contracts at '{}'", (ptPath() / "contracts").string());

    const BindingStorageScope storage_scope(makeBindingStoragePath());
    const std::filesystem::path & storage_path = storage_scope.storage_path;
    ASSERT_TRUE(prepareBindingStorageDirectories(storage_path));

    asio::io_context io_context;
    evm::EVM evm_instance(io_context, EVMC_SHANGHAI, solcPath(), ptPath());
    io_context.run();

    registry::Registry registry(io_context);
    const chain::Address owner = makeAddressFromSuffix("binding_runtime_owner");
    const std::string owner_hex = evmc::hex(owner);

    runAwaitable(io_context, evm_instance.addAccount(owner, evm::DEFAULT_GAS_LIMIT));
    runAwaitable(io_context, evm_instance.setGas(owner, evm::DEFAULT_GAS_LIMIT));

    const auto tx_deploy = deployIdentityTransformation(io_context, evm_instance, registry, owner_hex, storage_path);
    ASSERT_TRUE(tx_deploy.has_value()) << tx_deploy.error();

    ConnectorRecord time = makeConnectorRecord("TIME", owner_hex);
    addDimension(time, "");
    ASSERT_TRUE(deployConnectorRecord(io_context, evm_instance, registry, std::move(time), storage_path).has_value());

    ConnectorRecord base = makeConnectorRecord("BASE", owner_hex);
    addDimension(base, "");
    addDimension(base, "");
    ASSERT_TRUE(deployConnectorRecord(io_context, evm_instance, registry, std::move(base), storage_path).has_value());

    ConnectorRecord target = makeConnectorRecord("TARGET", owner_hex);
    addDimension(target, "BASE", {{"0", "TIME"}});
    ASSERT_TRUE(deployConnectorRecord(io_context, evm_instance, registry, std::move(target), storage_path).has_value());

    ConnectorRecord host = makeConnectorRecord("HOST", owner_hex);
    addDimension(host, "BASE");
    ASSERT_TRUE(deployConnectorRecord(io_context, evm_instance, registry, std::move(host), storage_path).has_value());

    ConnectorRecord root = makeConnectorRecord("ROOT", owner_hex);
    addDimension(root, "HOST", {{"1", "TARGET"}});
    ASSERT_TRUE(deployConnectorRecord(io_context, evm_instance, registry, std::move(root), storage_path).has_value());

    const auto particles_result = runGeneration(io_context, evm_instance, owner, "ROOT", 4);
    ASSERT_TRUE(particles_result.has_value()) << particles_result.error();

    const auto & particles = particles_result.value();
    ASSERT_EQ(particles.size(), 3u);

    EXPECT_EQ(particles[0].path(), "/ROOT:0/HOST:0/BASE:0");
    EXPECT_EQ(particles[1].path(), "/ROOT:0/HOST:0/BASE:1/TARGET:0/BASE:0/TIME:0");
    EXPECT_EQ(particles[2].path(), "/ROOT:0/HOST:0/BASE:1/TARGET:0/BASE:1");

    expectIdentityData(particles[0], 4);
    expectIdentityData(particles[1], 4);
    expectIdentityData(particles[2], 4);
}

TEST_F(UnitTest, PT_Bindings_RunnerSupportsChainedBoundCompositesAsComposite)
{
    ASSERT_TRUE(std::filesystem::exists(solcPath())) << std::format("Missing Solidity compiler at '{}'", solcPath().string());
    ASSERT_TRUE(std::filesystem::exists(ptPath() / "contracts")) << std::format("Missing PT contracts at '{}'", (ptPath() / "contracts").string());

    const BindingStorageScope storage_scope(makeBindingStoragePath());
    const std::filesystem::path & storage_path = storage_scope.storage_path;
    ASSERT_TRUE(prepareBindingStorageDirectories(storage_path));

    asio::io_context io_context;
    evm::EVM evm_instance(io_context, EVMC_SHANGHAI, solcPath(), ptPath());
    io_context.run();

    registry::Registry registry(io_context);
    const chain::Address owner = makeAddressFromSuffix("binding_runtime_owner");
    const std::string owner_hex = evmc::hex(owner);

    runAwaitable(io_context, evm_instance.addAccount(owner, evm::DEFAULT_GAS_LIMIT));
    runAwaitable(io_context, evm_instance.setGas(owner, evm::DEFAULT_GAS_LIMIT));

    const auto tx_deploy = deployIdentityTransformation(io_context, evm_instance, registry, owner_hex, storage_path);
    ASSERT_TRUE(tx_deploy.has_value()) << tx_deploy.error();

    ConnectorRecord time = makeConnectorRecord("TIME", owner_hex);
    addDimension(time, "");
    ASSERT_TRUE(deployConnectorRecord(io_context, evm_instance, registry, std::move(time), storage_path).has_value());

    ConnectorRecord alt = makeConnectorRecord("ALT", owner_hex);
    addDimension(alt, "");
    ASSERT_TRUE(deployConnectorRecord(io_context, evm_instance, registry, std::move(alt), storage_path).has_value());

    ConnectorRecord base = makeConnectorRecord("BASE", owner_hex);
    addDimension(base, "");
    addDimension(base, "");
    ASSERT_TRUE(deployConnectorRecord(io_context, evm_instance, registry, std::move(base), storage_path).has_value());

    ConnectorRecord child = makeConnectorRecord("CHILD", owner_hex);
    addDimension(child, "BASE", {{"0", "TIME"}});
    ASSERT_TRUE(deployConnectorRecord(io_context, evm_instance, registry, std::move(child), storage_path).has_value());

    ConnectorRecord mid = makeConnectorRecord("MID", owner_hex);
    addDimension(mid, "CHILD", {{"0", "ALT"}});
    ASSERT_TRUE(deployConnectorRecord(io_context, evm_instance, registry, std::move(mid), storage_path).has_value());

    ConnectorRecord root = makeConnectorRecord("ROOT", owner_hex);
    addDimension(root, "MID");
    ASSERT_TRUE(deployConnectorRecord(io_context, evm_instance, registry, std::move(root), storage_path).has_value());

    const auto particles_result = runGeneration(io_context, evm_instance, owner, "ROOT", 4);
    ASSERT_TRUE(particles_result.has_value()) << particles_result.error();

    const auto & particles = particles_result.value();
    ASSERT_EQ(particles.size(), 2u);

    EXPECT_EQ(particles[0].path(), "/ROOT:0/MID:0/CHILD:0/BASE:0/TIME:0/ALT:0");
    EXPECT_EQ(particles[1].path(), "/ROOT:0/MID:0/CHILD:0/BASE:1");

    expectIdentityData(particles[0], 4);
    expectIdentityData(particles[1], 4);
}

TEST_F(UnitTest, PT_Bindings_RunnerMapsParentBindingsAcrossChildStaticSlotRange)
{
    ASSERT_TRUE(std::filesystem::exists(solcPath())) << std::format("Missing Solidity compiler at '{}'", solcPath().string());
    ASSERT_TRUE(std::filesystem::exists(ptPath() / "contracts")) << std::format("Missing PT contracts at '{}'", (ptPath() / "contracts").string());

    const BindingStorageScope storage_scope(makeBindingStoragePath());
    const std::filesystem::path & storage_path = storage_scope.storage_path;
    ASSERT_TRUE(prepareBindingStorageDirectories(storage_path));

    asio::io_context io_context;
    evm::EVM evm_instance(io_context, EVMC_SHANGHAI, solcPath(), ptPath());
    io_context.run();

    registry::Registry registry(io_context);
    const chain::Address owner = makeAddressFromSuffix("binding_runtime_owner");
    const std::string owner_hex = evmc::hex(owner);

    runAwaitable(io_context, evm_instance.addAccount(owner, evm::DEFAULT_GAS_LIMIT));
    runAwaitable(io_context, evm_instance.setGas(owner, evm::DEFAULT_GAS_LIMIT));

    const auto tx_deploy = deployIdentityTransformation(io_context, evm_instance, registry, owner_hex, storage_path);
    ASSERT_TRUE(tx_deploy.has_value()) << tx_deploy.error();

    ConnectorRecord time = makeConnectorRecord("TIME", owner_hex);
    addDimension(time, "");
    ASSERT_TRUE(deployConnectorRecord(io_context, evm_instance, registry, std::move(time), storage_path).has_value());

    ConnectorRecord alt = makeConnectorRecord("ALT", owner_hex);
    addDimension(alt, "");
    ASSERT_TRUE(deployConnectorRecord(io_context, evm_instance, registry, std::move(alt), storage_path).has_value());

    ConnectorRecord base3 = makeConnectorRecord("BASE3", owner_hex);
    addDimension(base3, "");
    addDimension(base3, "");
    addDimension(base3, "");
    ASSERT_TRUE(deployConnectorRecord(io_context, evm_instance, registry, std::move(base3), storage_path).has_value());

    ConnectorRecord child = makeConnectorRecord("CHILD", owner_hex);
    addDimension(child, "BASE3", {{"1", "TIME"}});
    ASSERT_TRUE(deployConnectorRecord(io_context, evm_instance, registry, std::move(child), storage_path).has_value());

    ConnectorRecord parent = makeConnectorRecord("PARENT", owner_hex);
    addDimension(parent, "CHILD", {{"0", "ALT"}, {"1", "TIME"}});
    ASSERT_TRUE(deployConnectorRecord(io_context, evm_instance, registry, std::move(parent), storage_path).has_value());

    const auto particles_result = runGeneration(io_context, evm_instance, owner, "PARENT", 4);
    ASSERT_TRUE(particles_result.has_value()) << particles_result.error();

    const auto & particles = particles_result.value();
    ASSERT_EQ(particles.size(), 3u);

    EXPECT_EQ(particles[0].path(), "/PARENT:0/CHILD:0/BASE3:0/ALT:0");
    EXPECT_EQ(particles[1].path(), "/PARENT:0/CHILD:0/BASE3:1/TIME:0/TIME:0");
    EXPECT_EQ(particles[2].path(), "/PARENT:0/CHILD:0/BASE3:2");

    expectIdentityData(particles[0], 4);
    expectIdentityData(particles[1], 4);
    expectIdentityData(particles[2], 4);
}

TEST_F(UnitTest, PT_Bindings_RunnerSupportsReusingBoundCompositeAcrossParentDimensionsWithDifferentBindings)
{
    ASSERT_TRUE(std::filesystem::exists(solcPath())) << std::format("Missing Solidity compiler at '{}'", solcPath().string());
    ASSERT_TRUE(std::filesystem::exists(ptPath() / "contracts")) << std::format("Missing PT contracts at '{}'", (ptPath() / "contracts").string());

    const BindingStorageScope storage_scope(makeBindingStoragePath());
    const std::filesystem::path & storage_path = storage_scope.storage_path;
    ASSERT_TRUE(prepareBindingStorageDirectories(storage_path));

    asio::io_context io_context;
    evm::EVM evm_instance(io_context, EVMC_SHANGHAI, solcPath(), ptPath());
    io_context.run();

    registry::Registry registry(io_context);
    const chain::Address owner = makeAddressFromSuffix("binding_runtime_owner");
    const std::string owner_hex = evmc::hex(owner);

    runAwaitable(io_context, evm_instance.addAccount(owner, evm::DEFAULT_GAS_LIMIT));
    runAwaitable(io_context, evm_instance.setGas(owner, evm::DEFAULT_GAS_LIMIT));

    const auto tx_deploy = deployIdentityTransformation(io_context, evm_instance, registry, owner_hex, storage_path);
    ASSERT_TRUE(tx_deploy.has_value()) << tx_deploy.error();

    ConnectorRecord time = makeConnectorRecord("TIME", owner_hex);
    addDimension(time, "");
    ASSERT_TRUE(deployConnectorRecord(io_context, evm_instance, registry, std::move(time), storage_path).has_value());

    ConnectorRecord alt = makeConnectorRecord("ALT", owner_hex);
    addDimension(alt, "");
    ASSERT_TRUE(deployConnectorRecord(io_context, evm_instance, registry, std::move(alt), storage_path).has_value());

    ConnectorRecord extra = makeConnectorRecord("EXTRA", owner_hex);
    addDimension(extra, "");
    ASSERT_TRUE(deployConnectorRecord(io_context, evm_instance, registry, std::move(extra), storage_path).has_value());

    ConnectorRecord base2 = makeConnectorRecord("BASE2", owner_hex);
    addDimension(base2, "");
    addDimension(base2, "");
    ASSERT_TRUE(deployConnectorRecord(io_context, evm_instance, registry, std::move(base2), storage_path).has_value());

    ConnectorRecord child = makeConnectorRecord("CHILD", owner_hex);
    addDimension(child, "BASE2", {{"0", "TIME"}});
    ASSERT_TRUE(deployConnectorRecord(io_context, evm_instance, registry, std::move(child), storage_path).has_value());

    ConnectorRecord root = makeConnectorRecord("ROOT", owner_hex);
    addDimension(root, "CHILD", {{"0", "ALT"}});
    addDimension(root, "CHILD", {{"0", "EXTRA"}});
    ASSERT_TRUE(deployConnectorRecord(io_context, evm_instance, registry, std::move(root), storage_path).has_value());

    const auto particles_result = runGeneration(io_context, evm_instance, owner, "ROOT", 4);
    ASSERT_TRUE(particles_result.has_value()) << particles_result.error();

    const auto & particles = particles_result.value();
    ASSERT_EQ(particles.size(), 4u);

    EXPECT_EQ(particles[0].path(), "/ROOT:0/CHILD:0/BASE2:0/TIME:0/ALT:0");
    EXPECT_EQ(particles[1].path(), "/ROOT:0/CHILD:0/BASE2:1");
    EXPECT_EQ(particles[2].path(), "/ROOT:1/CHILD:0/BASE2:0/TIME:0/EXTRA:0");
    EXPECT_EQ(particles[3].path(), "/ROOT:1/CHILD:0/BASE2:1");

    expectIdentityData(particles[0], 4);
    expectIdentityData(particles[1], 4);
    expectIdentityData(particles[2], 4);
    expectIdentityData(particles[3], 4);
}

TEST_F(UnitTest, PT_Bindings_RunnerRejectsUnsortedAndDuplicateDynamicRunningInstances)
{
    ASSERT_TRUE(std::filesystem::exists(solcPath())) << std::format("Missing Solidity compiler at '{}'", solcPath().string());
    ASSERT_TRUE(std::filesystem::exists(ptPath() / "contracts")) << std::format("Missing PT contracts at '{}'", (ptPath() / "contracts").string());

    const BindingStorageScope storage_scope(makeBindingStoragePath());
    const std::filesystem::path & storage_path = storage_scope.storage_path;
    ASSERT_TRUE(prepareBindingStorageDirectories(storage_path));

    asio::io_context io_context;
    evm::EVM evm_instance(io_context, EVMC_SHANGHAI, solcPath(), ptPath());
    io_context.run();

    registry::Registry registry(io_context);
    const chain::Address owner = makeAddressFromSuffix("binding_runtime_owner");
    const std::string owner_hex = evmc::hex(owner);

    runAwaitable(io_context, evm_instance.addAccount(owner, evm::DEFAULT_GAS_LIMIT));
    runAwaitable(io_context, evm_instance.setGas(owner, evm::DEFAULT_GAS_LIMIT));

    const auto tx_deploy = deployIdentityTransformation(io_context, evm_instance, registry, owner_hex, storage_path);
    ASSERT_TRUE(tx_deploy.has_value()) << tx_deploy.error();

    ConnectorRecord time = makeConnectorRecord("TIME", owner_hex);
    addDimension(time, "");
    ASSERT_TRUE(deployConnectorRecord(io_context, evm_instance, registry, std::move(time), storage_path).has_value());

    const auto unsorted_result = runGenerationExpectRevert(
        io_context,
        evm_instance,
        owner,
        "TIME",
        4,
        {{2u, 0u, 0u}, {1u, 0u, 0u}});
    ASSERT_TRUE(unsorted_result.has_value()) << unsorted_result.error();
    EXPECT_EQ(*unsorted_result, pt::PTExecuteError::Kind::RUNNING_INSTANCE_NOT_SORTED);

    const auto duplicate_result = runGenerationExpectRevert(
        io_context,
        evm_instance,
        owner,
        "TIME",
        4,
        {{1u, 0u, 0u}, {1u, 2u, 0u}});
    ASSERT_TRUE(duplicate_result.has_value()) << duplicate_result.error();
    EXPECT_EQ(*duplicate_result, pt::PTExecuteError::Kind::RUNNING_INSTANCE_DUPLICATE);
}

TEST_F(UnitTest, PT_Bindings_RunnerUsesBoundCompositeRootStaticRiForScalarSlotReplacement)
{
    ASSERT_TRUE(std::filesystem::exists(solcPath())) << std::format("Missing Solidity compiler at '{}'", solcPath().string());
    ASSERT_TRUE(std::filesystem::exists(ptPath() / "contracts")) << std::format("Missing PT contracts at '{}'", (ptPath() / "contracts").string());

    const BindingStorageScope storage_scope(makeBindingStoragePath());
    const std::filesystem::path & storage_path = storage_scope.storage_path;
    ASSERT_TRUE(prepareBindingStorageDirectories(storage_path));

    asio::io_context io_context;
    evm::EVM evm_instance(io_context, EVMC_SHANGHAI, solcPath(), ptPath());
    io_context.run();

    registry::Registry registry(io_context);
    const chain::Address owner = makeAddressFromSuffix("binding_runtime_owner");
    const std::string owner_hex = evmc::hex(owner);

    runAwaitable(io_context, evm_instance.addAccount(owner, evm::DEFAULT_GAS_LIMIT));
    runAwaitable(io_context, evm_instance.setGas(owner, evm::DEFAULT_GAS_LIMIT));

    const auto increment_deploy = deployTransformationRecord(
        io_context,
        evm_instance,
        registry,
        makeTransformationRecord(kIncrementTransformationName, "return x + 1;", owner_hex),
        storage_path);
    ASSERT_TRUE(increment_deploy.has_value()) << increment_deploy.error();

    ConnectorRecord bound_target = makeConnectorRecord("BOUND_TARGET", owner_hex);
    addDimensionWithTransformation(bound_target, "", kIncrementTransformationName);
    (*bound_target.mutable_connector()->mutable_static_ri())[0].set_start_point(5);
    (*bound_target.mutable_connector()->mutable_static_ri())[0].set_transformation_shift(0);
    ASSERT_TRUE(deployConnectorRecord(io_context, evm_instance, registry, std::move(bound_target), storage_path).has_value());

    ConnectorRecord base_slot = makeConnectorRecord("BASE_SLOT", owner_hex);
    addDimensionWithTransformation(base_slot, "", kIncrementTransformationName);
    ASSERT_TRUE(deployConnectorRecord(io_context, evm_instance, registry, std::move(base_slot), storage_path).has_value());

    ConnectorRecord host_bind = makeConnectorRecord("HOST_BIND", owner_hex);
    addDimensionWithTransformation(host_bind, "BASE_SLOT", kIncrementTransformationName, {{"0", "BOUND_TARGET"}});
    ASSERT_TRUE(deployConnectorRecord(io_context, evm_instance, registry, std::move(host_bind), storage_path).has_value());

    ConnectorRecord root_bind = makeConnectorRecord("ROOT_BIND", owner_hex);
    addDimensionWithTransformation(root_bind, "HOST_BIND", kIncrementTransformationName);
    ASSERT_TRUE(deployConnectorRecord(io_context, evm_instance, registry, std::move(root_bind), storage_path).has_value());

    const auto particles_result = runGeneration(io_context, evm_instance, owner, "ROOT_BIND", 4);
    ASSERT_TRUE(particles_result.has_value()) << particles_result.error();

    const auto & particles = particles_result.value();
    ASSERT_EQ(particles.size(), 1u);

    EXPECT_EQ(particles[0].path(), "/ROOT_BIND:0/HOST_BIND:0/BASE_SLOT:0/BOUND_TARGET:0");
    ASSERT_EQ(particles[0].data_size(), 4);
    EXPECT_EQ(particles[0].data(0), 5);
    EXPECT_EQ(particles[0].data(1), 6);
    EXPECT_EQ(particles[0].data(2), 7);
    EXPECT_EQ(particles[0].data(3), 8);
}

TEST_F(UnitTest, PT_Bindings_RunnerAppliesHostStaticRiToBindingExpandedDescendantPositions)
{
    ASSERT_TRUE(std::filesystem::exists(solcPath())) << std::format("Missing Solidity compiler at '{}'", solcPath().string());
    ASSERT_TRUE(std::filesystem::exists(ptPath() / "contracts")) << std::format("Missing PT contracts at '{}'", (ptPath() / "contracts").string());

    const BindingStorageScope storage_scope(makeBindingStoragePath());
    const std::filesystem::path & storage_path = storage_scope.storage_path;
    ASSERT_TRUE(prepareBindingStorageDirectories(storage_path));

    asio::io_context io_context;
    evm::EVM evm_instance(io_context, EVMC_SHANGHAI, solcPath(), ptPath());
    io_context.run();

    registry::Registry registry(io_context);
    const chain::Address owner = makeAddressFromSuffix("binding_runtime_owner");
    const std::string owner_hex = evmc::hex(owner);

    runAwaitable(io_context, evm_instance.addAccount(owner, evm::DEFAULT_GAS_LIMIT));
    runAwaitable(io_context, evm_instance.setGas(owner, evm::DEFAULT_GAS_LIMIT));

    const auto increment_deploy = deployTransformationRecord(
        io_context,
        evm_instance,
        registry,
        makeTransformationRecord(kIncrementTransformationName, "return x + 1;", owner_hex),
        storage_path);
    ASSERT_TRUE(increment_deploy.has_value()) << increment_deploy.error();

    ConnectorRecord target = makeConnectorRecord("TARGET", owner_hex);
    addDimensionWithTransformation(target, "", kIncrementTransformationName);
    ASSERT_TRUE(deployConnectorRecord(io_context, evm_instance, registry, std::move(target), storage_path).has_value());

    ConnectorRecord leaf = makeConnectorRecord("LEAF", owner_hex);
    addDimensionWithTransformation(leaf, "", kIncrementTransformationName);
    ASSERT_TRUE(deployConnectorRecord(io_context, evm_instance, registry, std::move(leaf), storage_path).has_value());

    ConnectorRecord child = makeConnectorRecord("CHILD", owner_hex);
    addDimensionWithTransformation(child, "LEAF", kIncrementTransformationName, {{"0", "TARGET"}});
    ASSERT_TRUE(deployConnectorRecord(io_context, evm_instance, registry, std::move(child), storage_path).has_value());

    ConnectorRecord host = makeConnectorRecord("HOST", owner_hex);
    addDimensionWithTransformation(host, "CHILD", kIncrementTransformationName);
    (*host.mutable_connector()->mutable_static_ri())[4].set_start_point(7);
    (*host.mutable_connector()->mutable_static_ri())[4].set_transformation_shift(0);
    ASSERT_TRUE(deployConnectorRecord(io_context, evm_instance, registry, std::move(host), storage_path).has_value());

    const auto particles_result = runGeneration(io_context, evm_instance, owner, "HOST", 4);
    ASSERT_TRUE(particles_result.has_value()) << particles_result.error();

    const auto & particles = particles_result.value();
    ASSERT_EQ(particles.size(), 1u);
    EXPECT_EQ(particles[0].path(), "/HOST:0/CHILD:0/LEAF:0/TARGET:0");
    ASSERT_EQ(particles[0].data_size(), 4);
    EXPECT_EQ(particles[0].data(0), 7);
    EXPECT_EQ(particles[0].data(1), 8);
    EXPECT_EQ(particles[0].data(2), 9);
    EXPECT_EQ(particles[0].data(3), 10);
}

TEST_F(UnitTest, PT_Bindings_RunnerSupportsNestedBindingDescendantStaticRiOverride)
{
    ASSERT_TRUE(std::filesystem::exists(solcPath())) << std::format("Missing Solidity compiler at '{}'", solcPath().string());
    ASSERT_TRUE(std::filesystem::exists(ptPath() / "contracts")) << std::format("Missing PT contracts at '{}'", (ptPath() / "contracts").string());

    const BindingStorageScope storage_scope(makeBindingStoragePath());
    const std::filesystem::path & storage_path = storage_scope.storage_path;
    ASSERT_TRUE(prepareBindingStorageDirectories(storage_path));

    asio::io_context io_context;
    evm::EVM evm_instance(io_context, EVMC_SHANGHAI, solcPath(), ptPath());
    io_context.run();

    registry::Registry registry(io_context);
    const chain::Address owner = makeAddressFromSuffix("binding_runtime_owner");
    const std::string owner_hex = evmc::hex(owner);

    runAwaitable(io_context, evm_instance.addAccount(owner, evm::DEFAULT_GAS_LIMIT));
    runAwaitable(io_context, evm_instance.setGas(owner, evm::DEFAULT_GAS_LIMIT));

    const auto add1_deploy = deployTransformationRecord(
        io_context,
        evm_instance,
        registry,
        makeTransformationRecord("Add1Tx", "return x + 1;", owner_hex),
        storage_path);
    ASSERT_TRUE(add1_deploy.has_value()) << add1_deploy.error();

    const auto add2_deploy = deployTransformationRecord(
        io_context,
        evm_instance,
        registry,
        makeTransformationRecord("Add2Tx", "return x + 2;", owner_hex),
        storage_path);
    ASSERT_TRUE(add2_deploy.has_value()) << add2_deploy.error();

    const auto add3_deploy = deployTransformationRecord(
        io_context,
        evm_instance,
        registry,
        makeTransformationRecord("Add3Tx", "return x + 3;", owner_hex),
        storage_path);
    ASSERT_TRUE(add3_deploy.has_value()) << add3_deploy.error();

    const auto add10_deploy = deployTransformationRecord(
        io_context,
        evm_instance,
        registry,
        makeTransformationRecord("Add10Tx", "return x + 10;", owner_hex),
        storage_path);
    ASSERT_TRUE(add10_deploy.has_value()) << add10_deploy.error();

    ConnectorRecord pitch = makeConnectorRecord("PITCH", owner_hex);
    addDimensionWithTransformation(pitch, "", "Add1Tx");
    ASSERT_TRUE(deployConnectorRecord(io_context, evm_instance, registry, std::move(pitch), storage_path).has_value());

    ConnectorRecord time = makeConnectorRecord("TIME", owner_hex);
    addDimensionWithTransformation(time, "", "Add1Tx");
    ASSERT_TRUE(deployConnectorRecord(io_context, evm_instance, registry, std::move(time), storage_path).has_value());

    ConnectorRecord t0 = makeConnectorRecord("T0", owner_hex);
    addDimensionWithTransformation(t0, "PITCH", "Add2Tx");
    addDimensionWithTransformation(t0, "TIME", "Add10Tx");
    (*t0.mutable_connector()->mutable_static_ri())[1].set_start_point(10);
    (*t0.mutable_connector()->mutable_static_ri())[1].set_transformation_shift(0);
    ASSERT_TRUE(deployConnectorRecord(io_context, evm_instance, registry, std::move(t0), storage_path).has_value());

    ConnectorRecord t1 = makeConnectorRecord("T1", owner_hex);
    addDimensionWithTransformation(t1, "PITCH", "Add3Tx");
    addDimensionWithTransformation(t1, "T0", "Add2Tx", {{"0", "TIME"}});
    // DFS position 6 in T1 points to /T1:1/T0:0/PITCH:0/TIME:0
    (*t1.mutable_connector()->mutable_static_ri())[6].set_start_point(5);
    (*t1.mutable_connector()->mutable_static_ri())[6].set_transformation_shift(0);
    ASSERT_TRUE(deployConnectorRecord(io_context, evm_instance, registry, std::move(t1), storage_path).has_value());

    const auto particles_result = runGeneration(io_context, evm_instance, owner, "T1", 4);
    ASSERT_TRUE(particles_result.has_value()) << particles_result.error();

    const auto & particles = particles_result.value();
    ASSERT_EQ(particles.size(), 3u);

    EXPECT_EQ(particles[0].path(), "/T1:0/PITCH:0");
    ASSERT_EQ(particles[0].data_size(), 4);
    EXPECT_EQ(particles[0].data(0), 0);
    EXPECT_EQ(particles[0].data(1), 3);
    EXPECT_EQ(particles[0].data(2), 6);
    EXPECT_EQ(particles[0].data(3), 9);

    EXPECT_EQ(particles[1].path(), "/T1:1/T0:0/PITCH:0/TIME:0");
    ASSERT_EQ(particles[1].data_size(), 4);
    EXPECT_EQ(particles[1].data(0), 15);
    EXPECT_EQ(particles[1].data(1), 19);
    EXPECT_EQ(particles[1].data(2), 23);
    EXPECT_EQ(particles[1].data(3), 27);

    EXPECT_EQ(particles[2].path(), "/T1:1/T0:1/TIME:0");
    ASSERT_EQ(particles[2].data_size(), 4);
    EXPECT_EQ(particles[2].data(0), 0);
    EXPECT_EQ(particles[2].data(1), 20);
    EXPECT_EQ(particles[2].data(2), 40);
    EXPECT_EQ(particles[2].data(3), 60);
}

TEST_F(UnitTest, PT_Bindings_RunnerAllowsBindingToSealedCompositeTarget)
{
    ASSERT_TRUE(std::filesystem::exists(solcPath())) << std::format("Missing Solidity compiler at '{}'", solcPath().string());
    ASSERT_TRUE(std::filesystem::exists(ptPath() / "contracts")) << std::format("Missing PT contracts at '{}'", (ptPath() / "contracts").string());

    const BindingStorageScope storage_scope(makeBindingStoragePath());
    const std::filesystem::path & storage_path = storage_scope.storage_path;
    ASSERT_TRUE(prepareBindingStorageDirectories(storage_path));

    asio::io_context io_context;
    evm::EVM evm_instance(io_context, EVMC_SHANGHAI, solcPath(), ptPath());
    io_context.run();

    registry::Registry registry(io_context);
    const chain::Address owner = makeAddressFromSuffix("binding_runtime_owner");
    const std::string owner_hex = evmc::hex(owner);

    runAwaitable(io_context, evm_instance.addAccount(owner, evm::DEFAULT_GAS_LIMIT));
    runAwaitable(io_context, evm_instance.setGas(owner, evm::DEFAULT_GAS_LIMIT));

    const auto tx_deploy = deployIdentityTransformation(io_context, evm_instance, registry, owner_hex, storage_path);
    ASSERT_TRUE(tx_deploy.has_value()) << tx_deploy.error();

    ConnectorRecord time = makeConnectorRecord("TIME", owner_hex);
    addDimension(time, "");
    ASSERT_TRUE(deployConnectorRecord(io_context, evm_instance, registry, std::move(time), storage_path).has_value());

    ConnectorRecord alt = makeConnectorRecord("ALT", owner_hex);
    addDimension(alt, "");
    ASSERT_TRUE(deployConnectorRecord(io_context, evm_instance, registry, std::move(alt), storage_path).has_value());

    ConnectorRecord dual = makeConnectorRecord("DUAL", owner_hex);
    addDimension(dual, "");
    addDimension(dual, "");
    ASSERT_TRUE(deployConnectorRecord(io_context, evm_instance, registry, std::move(dual), storage_path).has_value());

    ConnectorRecord sealed = makeConnectorRecord("SEALED", owner_hex);
    addDimension(sealed, "DUAL", {{"0", "TIME"}, {"1", "ALT"}});
    ASSERT_TRUE(deployConnectorRecord(io_context, evm_instance, registry, std::move(sealed), storage_path).has_value());

    ConnectorRecord host = makeConnectorRecord("HOST", owner_hex);
    addDimension(host, "DUAL");
    ASSERT_TRUE(deployConnectorRecord(io_context, evm_instance, registry, std::move(host), storage_path).has_value());

    ConnectorRecord root = makeConnectorRecord("ROOT", owner_hex);
    addDimension(root, "HOST", {{"1", "SEALED"}});
    ASSERT_TRUE(deployConnectorRecord(io_context, evm_instance, registry, std::move(root), storage_path).has_value());

    const auto particles_result = runGeneration(io_context, evm_instance, owner, "ROOT", 4);
    ASSERT_TRUE(particles_result.has_value()) << particles_result.error();

    const auto & particles = particles_result.value();
    ASSERT_EQ(particles.size(), 3u);

    EXPECT_EQ(particles[0].path(), "/ROOT:0/HOST:0/DUAL:0");
    EXPECT_EQ(particles[1].path(), "/ROOT:0/HOST:0/DUAL:1/SEALED:0/DUAL:0/TIME:0");
    EXPECT_EQ(particles[2].path(), "/ROOT:0/HOST:0/DUAL:1/SEALED:0/DUAL:1/ALT:0");

    expectIdentityData(particles[0], 4);
    expectIdentityData(particles[1], 4);
    expectIdentityData(particles[2], 4);
}
