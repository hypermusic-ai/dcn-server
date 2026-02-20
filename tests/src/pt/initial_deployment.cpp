#include "unit-tests.hpp"

#include <cstring>
#include <filesystem>
#include <format>
#include <future>
#include <mutex>
#include <ranges>

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
    struct PTBootstrapSnapshot
    {
        bool success = false;
        std::string error_message;

        evm::Address registry_address{};
        evm::Address runner_address{};
        evm::Address registry_owner{};
        evm::Address runner_owner{};
    };

    bool isZeroAddress(const evm::Address & address)
    {
        return std::ranges::all_of(address.bytes, [](std::uint8_t b) { return b == 0; });
    }

    evm::Address expectedGenesisAddress()
    {
        evm::Address genesis_address{};
        std::memcpy(genesis_address.bytes + (20 - 7), "genesis", 7);
        return genesis_address;
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

    PTBootstrapSnapshot deployInitialPT()
    {
        PTBootstrapSnapshot snapshot;

        const auto solc_path = solcPath();
        const auto pt_path = ptPath();

        if(!std::filesystem::exists(solc_path))
        {
            snapshot.error_message = std::format("Missing Solidity compiler at '{}'", solc_path.string());
            return snapshot;
        }

        if(!std::filesystem::exists(pt_path / "contracts"))
        {
            snapshot.error_message = std::format("Missing PT contracts directory at '{}'", (pt_path / "contracts").string());
            return snapshot;
        }

        try
        {
            asio::io_context io_context;
            evm::EVM evm(io_context, EVMC_SHANGHAI, solc_path, pt_path);

            io_context.run();

            snapshot.registry_address = evm.getRegistryAddress();
            snapshot.runner_address = evm.getRunnerAddress();

            auto registry_owner_future = asio::co_spawn(io_context, evm::fetchOwner(evm, snapshot.registry_address), asio::use_future);
            auto runner_owner_future = asio::co_spawn(io_context, evm::fetchOwner(evm, snapshot.runner_address), asio::use_future);

            io_context.restart();
            io_context.run();

            auto registry_owner_result = registry_owner_future.get();
            auto runner_owner_result = runner_owner_future.get();

            if(!registry_owner_result)
            {
                snapshot.error_message = std::format("fetchOwner(registry) failed: {}", registry_owner_result.error().kind);
                return snapshot;
            }

            if(!runner_owner_result)
            {
                snapshot.error_message = std::format("fetchOwner(runner) failed: {}", runner_owner_result.error().kind);
                return snapshot;
            }

            snapshot.registry_owner = evm::decodeReturnedValue<evm::Address>(registry_owner_result.value());
            snapshot.runner_owner = evm::decodeReturnedValue<evm::Address>(runner_owner_result.value());
            snapshot.success = true;
        }
        catch(const std::exception & e)
        {
            snapshot.error_message = std::format("PT bootstrap failed: {}", e.what());
        }
        catch(...)
        {
            snapshot.error_message = "PT bootstrap failed with unknown exception";
        }

        return snapshot;
    }

    const PTBootstrapSnapshot & getPTBootstrapSnapshot()
    {
        static PTBootstrapSnapshot snapshot;
        static std::once_flag once_flag;
        std::call_once(once_flag, []() { snapshot = deployInitialPT(); });
        return snapshot;
    }
}

TEST_F(UnitTest, PT_InitialDeployment_DeploysRegistryAndRunner)
{
    const auto & snapshot = getPTBootstrapSnapshot();
    ASSERT_TRUE(snapshot.success) << snapshot.error_message;

    EXPECT_FALSE(isZeroAddress(snapshot.registry_address));
    EXPECT_FALSE(isZeroAddress(snapshot.runner_address));
    EXPECT_NE(snapshot.registry_address, snapshot.runner_address);
}

TEST_F(UnitTest, PT_InitialDeployment_SetsGenesisOwnerForCoreContracts)
{
    const auto & snapshot = getPTBootstrapSnapshot();
    ASSERT_TRUE(snapshot.success) << snapshot.error_message;

    const auto expected_owner = expectedGenesisAddress();
    EXPECT_EQ(snapshot.registry_owner, expected_owner);
    EXPECT_EQ(snapshot.runner_owner, expected_owner);
}
