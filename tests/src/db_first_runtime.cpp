#include "unit-tests.hpp"
#include "test_connector_helpers.hpp"

#include <array>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include <nlohmann/json.hpp>
#include <sqlite3.h>

#include "sqlite_registry_store.hpp"

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
    using dcn::tests::helpers::makeAddressFromByte;
    using dcn::tests::helpers::runAwaitable;
    using json = nlohmann::json;

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

    std::filesystem::path makeTestPath(const std::string & test_name)
    {
        const auto suffix = std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
        return buildPath() / "tests" / "db_first_runtime" / (test_name + "_" + suffix);
    }

    struct PathScope
    {
        explicit PathScope(std::filesystem::path path_)
            : path(std::move(path_))
        {
        }

        ~PathScope()
        {
            std::error_code ec;
            std::filesystem::remove_all(path, ec);
        }

        std::filesystem::path path;
    };

    bool prepareStorageLayout(const std::filesystem::path & storage_path)
    {
        static const std::array<std::string, 3> entity_dirs{
            "connectors",
            "transformations",
            "conditions"
        };

        std::error_code ec;
        for(const auto & entity_dir : entity_dirs)
        {
            ec.clear();
            std::filesystem::create_directories(storage_path / entity_dir / "build", ec);
            if(ec)
            {
                return false;
            }
        }

        return true;
    }

    std::optional<std::string> readTextFile(const std::filesystem::path & file_path)
    {
        std::ifstream input(file_path, std::ios::in);
        if(!input.is_open())
        {
            return std::nullopt;
        }

        return std::string((std::istreambuf_iterator<char>(input)), std::istreambuf_iterator<char>());
    }

    template<class RecordT>
    bool writeJsonRecord(const std::filesystem::path & file_path, const RecordT & record)
    {
        const auto json_result = parse::parseToJson(record, parse::use_protobuf);
        if(!json_result)
        {
            return false;
        }

        std::ofstream output(file_path, std::ios::out | std::ios::trunc);
        if(!output.is_open())
        {
            return false;
        }

        output << *json_result;
        return output.good();
    }

    bool executeSqlScript(const std::filesystem::path & db_path, const std::string & script)
    {
        sqlite3 * db = nullptr;
        const int open_rc = sqlite3_open_v2(db_path.string().c_str(), &db, SQLITE_OPEN_READWRITE, nullptr);
        if(open_rc != SQLITE_OK)
        {
            if(db != nullptr)
            {
                sqlite3_close(db);
            }
            return false;
        }

        char * error_message = nullptr;
        const int exec_rc = sqlite3_exec(db, script.c_str(), nullptr, nullptr, &error_message);
        if(error_message != nullptr)
        {
            sqlite3_free(error_message);
        }

        sqlite3_close(db);
        return exec_rc == SQLITE_OK;
    }

    server::RouteArg makeStringRouteArg(const std::string & value)
    {
        return server::RouteArg(
            server::RouteArgDef(server::RouteArgType::string, server::RouteArgRequirement::required),
            value);
    }

    TransformationRecord makeTransformationRecord(
        const std::string & name,
        const std::string & owner_hex,
        const std::string & sol_src = "return x;")
    {
        TransformationRecord record;
        record.set_owner(owner_hex);
        record.mutable_transformation()->set_name(name);
        record.mutable_transformation()->set_sol_src(sol_src);
        return record;
    }

    ConditionRecord makeConditionRecord(
        const std::string & name,
        const std::string & owner_hex,
        const std::string & sol_src = "return true;")
    {
        ConditionRecord record;
        record.set_owner(owner_hex);
        record.mutable_condition()->set_name(name);
        record.mutable_condition()->set_sol_src(sol_src);
        return record;
    }

    ConnectorRecord makeConnectorRecord(const std::string & name, const std::string & owner_hex)
    {
        ConnectorRecord record;
        record.set_owner(owner_hex);
        record.mutable_connector()->set_name(name);
        return record;
    }

    void addConnectorDimension(
        ConnectorRecord & record,
        const std::string & composite_name,
        const std::string & transformation_name,
        std::initializer_list<std::pair<std::string, std::string>> bindings = {})
    {
        auto * dimension = record.mutable_connector()->add_dimensions();
        dimension->set_composite(composite_name);

        if(!transformation_name.empty())
        {
            auto * transformation = dimension->add_transformations();
            transformation->set_name(transformation_name);
        }

        for(const auto & [slot, target] : bindings)
        {
            (*dimension->mutable_bindings())[slot] = target;
        }
    }

    std::vector<std::uint8_t> makeSingleStringCallInput(
        const std::string_view signature,
        const std::string & value)
    {
        std::vector<std::uint8_t> input_data;
        const auto selector = chain::constructSelector(std::string(signature));
        input_data.insert(input_data.end(), selector.begin(), selector.end());

        std::vector<std::uint8_t> offset(32, 0);
        offset[31] = 0x20;
        input_data.insert(input_data.end(), offset.begin(), offset.end());

        const auto encoded_value = evm::encodeAsArg(value);
        input_data.insert(input_data.end(), encoded_value.begin(), encoded_value.end());

        return input_data;
    }

    std::optional<bool> containsRegistryEntry(
        asio::io_context & io_context,
        evm::EVM & evm_instance,
        const std::string_view signature,
        const std::string & name)
    {
        auto input_data = makeSingleStringCallInput(signature, name);
        runAwaitable(io_context, evm_instance.setGas(evm_instance.getRegistryAddress(), evm::DEFAULT_GAS_LIMIT));

        const auto exec_result = runAwaitable(
            io_context,
            evm_instance.execute(
                evm_instance.getRegistryAddress(),
                evm_instance.getRegistryAddress(),
                std::move(input_data),
                evm::DEFAULT_GAS_LIMIT,
                0));

        if(!exec_result)
        {
            return std::nullopt;
        }

        const auto & output = exec_result.value();
        if(output.size() < 32)
        {
            return std::nullopt;
        }

        return output.back() != 0;
    }

    std::optional<bool> containsConnector(
        asio::io_context & io_context,
        evm::EVM & evm_instance,
        const std::string & name)
    {
        return containsRegistryEntry(io_context, evm_instance, "containsConnector(string)", name);
    }

    std::optional<bool> containsTransformation(
        asio::io_context & io_context,
        evm::EVM & evm_instance,
        const std::string & name)
    {
        return containsRegistryEntry(io_context, evm_instance, "containsTransformation(string)", name);
    }

    std::optional<bool> containsCondition(
        asio::io_context & io_context,
        evm::EVM & evm_instance,
        const std::string & name)
    {
        return containsRegistryEntry(io_context, evm_instance, "containsCondition(string)", name);
    }

    http::Request makeExecuteRequest(const std::string & access_token, const std::string & connector_name)
    {
        json request_json = {
            {"connector_name", connector_name},
            {"particles_count", 8},
            {"running_instances", json::array({
                json{
                    {"start_point", 0},
                    {"transformation_shift", 0}
                }
            })}
        };

        http::Request request;
        request.setMethod(http::Method::POST)
               .setPath(http::URL("/execute"))
               .setVersion("HTTP/1.1")
               .setBody(request_json.dump())
               .setHeader(http::Header::Authorization, std::format("Bearer {}", access_token));

        return request;
    }
}

TEST_F(UnitTest, Registry_SQLite_PersistsRecordsAcrossReopen)
{
    const auto storage_path = makeTestPath("registry_persistence");
    PathScope storage_scope(storage_path);
    ASSERT_TRUE(std::filesystem::create_directories(storage_path));
    const auto db_path = storage_path / "registry.sqlite";

    const std::string owner_hex = evmc::hex(makeAddressFromByte(0xA1));
    const auto transformation_record = makeTransformationRecord("PersistTx", owner_hex);
    const auto condition_record = makeConditionRecord("PersistCond", owner_hex);

    ConnectorRecord connector_record = makeConnectorRecord("PersistConnector", owner_hex);
    addConnectorDimension(connector_record, "", "PersistTx");
    connector_record.mutable_connector()->set_condition_name("PersistCond");

    {
        asio::io_context io_context;
        registry::Registry registry(io_context, db_path.string());

        ASSERT_TRUE(runAwaitable(io_context, registry.addTransformation(makeAddressFromByte(0x11), transformation_record)));
        ASSERT_TRUE(runAwaitable(io_context, registry.addCondition(makeAddressFromByte(0x12), condition_record)));
        ASSERT_TRUE(runAwaitable(io_context, registry.addConnector(makeAddressFromByte(0x13), connector_record)));
    }

    {
        asio::io_context io_context;
        registry::Registry registry(io_context, db_path.string());

        const auto transformation_handle = runAwaitable(io_context, registry.getTransformationRecordHandle("PersistTx"));
        ASSERT_TRUE(transformation_handle.has_value());
        ASSERT_TRUE(*transformation_handle);
        EXPECT_EQ((*transformation_handle)->owner(), owner_hex);
        EXPECT_EQ((*transformation_handle)->transformation().name(), "PersistTx");

        const auto condition_handle = runAwaitable(io_context, registry.getConditionRecordHandle("PersistCond"));
        ASSERT_TRUE(condition_handle.has_value());
        ASSERT_TRUE(*condition_handle);
        EXPECT_EQ((*condition_handle)->owner(), owner_hex);
        EXPECT_EQ((*condition_handle)->condition().name(), "PersistCond");

        const auto connector_handle = runAwaitable(io_context, registry.getConnectorRecordHandle("PersistConnector"));
        ASSERT_TRUE(connector_handle.has_value());
        ASSERT_TRUE(*connector_handle);
        EXPECT_EQ((*connector_handle)->owner(), owner_hex);
        EXPECT_EQ((*connector_handle)->connector().name(), "PersistConnector");

        const auto format_hash = runAwaitable(io_context, registry.getFormatHash("PersistConnector"));
        EXPECT_TRUE(format_hash.has_value());
    }
}

TEST_F(UnitTest, SQLiteRegistryStore_CheckpointWal_TruncatePersistsAndShrinksWal)
{
    const auto storage_path = makeTestPath("registry_checkpoint_truncate");
    PathScope storage_scope(storage_path);
    ASSERT_TRUE(std::filesystem::create_directories(storage_path));
    const auto db_path = storage_path / "registry.sqlite";

    const std::string owner_hex = evmc::hex(makeAddressFromByte(0xC1));
    {
        registry::SQLiteRegistryStore store(db_path.string());
        for(int i = 0; i < 96; ++i)
        {
            const auto name = std::format("WalTx{}", i);
            auto record = makeTransformationRecord(name, owner_hex, "return x + 1;");
            ASSERT_TRUE(store.addTransformation(makeAddressFromByte(static_cast<std::uint8_t>(0x10 + (i % 20))), record));
        }

        const auto wal_path = std::filesystem::path(db_path.string() + "-wal");
        ASSERT_TRUE(std::filesystem::exists(wal_path));

        std::error_code before_ec;
        const auto wal_size_before = std::filesystem::file_size(wal_path, before_ec);
        ASSERT_FALSE(before_ec);
        EXPECT_GT(wal_size_before, 0u);

        EXPECT_TRUE(store.checkpointWal(registry::WalCheckpointMode::TRUNCATE));

        std::error_code after_ec;
        const auto wal_size_after = std::filesystem::file_size(wal_path, after_ec);
        ASSERT_FALSE(after_ec);
        EXPECT_LE(wal_size_after, wal_size_before);
        EXPECT_LE(wal_size_after, static_cast<std::uintmax_t>(4096));
    }

    {
        asio::io_context io_context;
        registry::Registry registry(io_context, db_path.string());
        const auto handle = runAwaitable(io_context, registry.getTransformationRecordHandle("WalTx95"));
        ASSERT_TRUE(handle.has_value());
        ASSERT_TRUE(*handle);
        EXPECT_EQ((*handle)->owner(), owner_hex);
    }
}

TEST_F(UnitTest, Registry_CheckpointWal_PassiveReturnsTrue)
{
    const auto storage_path = makeTestPath("registry_checkpoint_passive");
    PathScope storage_scope(storage_path);
    ASSERT_TRUE(std::filesystem::create_directories(storage_path));
    const auto db_path = storage_path / "registry.sqlite";

    asio::io_context io_context;
    registry::Registry registry(io_context, db_path.string());

    const std::string owner_hex = evmc::hex(makeAddressFromByte(0xC2));
    const auto record = makeTransformationRecord("PassiveCheckpointTx", owner_hex, "return x;");
    ASSERT_TRUE(runAwaitable(io_context, registry.addTransformation(makeAddressFromByte(0x55), record)));

    EXPECT_TRUE(runAwaitable(io_context, registry.checkpointWal(registry::WalCheckpointMode::PASSIVE)));
}

TEST_F(UnitTest, API_ReadEndpoints_ReturnFromDbWithoutEvmDeployment)
{
    asio::io_context io_context;
    registry::Registry registry(io_context);

    const std::string owner_hex = evmc::hex(makeAddressFromByte(0xB1));

    const auto transformation_record = makeTransformationRecord("DbOnlyTx", owner_hex, "return x + 1;");
    const auto condition_record = makeConditionRecord("DbOnlyCond", owner_hex, "return args[0] > 0;");

    ConnectorRecord connector_record = makeConnectorRecord("DbOnlyConnector", owner_hex);
    addConnectorDimension(connector_record, "", "DbOnlyTx");
    connector_record.mutable_connector()->set_condition_name("DbOnlyCond");

    ASSERT_TRUE(runAwaitable(io_context, registry.addTransformation(makeAddressFromByte(0x21), transformation_record)));
    ASSERT_TRUE(runAwaitable(io_context, registry.addCondition(makeAddressFromByte(0x22), condition_record)));
    ASSERT_TRUE(runAwaitable(io_context, registry.addConnector(makeAddressFromByte(0x23), connector_record)));

    {
        http::Request request;
        request.setMethod(http::Method::GET)
               .setPath(http::URL("/transformation/DbOnlyTx"))
               .setVersion("HTTP/1.1");

        std::vector<server::RouteArg> route_args{makeStringRouteArg("DbOnlyTx")};
        const auto response = runAwaitable(
            io_context,
            GET_transformation(request, std::move(route_args), server::QueryArgsList{}, registry));

        ASSERT_EQ(response.getCode(), http::Code::OK);
        const auto body = json::parse(response.getBody(), nullptr, false);
        ASSERT_FALSE(body.is_discarded());
        EXPECT_EQ(body["name"], "DbOnlyTx");
        EXPECT_EQ(body["owner"], owner_hex);
        EXPECT_EQ(body["address"], "0x0");
    }

    {
        http::Request request;
        request.setMethod(http::Method::GET)
               .setPath(http::URL("/condition/DbOnlyCond"))
               .setVersion("HTTP/1.1");

        std::vector<server::RouteArg> route_args{makeStringRouteArg("DbOnlyCond")};
        const auto response = runAwaitable(
            io_context,
            GET_condition(request, std::move(route_args), server::QueryArgsList{}, registry));

        ASSERT_EQ(response.getCode(), http::Code::OK);
        const auto body = json::parse(response.getBody(), nullptr, false);
        ASSERT_FALSE(body.is_discarded());
        EXPECT_EQ(body["name"], "DbOnlyCond");
        EXPECT_EQ(body["owner"], owner_hex);
        EXPECT_EQ(body["address"], "0x0");
    }

    {
        http::Request request;
        request.setMethod(http::Method::GET)
               .setPath(http::URL("/connector/DbOnlyConnector"))
               .setVersion("HTTP/1.1");

        std::vector<server::RouteArg> route_args{makeStringRouteArg("DbOnlyConnector")};
        const auto response = runAwaitable(
            io_context,
            GET_connector(request, std::move(route_args), server::QueryArgsList{}, registry));

        ASSERT_EQ(response.getCode(), http::Code::OK);
        const auto body = json::parse(response.getBody(), nullptr, false);
        ASSERT_FALSE(body.is_discarded());
        EXPECT_EQ(body["name"], "DbOnlyConnector");
        EXPECT_EQ(body["owner"], owner_hex);
        EXPECT_EQ(body["address"], "0x0");
        ASSERT_TRUE(body.contains("format_hash"));
        EXPECT_TRUE(body["format_hash"].is_string());
        EXPECT_FALSE(body["format_hash"].get<std::string>().empty());
    }
}

TEST_F(UnitTest, API_Execute_MissingConnectorReturnsNotFound)
{
    ASSERT_TRUE(std::filesystem::exists(solcPath())) << std::format("Missing Solidity compiler at '{}'", solcPath().string());
    ASSERT_TRUE(std::filesystem::exists(ptPath() / "contracts")) << std::format("Missing PT contracts at '{}'", (ptPath() / "contracts").string());

    const auto storage_path = makeTestPath("execute_missing_connector");
    PathScope storage_scope(storage_path);
    ASSERT_TRUE(prepareStorageLayout(storage_path));
    const auto db_path = storage_path / "registry.sqlite";

    asio::io_context io_context;
    registry::Registry registry(io_context, db_path.string());
    auth::AuthManager auth_manager(io_context);
    evm::EVM evm_instance(io_context, EVMC_SHANGHAI, solcPath(), ptPath());
    io_context.run();

    const chain::Address caller = makeAddressFromByte(0x44);
    (void)runAwaitable(io_context, evm_instance.addAccount(caller, evm::DEFAULT_GAS_LIMIT));
    (void)runAwaitable(io_context, evm_instance.setGas(caller, evm::DEFAULT_GAS_LIMIT));

    const std::string access_token = runAwaitable(io_context, auth_manager.generateAccessToken(caller));

    config::Config cfg;
    cfg.storage_path = storage_path;

    auto request = makeExecuteRequest(access_token, "MissingConnector");
    const auto response = runAwaitable(
        io_context,
        POST_execute(
            request,
            std::vector<server::RouteArg>{},
            server::QueryArgsList{},
            auth_manager,
            registry,
            evm_instance,
            cfg));

    ASSERT_EQ(response.getCode(), http::Code::NotFound);
    const auto body = json::parse(response.getBody(), nullptr, false);
    ASSERT_FALSE(body.is_discarded());
    EXPECT_EQ(body["message"], "Connector not found");
}

TEST_F(UnitTest, API_PostConnector_DeploysMissingTransformationDependencyFromDb)
{
    ASSERT_TRUE(std::filesystem::exists(solcPath())) << std::format("Missing Solidity compiler at '{}'", solcPath().string());
    ASSERT_TRUE(std::filesystem::exists(ptPath() / "contracts")) << std::format("Missing PT contracts at '{}'", (ptPath() / "contracts").string());

    const auto storage_path = makeTestPath("post_connector_dependency_deploy");
    PathScope storage_scope(storage_path);
    ASSERT_TRUE(prepareStorageLayout(storage_path));
    const auto db_path = storage_path / "registry.sqlite";

    asio::io_context io_context;
    registry::Registry registry(io_context, db_path.string());
    auth::AuthManager auth_manager(io_context);
    evm::EVM evm_instance(io_context, EVMC_SHANGHAI, solcPath(), ptPath());
    io_context.run();

    const chain::Address caller = makeAddressFromByte(0x49);
    const std::string caller_hex = evmc::hex(caller);
    (void)runAwaitable(io_context, evm_instance.addAccount(caller, evm::DEFAULT_GAS_LIMIT));
    (void)runAwaitable(io_context, evm_instance.setGas(caller, evm::DEFAULT_GAS_LIMIT));
    const std::string access_token = runAwaitable(io_context, auth_manager.generateAccessToken(caller));

    const auto transformation_record = makeTransformationRecord("mul", caller_hex, "return x * 2;");
    ASSERT_TRUE(runAwaitable(io_context, registry.addTransformation(makeAddressFromByte(0x4A), transformation_record)));

    const auto contains_tx_before = containsTransformation(io_context, evm_instance, "mul");
    ASSERT_TRUE(contains_tx_before.has_value());
    EXPECT_FALSE(*contains_tx_before);

    Connector connector;
    connector.set_name("connector_with_mul");
    auto * dimension = connector.add_dimensions();
    auto * transformation = dimension->add_transformations();
    transformation->set_name("mul");

    const auto connector_json_res = parse::parseToJson(connector, parse::use_protobuf);
    ASSERT_TRUE(connector_json_res.has_value());

    config::Config cfg;
    cfg.storage_path = storage_path;

    http::Request request;
    request.setMethod(http::Method::POST)
           .setPath(http::URL("/connector"))
           .setVersion("HTTP/1.1")
           .setBody(*connector_json_res)
           .setHeader(http::Header::Authorization, std::format("Bearer {}", access_token));

    const auto response = runAwaitable(
        io_context,
        POST_connector(
            request,
            std::vector<server::RouteArg>{},
            server::QueryArgsList{},
            auth_manager,
            registry,
            evm_instance,
            cfg));

    ASSERT_EQ(response.getCode(), http::Code::Created);
    const auto body = json::parse(response.getBody(), nullptr, false);
    ASSERT_FALSE(body.is_discarded());
    EXPECT_EQ(body["name"], "connector_with_mul");
    EXPECT_EQ(body["owner"], caller_hex);

    const auto contains_tx_after = containsTransformation(io_context, evm_instance, "mul");
    ASSERT_TRUE(contains_tx_after.has_value());
    EXPECT_TRUE(*contains_tx_after);

    const auto contains_connector_after = containsConnector(io_context, evm_instance, "connector_with_mul");
    ASSERT_TRUE(contains_connector_after.has_value());
    EXPECT_TRUE(*contains_connector_after);

    const auto connector_handle = runAwaitable(io_context, registry.getConnectorRecordHandle("connector_with_mul"));
    ASSERT_TRUE(connector_handle.has_value());
    ASSERT_TRUE(*connector_handle);
    EXPECT_EQ((*connector_handle)->owner(), caller_hex);
}

TEST_F(UnitTest, API_PostConnector_MissingTransformationDependencyReturnsBadRequest)
{
    ASSERT_TRUE(std::filesystem::exists(solcPath())) << std::format("Missing Solidity compiler at '{}'", solcPath().string());
    ASSERT_TRUE(std::filesystem::exists(ptPath() / "contracts")) << std::format("Missing PT contracts at '{}'", (ptPath() / "contracts").string());

    const auto storage_path = makeTestPath("post_connector_missing_dependency");
    PathScope storage_scope(storage_path);
    ASSERT_TRUE(prepareStorageLayout(storage_path));
    const auto db_path = storage_path / "registry.sqlite";

    asio::io_context io_context;
    registry::Registry registry(io_context, db_path.string());
    auth::AuthManager auth_manager(io_context);
    evm::EVM evm_instance(io_context, EVMC_SHANGHAI, solcPath(), ptPath());
    io_context.run();

    const chain::Address caller = makeAddressFromByte(0x4B);
    const std::string caller_hex = evmc::hex(caller);
    (void)runAwaitable(io_context, evm_instance.addAccount(caller, evm::DEFAULT_GAS_LIMIT));
    (void)runAwaitable(io_context, evm_instance.setGas(caller, evm::DEFAULT_GAS_LIMIT));
    const std::string access_token = runAwaitable(io_context, auth_manager.generateAccessToken(caller));

    Connector connector;
    connector.set_name("connector_missing_tx");
    auto * dimension = connector.add_dimensions();
    auto * transformation = dimension->add_transformations();
    transformation->set_name("mul_missing");

    const auto connector_json_res = parse::parseToJson(connector, parse::use_protobuf);
    ASSERT_TRUE(connector_json_res.has_value());

    config::Config cfg;
    cfg.storage_path = storage_path;

    http::Request request;
    request.setMethod(http::Method::POST)
           .setPath(http::URL("/connector"))
           .setVersion("HTTP/1.1")
           .setBody(*connector_json_res)
           .setHeader(http::Header::Authorization, std::format("Bearer {}", access_token));

    const auto response = runAwaitable(
        io_context,
        POST_connector(
            request,
            std::vector<server::RouteArg>{},
            server::QueryArgsList{},
            auth_manager,
            registry,
            evm_instance,
            cfg));

    ASSERT_EQ(response.getCode(), http::Code::BadRequest);
    const auto body = json::parse(response.getBody(), nullptr, false);
    ASSERT_FALSE(body.is_discarded());
    EXPECT_EQ(body["message"], "Failed to deploy connector. Error: Transformation missing");

    const auto contains_connector = containsConnector(io_context, evm_instance, "connector_missing_tx");
    ASSERT_TRUE(contains_connector.has_value());
    EXPECT_FALSE(*contains_connector);

    const auto connector_handle = runAwaitable(io_context, registry.getConnectorRecordHandle("connector_missing_tx"));
    EXPECT_FALSE(connector_handle.has_value());
}

TEST_F(UnitTest, API_Execute_LazilyDeploysConnectorClosureFromDb)
{
    ASSERT_TRUE(std::filesystem::exists(solcPath())) << std::format("Missing Solidity compiler at '{}'", solcPath().string());
    ASSERT_TRUE(std::filesystem::exists(ptPath() / "contracts")) << std::format("Missing PT contracts at '{}'", (ptPath() / "contracts").string());

    const auto storage_path = makeTestPath("execute_lazy_deploy");
    PathScope storage_scope(storage_path);
    ASSERT_TRUE(prepareStorageLayout(storage_path));
    const auto db_path = storage_path / "registry.sqlite";

    asio::io_context io_context;
    registry::Registry registry(io_context, db_path.string());
    auth::AuthManager auth_manager(io_context);
    evm::EVM evm_instance(io_context, EVMC_SHANGHAI, solcPath(), ptPath());
    io_context.run();

    const chain::Address caller = makeAddressFromByte(0x54);
    const std::string owner_hex = evmc::hex(caller);
    (void)runAwaitable(io_context, evm_instance.addAccount(caller, evm::DEFAULT_GAS_LIMIT));
    (void)runAwaitable(io_context, evm_instance.setGas(caller, evm::DEFAULT_GAS_LIMIT));
    const std::string access_token = runAwaitable(io_context, auth_manager.generateAccessToken(caller));

    const auto transformation_record = makeTransformationRecord("LazyTx", owner_hex);
    ConnectorRecord connector_record = makeConnectorRecord("LazyConnector", owner_hex);
    addConnectorDimension(connector_record, "", "LazyTx");

    ASSERT_TRUE(runAwaitable(io_context, registry.addTransformation(makeAddressFromByte(0x31), transformation_record)));
    ASSERT_TRUE(runAwaitable(io_context, registry.addConnector(makeAddressFromByte(0x32), connector_record)));

    const auto contains_tx_before = containsTransformation(io_context, evm_instance, "LazyTx");
    ASSERT_TRUE(contains_tx_before.has_value());
    EXPECT_FALSE(*contains_tx_before);

    const auto contains_connector_before = containsConnector(io_context, evm_instance, "LazyConnector");
    ASSERT_TRUE(contains_connector_before.has_value());
    EXPECT_FALSE(*contains_connector_before);

    config::Config cfg;
    cfg.storage_path = storage_path;

    auto request = makeExecuteRequest(access_token, "LazyConnector");
    const auto response = runAwaitable(
        io_context,
        POST_execute(
            request,
            std::vector<server::RouteArg>{},
            server::QueryArgsList{},
            auth_manager,
            registry,
            evm_instance,
            cfg));

    ASSERT_EQ(response.getCode(), http::Code::OK);
    const auto body = json::parse(response.getBody(), nullptr, false);
    ASSERT_FALSE(body.is_discarded());
    EXPECT_TRUE(body.is_array());

    const auto contains_tx_after = containsTransformation(io_context, evm_instance, "LazyTx");
    ASSERT_TRUE(contains_tx_after.has_value());
    EXPECT_TRUE(*contains_tx_after);

    const auto contains_connector_after = containsConnector(io_context, evm_instance, "LazyConnector");
    ASSERT_TRUE(contains_connector_after.has_value());
    EXPECT_TRUE(*contains_connector_after);
}

TEST_F(UnitTest, API_Execute_BrokenDbDependencyReturnsInvariantError)
{
    ASSERT_TRUE(std::filesystem::exists(solcPath())) << std::format("Missing Solidity compiler at '{}'", solcPath().string());
    ASSERT_TRUE(std::filesystem::exists(ptPath() / "contracts")) << std::format("Missing PT contracts at '{}'", (ptPath() / "contracts").string());

    const auto storage_path = makeTestPath("execute_invariant_error");
    PathScope storage_scope(storage_path);
    ASSERT_TRUE(prepareStorageLayout(storage_path));
    const auto db_path = storage_path / "registry.sqlite";

    const chain::Address owner = makeAddressFromByte(0x61);
    const std::string owner_hex = evmc::hex(owner);

    ConnectorRecord broken_connector = makeConnectorRecord("BrokenConnector", owner_hex);
    addConnectorDimension(broken_connector, "", "MissingTx");

    {
        registry::SQLiteRegistryStore store(db_path.string());
        const evmc::bytes32 zero_format_hash{};
        const std::vector<registry::ScalarLabel> empty_labels;
        ASSERT_TRUE(store.addConnector(makeAddressFromByte(0x62), broken_connector, zero_format_hash, empty_labels));
    }

    asio::io_context io_context;
    registry::Registry registry(io_context, db_path.string());
    auth::AuthManager auth_manager(io_context);
    evm::EVM evm_instance(io_context, EVMC_SHANGHAI, solcPath(), ptPath());
    io_context.run();

    const chain::Address caller = makeAddressFromByte(0x63);
    const std::string access_token = runAwaitable(io_context, auth_manager.generateAccessToken(caller));

    config::Config cfg;
    cfg.storage_path = storage_path;

    auto request = makeExecuteRequest(access_token, "BrokenConnector");
    const auto response = runAwaitable(
        io_context,
        POST_execute(
            request,
            std::vector<server::RouteArg>{},
            server::QueryArgsList{},
            auth_manager,
            registry,
            evm_instance,
            cfg));

    ASSERT_EQ(response.getCode(), http::Code::InternalServerError);
    const auto body = json::parse(response.getBody(), nullptr, false);
    ASSERT_FALSE(body.is_discarded());
    EXPECT_EQ(body["message"], "Connector deployment invariant failed");

    const auto contains_connector_after = containsConnector(io_context, evm_instance, "BrokenConnector");
    ASSERT_TRUE(contains_connector_after.has_value());
    EXPECT_FALSE(*contains_connector_after);
}

TEST_F(UnitTest, SQLiteRegistryStore_QueryHelpers_PrepareFailureReturnsSafeDefaults)
{
    const auto storage_path = makeTestPath("sqlite_prepare_failure");
    PathScope storage_scope(storage_path);
    ASSERT_TRUE(prepareStorageLayout(storage_path));
    const auto db_path = storage_path / "registry.sqlite";

    registry::SQLiteRegistryStore store(db_path.string());

    ASSERT_TRUE(executeSqlScript(
        db_path,
        "PRAGMA foreign_keys=OFF;"
        "DROP TABLE connectors;"
        "DROP TABLE transformations;"
        "DROP TABLE conditions;"
        "DROP TABLE format_members;"
        "DROP TABLE scalar_labels_by_format;"
        "DROP TABLE owned_connectors;"
        "DROP TABLE owned_transformations;"
        "DROP TABLE owned_conditions;"));

    const evmc::bytes32 format_hash{};
    const chain::Address owner = makeAddressFromByte(0x64);

    EXPECT_FALSE(store.hasConnector("missing"));
    EXPECT_FALSE(store.getConnectorRecordHandle("missing").has_value());
    EXPECT_FALSE(store.getConnectorFormatHash("missing").has_value());

    EXPECT_EQ(store.getFormatConnectorNamesCount(format_hash), 0u);
    const auto format_page = store.getFormatConnectorNamesCursor(format_hash, std::nullopt, 16);
    EXPECT_TRUE(format_page.entries.empty());
    EXPECT_FALSE(format_page.has_more);
    EXPECT_FALSE(format_page.next_after.has_value());
    EXPECT_FALSE(store.getScalarLabelsByFormatHash(format_hash).has_value());

    EXPECT_FALSE(store.hasTransformation("missing"));
    EXPECT_FALSE(store.getTransformationRecordHandle("missing").has_value());

    EXPECT_FALSE(store.hasCondition("missing"));
    EXPECT_FALSE(store.getConditionRecordHandle("missing").has_value());

    const auto owned_connectors_page = store.getOwnedConnectorsCursor(owner, std::nullopt, 16);
    EXPECT_TRUE(owned_connectors_page.entries.empty());
    EXPECT_FALSE(owned_connectors_page.has_more);
    EXPECT_FALSE(owned_connectors_page.next_after.has_value());

    const auto owned_transformations_page = store.getOwnedTransformationsCursor(owner, std::nullopt, 16);
    EXPECT_TRUE(owned_transformations_page.entries.empty());
    EXPECT_FALSE(owned_transformations_page.has_more);
    EXPECT_FALSE(owned_transformations_page.next_after.has_value());

    const auto owned_conditions_page = store.getOwnedConditionsCursor(owner, std::nullopt, 16);
    EXPECT_TRUE(owned_conditions_page.entries.empty());
    EXPECT_FALSE(owned_conditions_page.has_more);
    EXPECT_FALSE(owned_conditions_page.next_after.has_value());
}

TEST_F(UnitTest, Loader_StartupImport_DbHitJsonIsNoopAndKeepsFile)
{
    ASSERT_TRUE(std::filesystem::exists(solcPath())) << std::format("Missing Solidity compiler at '{}'", solcPath().string());
    ASSERT_TRUE(std::filesystem::exists(ptPath() / "contracts")) << std::format("Missing PT contracts at '{}'", (ptPath() / "contracts").string());

    const auto storage_path = makeTestPath("startup_import_db_hit");
    PathScope storage_scope(storage_path);
    ASSERT_TRUE(prepareStorageLayout(storage_path));
    const auto db_path = storage_path / "registry.sqlite";

    asio::io_context io_context;
    registry::Registry registry(io_context, db_path.string());
    evm::EVM evm_instance(io_context, EVMC_SHANGHAI, solcPath(), ptPath());
    io_context.run();

    const std::string owner_hex = evmc::hex(makeAddressFromByte(0x71));
    const auto db_record = makeTransformationRecord("ImportedTx", owner_hex, "return x;");
    ASSERT_TRUE(runAwaitable(io_context, registry.addTransformation(makeAddressFromByte(0x72), db_record)));

    const auto contains_before = containsTransformation(io_context, evm_instance, "ImportedTx");
    ASSERT_TRUE(contains_before.has_value());
    EXPECT_FALSE(*contains_before);

    const auto json_record = makeTransformationRecord("ImportedTx", owner_hex, "return x + 99;");
    const auto json_file_path = storage_path / "transformations" / "ImportedTx.json";
    ASSERT_TRUE(writeJsonRecord(json_file_path, json_record));

    const auto file_before = readTextFile(json_file_path);
    ASSERT_TRUE(file_before.has_value());

    const bool import_result = runAwaitable(
        io_context,
        loader::importJsonStorageToDatabase(evm_instance, registry, storage_path));
    EXPECT_TRUE(import_result);

    const auto file_after = readTextFile(json_file_path);
    ASSERT_TRUE(file_after.has_value());
    EXPECT_EQ(*file_after, *file_before);

    const auto persisted_record = runAwaitable(io_context, registry.getTransformationRecordHandle("ImportedTx"));
    ASSERT_TRUE(persisted_record.has_value());
    ASSERT_TRUE(*persisted_record);
    EXPECT_EQ((*persisted_record)->transformation().sol_src(), "return x;");

    const auto contains_after = containsTransformation(io_context, evm_instance, "ImportedTx");
    ASSERT_TRUE(contains_after.has_value());
    EXPECT_FALSE(*contains_after);
}

TEST_F(UnitTest, Loader_StartupImport_DbMissImportsConnectorDependencyChain)
{
    ASSERT_TRUE(std::filesystem::exists(solcPath())) << std::format("Missing Solidity compiler at '{}'", solcPath().string());
    ASSERT_TRUE(std::filesystem::exists(ptPath() / "contracts")) << std::format("Missing PT contracts at '{}'", (ptPath() / "contracts").string());

    const auto storage_path = makeTestPath("startup_import_chain");
    PathScope storage_scope(storage_path);
    ASSERT_TRUE(prepareStorageLayout(storage_path));
    const auto db_path = storage_path / "registry.sqlite";

    asio::io_context io_context;
    registry::Registry registry(io_context, db_path.string());
    evm::EVM evm_instance(io_context, EVMC_SHANGHAI, solcPath(), ptPath());
    io_context.run();

    const std::string owner_hex = evmc::hex(makeAddressFromByte(0x81));

    const auto transformation_record = makeTransformationRecord("ImportTx", owner_hex);
    const auto condition_record = makeConditionRecord("ImportCond", owner_hex, "return true;");

    ConnectorRecord leaf_connector = makeConnectorRecord("ImportLeaf", owner_hex);
    addConnectorDimension(leaf_connector, "", "ImportTx");

    ConnectorRecord root_connector = makeConnectorRecord("ImportRoot", owner_hex);
    addConnectorDimension(root_connector, "ImportLeaf", "ImportTx");
    root_connector.mutable_connector()->set_condition_name("ImportCond");

    ASSERT_TRUE(writeJsonRecord(storage_path / "transformations" / "ImportTx.json", transformation_record));
    ASSERT_TRUE(writeJsonRecord(storage_path / "conditions" / "ImportCond.json", condition_record));
    ASSERT_TRUE(writeJsonRecord(storage_path / "connectors" / "ImportLeaf.json", leaf_connector));
    ASSERT_TRUE(writeJsonRecord(storage_path / "connectors" / "ImportRoot.json", root_connector));

    const bool import_result = runAwaitable(
        io_context,
        loader::importJsonStorageToDatabase(evm_instance, registry, storage_path));
    EXPECT_TRUE(import_result);

    const auto tx_handle = runAwaitable(io_context, registry.getTransformationRecordHandle("ImportTx"));
    ASSERT_TRUE(tx_handle.has_value());
    ASSERT_TRUE(*tx_handle);

    const auto cond_handle = runAwaitable(io_context, registry.getConditionRecordHandle("ImportCond"));
    ASSERT_TRUE(cond_handle.has_value());
    ASSERT_TRUE(*cond_handle);

    const auto leaf_handle = runAwaitable(io_context, registry.getConnectorRecordHandle("ImportLeaf"));
    ASSERT_TRUE(leaf_handle.has_value());
    ASSERT_TRUE(*leaf_handle);

    const auto root_handle = runAwaitable(io_context, registry.getConnectorRecordHandle("ImportRoot"));
    ASSERT_TRUE(root_handle.has_value());
    ASSERT_TRUE(*root_handle);
    EXPECT_EQ((*root_handle)->connector().condition_name(), "ImportCond");

    const auto contains_tx = containsTransformation(io_context, evm_instance, "ImportTx");
    ASSERT_TRUE(contains_tx.has_value());
    EXPECT_TRUE(*contains_tx);

    const auto contains_cond = containsCondition(io_context, evm_instance, "ImportCond");
    ASSERT_TRUE(contains_cond.has_value());
    EXPECT_TRUE(*contains_cond);

    const auto contains_leaf = containsConnector(io_context, evm_instance, "ImportLeaf");
    ASSERT_TRUE(contains_leaf.has_value());
    EXPECT_TRUE(*contains_leaf);

    const auto contains_root = containsConnector(io_context, evm_instance, "ImportRoot");
    ASSERT_TRUE(contains_root.has_value());
    EXPECT_TRUE(*contains_root);
}

TEST_F(UnitTest, Loader_StartupImport_UnresolvedDependencySkipsAndDoesNotInsertObject)
{
    ASSERT_TRUE(std::filesystem::exists(solcPath())) << std::format("Missing Solidity compiler at '{}'", solcPath().string());
    ASSERT_TRUE(std::filesystem::exists(ptPath() / "contracts")) << std::format("Missing PT contracts at '{}'", (ptPath() / "contracts").string());

    const auto storage_path = makeTestPath("startup_import_missing_dependency");
    PathScope storage_scope(storage_path);
    ASSERT_TRUE(prepareStorageLayout(storage_path));
    const auto db_path = storage_path / "registry.sqlite";

    asio::io_context io_context;
    registry::Registry registry(io_context, db_path.string());
    evm::EVM evm_instance(io_context, EVMC_SHANGHAI, solcPath(), ptPath());
    io_context.run();

    const std::string owner_hex = evmc::hex(makeAddressFromByte(0x91));
    ConnectorRecord broken_connector = makeConnectorRecord("ImportBroken", owner_hex);
    addConnectorDimension(broken_connector, "", "MissingImportTx");
    ASSERT_TRUE(writeJsonRecord(storage_path / "connectors" / "ImportBroken.json", broken_connector));

    const bool import_result = runAwaitable(
        io_context,
        loader::importJsonStorageToDatabase(evm_instance, registry, storage_path));
    EXPECT_FALSE(import_result);

    const auto connector_handle = runAwaitable(io_context, registry.getConnectorRecordHandle("ImportBroken"));
    EXPECT_FALSE(connector_handle.has_value());

    const auto contains_connector = containsConnector(io_context, evm_instance, "ImportBroken");
    ASSERT_TRUE(contains_connector.has_value());
    EXPECT_FALSE(*contains_connector);
}
