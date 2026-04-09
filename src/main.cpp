#include "decentralised_art.hpp"
#include <exception>
#include <string_view>

#ifndef Solidity_SOLC_EXECUTABLE
    #error "Solidity_SOLC_EXECUTABLE is not defined"
#endif

static void _configureLogger(const std::filesystem::path& logs_path)
{
    std::filesystem::create_directories(logs_path);

    // Create sinks
    auto console_sink = std::make_shared<spdlog::sinks::stdout_color_sink_mt>();
    const std::string log_name = dcn::utils::currentTimestamp() + "-DCNServer.log";
    auto file_sink = std::make_shared<spdlog::sinks::basic_file_sink_mt>(
        (logs_path / log_name).string(), true);

    // set different log levels per sink
    console_sink->set_level(spdlog::level::info);
    file_sink->set_level(spdlog::level::debug);
    console_sink->set_pattern("[%T] [%^%l%$] %v");
    file_sink->set_pattern("[%Y-%m-%d %H:%M:%S] [%l] %v");
    
    spdlog::logger logger("multi_sink", {console_sink, file_sink});
    logger.set_level(spdlog::level::debug);
    logger.flush_on(spdlog::level::info);

    spdlog::set_default_logger(std::make_shared<spdlog::logger>(logger));
}

static std::vector<int> _configureShutdownSignals()
{
    std::vector<int> shutdown_signal_ids{
        SIGINT,
        SIGTERM
    };
#if defined(SIGBREAK)
    shutdown_signal_ids.push_back(SIGBREAK);
#endif
#if defined(SIGQUIT)
    shutdown_signal_ids.push_back(SIGQUIT);
#endif
#if defined(SIGHUP)
    shutdown_signal_ids.push_back(SIGHUP);
#endif
    return shutdown_signal_ids;
}

static asio::awaitable<void> _runStartupAndListen(
    dcn::storage::Registry & registry,
    dcn::evm::EVM & evm,
    dcn::server::Server & server,
    const dcn::config::Config & cfg)
{
    const dcn::loader::LoaderBatchConfig loader_batch_config{
        .connectors = cfg.loader_batch_connectors,
        .transformations = cfg.loader_batch_transformations,
        .conditions = cfg.loader_batch_conditions
    };

    spdlog::info("Starting JSON storage import...");

    const bool import_success = co_await dcn::loader::importJsonStorageToDatabase(
        evm,
        registry,
        cfg.storage_path,
        loader_batch_config);

    if(!import_success)
    {
        spdlog::warn("JSON storage import finished with errors");
    }
    else
    {
        spdlog::info("JSON storage import finished");
    }

    if(cfg.chain_ingestion.enabled)
    {
        //asio::co_spawn(io_context, dcn::chain::runEventIngestion(chain_ingestion_cfg, registry), asio::detached);
    }

    co_await server.listen();
    co_return;
}

static asio::awaitable<void> _runGracefulShutdown(
    dcn::server::Server & server,
    std::optional<dcn::storage::RegistryWalSyncWorker> & wal_sync_worker,
    std::atomic<bool> & wal_sync_worker_stopped,
    dcn::storage::Registry & registry,
    bool wal_enabled)
{
    spdlog::info("Decentralised Art server stopping...");
    co_await server.close();
    spdlog::info("Decentralised Art server close requested");

    if(wal_sync_worker != std::nullopt)
    {
        spdlog::info("Requesting registry WAL sync worker stop...");
        wal_sync_worker->requestStop();

        auto exec = co_await asio::this_coro::executor;
        asio::steady_timer wait_timer(exec);

        while(!wal_sync_worker_stopped.load(std::memory_order_acquire))
        {
            wait_timer.expires_after(std::chrono::milliseconds(25));
            std::error_code wait_ec;
            co_await wait_timer.async_wait(asio::redirect_error(asio::use_awaitable, wait_ec));
            if(wait_ec)
            {
                if(wait_ec != asio::error::operation_aborted)
                {
                    spdlog::warn("Waiting for registry WAL sync worker stop failed: {}", wait_ec.message());
                }
                break;
            }
        }

        spdlog::info("Registry WAL sync worker stopped");
    }

    if(wal_enabled)
    {
        spdlog::info("Running final registry WAL truncate checkpoint...");
        const bool checkpoint_ok =
            co_await registry.checkpointWal(dcn::storage::WalCheckpointMode::TRUNCATE);
        if(!checkpoint_ok)
        {
            spdlog::warn("Final registry WAL truncate checkpoint failed");
        }
        else
        {
            spdlog::info("Final registry WAL truncate checkpoint complete");
        }
    }

    co_return;
}

static void _runImmediateShutdown(std::optional<dcn::storage::RegistryWalSyncWorker> & wal_sync_worker)
{
    if(wal_sync_worker)
    {
        spdlog::info("Registry WAL sync worker stopping...");
        wal_sync_worker->requestStop();
        spdlog::info("Stopped registry WAL sync worker");
    }
}

int main(int argc, char* argv[])
{
    dcn::config::Config cfg;
    cfg.bin_path = std::filesystem::path(argv[0]).parent_path();
    cfg.logs_path = cfg.bin_path.parent_path() / "logs";
    cfg.resources_path = cfg.bin_path.parent_path() / "resources";
    cfg.storage_path = cfg.bin_path.parent_path() / "storage";

    const bool terminal_configured = dcn::native::configureTerminal();
    _configureLogger(cfg.logs_path);

    if(!terminal_configured)
    {
        std::printf("%s", dcn::utils::getLogo(dcn::utils::LogoASCII).c_str());
        std::fflush(stdout);
        spdlog::warn("Terminal configuration was not fully applied");
    }
    else
    {
        std::printf("%s", dcn::utils::getLogo(dcn::utils::LogoUnicode).c_str());
        std::fflush(stdout);
        spdlog::debug("Terminal configuration applied successfully");
    }

    const std::string build_timestamp = dcn::utils::loadBuildTimestamp(cfg.bin_path / "build_timestamp");
    spdlog::debug("Build timestamp: {}", build_timestamp);

    spdlog::debug("Version: {}.{}.{}\n", dcn::MAJOR_VERSION, dcn::MINOR_VERSION, dcn::PATCH_VERSION);

    spdlog::debug("Decentralised Art server started with {} arguments", argc);
    for(int i = 0; i < argc; ++i)
    {
        spdlog::debug("Argument at [{}] : {}", i, argv[i]);
    }
    
    dcn::cmd::ArgParser arg_parser;
    arg_parser.addArg<bool>("-h", "Display help message and exit");
    arg_parser.addArg<bool>("--help", "Display help message and exit");
    arg_parser.addArg<bool>("--version", "Display version and exit");
    arg_parser.addArg<bool>("--verbose", "Enable verbose logging");
    arg_parser.addArg<unsigned int>("--port", "Port to listen on");
    arg_parser.addArg<std::string>("--chain-rpc", "Ethereum JSON-RPC endpoint URL used for event sync");
    arg_parser.addArg<std::string>("--chain-registry", "PT registry proxy address on chain");
    arg_parser.addArg<unsigned int>("--chain-start-block", "Optional first block for event sync when no local cursor exists");
    arg_parser.addArg<unsigned int>("--chain-poll-ms", "Chain poll interval in milliseconds");
    arg_parser.addArg<unsigned int>("--chain-confirmations", "Finality confirmation depth");
    arg_parser.addArg<unsigned int>("--chain-batch-size", "Max number of blocks fetched per eth_getLogs request");
    arg_parser.addArg<std::filesystem::path>("--registry-db", "SQLite path for registry storage");
    arg_parser.addArg<unsigned int>("--registry-wal-sync-ms", "Interval in milliseconds for periodic SQLite WAL passive checkpoints");
    arg_parser.addArg<unsigned int>("--loader-batch-connectors", "Batch size used while adding loaded connectors to registry");
    arg_parser.addArg<unsigned int>("--loader-batch-transformations", "Batch size used while adding loaded transformations to registry");
    arg_parser.addArg<unsigned int>("--loader-batch-conditions", "Batch size used while adding loaded conditions to registry");

    arg_parser.parse(argc, argv);

    if(arg_parser.getArg<bool>("--version").value_or(false))
    {
        spdlog::info("Decentralised Art server build timestamp: {}", build_timestamp);
        spdlog::info("Version: {}.{}.{}", dcn::MAJOR_VERSION, dcn::MINOR_VERSION, dcn::PATCH_VERSION);
        return 0;
    }

    if(arg_parser.getArg<bool>("--help").value_or(false) || arg_parser.getArg<bool>("-h").value_or(false))
    {
        spdlog::info(arg_parser.constructHelpMessage());
        return 0;
    }
    
    cfg.verbose = arg_parser.getArg<bool>("--verbose").value_or(false);

    cfg.port = arg_parser.getArg<unsigned int>("--port").value_or(dcn::DEFAULT_PORT);

    cfg.chain_ingestion.poll_interval_ms = arg_parser.getArg<unsigned int>("--chain-poll-ms").value_or(5000);
    cfg.chain_ingestion.confirmations = arg_parser.getArg<unsigned int>("--chain-confirmations").value_or(12);
    cfg.chain_ingestion.block_batch_size = arg_parser.getArg<unsigned int>("--chain-batch-size").value_or(500);

    // cfg.chain_ingestion.start_block = arg_parser.getArg<unsigned int>("--chain-start-block").value_or(0);
    cfg.chain_ingestion.rpc_url = arg_parser.getArg<std::string>("--chain-rpc").value_or("");
    cfg.chain_ingestion.registry_address = arg_parser.getArg<std::string>("--chain-registry").value_or("");


    // if((chain_rpc_arg.has_value() && !chain_registry_arg.has_value()) ||
    //     (!chain_rpc_arg.has_value() && chain_registry_arg.has_value()))
    // {
    //     spdlog::error("Both --chain-rpc and --chain-registry must be provided together");
    //     return 1;
    // }

    // if(chain_rpc_arg && chain_registry_arg)
    // {
    //     const auto registry_addr_res = evmc::from_hex<dcn::chain::Address>(chain_registry_arg->at(0));
    //     if(!registry_addr_res)
    //     {
    //         spdlog::error("Invalid --chain-registry address");
    //         return 1;
    //     }

    //     chain_ingestion_cfg.enabled = true;
    //     chain_ingestion_cfg.rpc_url = chain_rpc_arg->at(0);
    //     chain_ingestion_cfg.registry_address = *registry_addr_res;

    //     spdlog::info(
    //         "Chain sync enabled. Registry={}, poll={}ms, confirmations={}, batch={}",
    //         chain_registry_arg->at(0),
    //         chain_ingestion_cfg.poll_interval_ms,
    //         chain_ingestion_cfg.confirmations,
    //         chain_ingestion_cfg.block_batch_size);
    // }

    cfg.loader_batch_connectors = arg_parser.getArg<unsigned int>("--loader-batch-connectors").value_or(1000);
    cfg.loader_batch_transformations = arg_parser.getArg<unsigned int>("--loader-batch-transformations").value_or(5000);
    cfg.loader_batch_conditions = arg_parser.getArg<unsigned int>("--loader-batch-conditions").value_or(5000);

    cfg.registry_wal_sync_ms = arg_parser.getArg<unsigned int>("--registry-wal-sync-ms").value_or(30000);

    const std::chrono::milliseconds registry_wal_sync_interval(cfg.registry_wal_sync_ms);

    cfg.registry_db = arg_parser.getArg<std::filesystem::path>("--registry-db").value_or(
        cfg.storage_path / "registry.sqlite"
    );

    spdlog::info("Current working path: {}", std::filesystem::current_path().string());

    // solidity check
    const auto solc_path = cfg.bin_path / TOSTRING(Solidity_SOLC_EXECUTABLE);

    spdlog::info(std::format("Path to solidity solc compiler : {}", solc_path.string()));

    const auto [exit_code, solc_version_out] = dcn::native::runProcess(solc_path.string(), {"--version"});
    spdlog::info(std::format("Solc info:\n{}", solc_version_out));
    
    const auto pt_path = cfg.bin_path.parent_path() / "pt";
    spdlog::info(std::format("Path to PT framework : {}", pt_path.string()));

    const bool registry_db_in_memory = cfg.registry_db.empty() || cfg.registry_db == ":memory:";

    asio::io_context io_context;

    dcn::storage::Registry registry(io_context, cfg.registry_db.string());

    dcn::auth::AuthManager auth_manager(io_context);

    dcn::evm::EVM evm(io_context, EVMC_SHANGHAI, solc_path, pt_path);

    dcn::server::Server server(io_context, {asio::ip::tcp::v4(), asio::ip::port_type(cfg.port)});

    server.setIdleInterval(5000ms);
    
    const auto favicon = dcn::file::loadBinaryFile(cfg.resources_path / "media" / "img" / "favicon.svg");
    
    // HTML
    const auto simple_form_html = dcn::file::loadTextFile(cfg.resources_path / "html" / "simple_form.html");
    
    // JS
    const auto simple_form_js = dcn::file::loadTextFile(cfg.resources_path / "js" / "simple_form.js");
    const auto auth_js = dcn::file::loadTextFile(cfg.resources_path / "js" / "auth.js");
    const auto execute_js = dcn::file::loadTextFile(cfg.resources_path / "js" / "execute.js");
    const auto utils_js = dcn::file::loadTextFile(cfg.resources_path / "js" / "utils.js");

    // CSS
    const auto simple_form_css = dcn::file::loadTextFile(cfg.resources_path / "styles" / "simple_form.css");

    if(simple_form_html && simple_form_js && auth_js && execute_js && utils_js && simple_form_css)
    {
        server.addRoute({dcn::http::Method::HEAD, "/"},     dcn::HEAD_serveFile);
        server.addRoute({dcn::http::Method::OPTIONS, "/"},  dcn::OPTIONS_serveFile);
        server.addRoute({dcn::http::Method::GET, "/"},      dcn::GET_serveFile, "text/html; charset=utf-8", std::cref(simple_form_html.value()));

        server.addRoute({dcn::http::Method::HEAD, "/js/simple_form"},    dcn::HEAD_serveFile);
        server.addRoute({dcn::http::Method::OPTIONS, "/js/simple_form"}, dcn::OPTIONS_serveFile);
        server.addRoute({dcn::http::Method::GET, "/js/simple_form"},     dcn::GET_serveFile, "text/javascript; charset=utf-8", std::cref(simple_form_js.value()));
    
        server.addRoute({dcn::http::Method::HEAD, "/js/auth"},    dcn::HEAD_serveFile);
        server.addRoute({dcn::http::Method::OPTIONS, "/js/auth"}, dcn::OPTIONS_serveFile);
        server.addRoute({dcn::http::Method::GET, "/js/auth"},     dcn::GET_serveFile, "text/javascript; charset=utf-8", std::cref(auth_js.value()));

        server.addRoute({dcn::http::Method::HEAD, "/js/execute"},    dcn::HEAD_serveFile);
        server.addRoute({dcn::http::Method::OPTIONS, "/js/execute"}, dcn::OPTIONS_serveFile);
        server.addRoute({dcn::http::Method::GET, "/js/execute"},     dcn::GET_serveFile, "text/javascript; charset=utf-8", std::cref(execute_js.value()));

        server.addRoute({dcn::http::Method::HEAD, "/js/utils"},    dcn::HEAD_serveFile);
        server.addRoute({dcn::http::Method::OPTIONS, "/js/utils"}, dcn::OPTIONS_serveFile);
        server.addRoute({dcn::http::Method::GET, "/js/utils"},     dcn::GET_serveFile, "text/javascript; charset=utf-8", std::cref(utils_js.value()));

        server.addRoute({dcn::http::Method::HEAD, "/styles/simple_form.css"},       dcn::HEAD_serveFile);
        server.addRoute({dcn::http::Method::OPTIONS, "/styles/simple_form.css"},    dcn::OPTIONS_serveFile);
        server.addRoute({dcn::http::Method::GET, "/styles/simple_form.css"},        dcn::GET_serveFile, "text/css; charset=utf-8", std::cref(simple_form_css.value()));
    }
    else
    {
        spdlog::error("Failed to load static files");
    }

    if(favicon)
    {
        server.addRoute({dcn::http::Method::HEAD, "/favicon.svg"},      dcn::HEAD_serveFile);
        server.addRoute({dcn::http::Method::OPTIONS, "/favicon.svg"},   dcn::OPTIONS_serveFile);
        server.addRoute({dcn::http::Method::GET, "/favicon.svg"},       dcn::GET_serveBinaryFile, "image/svg+xml; charset=utf-8", std::cref(favicon.value()));
    }
    else
    {
        spdlog::error("Failed to load favicon");
    }
    
    server.addRoute({dcn::http::Method::GET, "/version"},   dcn::GET_version, std::cref(build_timestamp));

    server.addRoute({dcn::http::Method::GET, "/nonce/<string>"},    dcn::GET_nonce, std::ref(auth_manager));

    server.addRoute({dcn::http::Method::OPTIONS, "/auth"},  dcn::OPTIONS_auth);
    server.addRoute({dcn::http::Method::POST, "/auth"},     dcn::POST_auth, std::ref(auth_manager));

    server.addRoute(
        {dcn::http::Method::OPTIONS, "/account/<string>?limit=<uint>&after_connectors=<~string>&after_transformations=<~string>&after_conditions=<~string>"},
        dcn::OPTIONS_accountInfo);
    server.addRoute(
        {dcn::http::Method::GET, "/account/<string>?limit=<uint>&after_connectors=<~string>&after_transformations=<~string>&after_conditions=<~string>"},
        dcn::GET_accountInfo,
        std::ref(registry));

    server.addRoute({dcn::http::Method::OPTIONS, "/format/<string>?limit=<uint>&after=<~string>"}, dcn::OPTIONS_format);
    server.addRoute({dcn::http::Method::GET, "/format/<string>?limit=<uint>&after=<~string>"}, dcn::GET_format, std::ref(registry));

    server.addRoute({dcn::http::Method::HEAD, "/connector/<string>"},       dcn::HEAD_connector, std::ref(registry));
    server.addRoute({dcn::http::Method::OPTIONS, "/connector"},             dcn::OPTIONS_connector);
    server.addRoute({dcn::http::Method::OPTIONS, "/connector/<string>"},    dcn::OPTIONS_connector);
    server.addRoute({dcn::http::Method::GET,     "/connector/<string>"},    dcn::GET_connector, std::ref(registry));
    server.addRoute({dcn::http::Method::POST,    "/connector"},                       dcn::POST_connector, std::ref(auth_manager), std::ref(registry), std::ref(evm), std::cref(cfg));

    server.addRoute({dcn::http::Method::HEAD, "/transformation/<string>"},    dcn::HEAD_transformation, std::ref(registry));
    server.addRoute({dcn::http::Method::OPTIONS, "/transformation"},          dcn::OPTIONS_transformation);
    server.addRoute({dcn::http::Method::OPTIONS, "/transformation/<string>"}, dcn::OPTIONS_transformation);
    server.addRoute({dcn::http::Method::GET,     "/transformation/<string>"}, dcn::GET_transformation, std::ref(registry));
    server.addRoute({dcn::http::Method::POST,    "/transformation"},                    dcn::POST_transformation, std::ref(auth_manager), std::ref(registry), std::ref(evm), std::cref(cfg));

    server.addRoute({dcn::http::Method::HEAD, "/condition/<string>"},    dcn::HEAD_condition, std::ref(registry));
    server.addRoute({dcn::http::Method::OPTIONS, "/condition"},          dcn::OPTIONS_condition);
    server.addRoute({dcn::http::Method::OPTIONS, "/condition/<string>"}, dcn::OPTIONS_condition);
    server.addRoute({dcn::http::Method::GET,     "/condition/<string>"}, dcn::GET_condition, std::ref(registry));
    server.addRoute({dcn::http::Method::POST,    "/condition"},                    dcn::POST_condition, std::ref(auth_manager), std::ref(registry), std::ref(evm), std::cref(cfg));

    server.addRoute({dcn::http::Method::OPTIONS, "/execute"},   dcn::OPTIONS_execute);
    server.addRoute({dcn::http::Method::POST, "/execute"},      dcn::POST_execute, std::cref(auth_manager), std::ref(registry), std::ref(evm), std::cref(cfg));

    // create directories
    std::filesystem::create_directory(cfg.storage_path);
    std::filesystem::create_directory(cfg.storage_path / "connectors");
    std::filesystem::create_directory(cfg.storage_path / "connectors" / "build");
    std::filesystem::create_directory(cfg.storage_path / "transformations");
    std::filesystem::create_directory(cfg.storage_path / "transformations" / "build");
    std::filesystem::create_directory(cfg.storage_path / "conditions");
    std::filesystem::create_directory(cfg.storage_path / "conditions" / "build");

    if(!cfg.registry_db.parent_path().empty())
    {
        std::error_code registry_dir_ec;
        std::filesystem::create_directories(cfg.registry_db.parent_path(), registry_dir_ec);
        if(registry_dir_ec)
        {
            spdlog::error(
                "Failed to create registry DB directory '{}': {}",
                cfg.registry_db.parent_path().string(),
                registry_dir_ec.message());
            return 1;
        }
    }

    if(!dcn::loader::ensurePTBuildVersion(cfg.storage_path))
    {
        spdlog::error("Failed to prepare PT Solidity build cache");
        return 1;
    }


    const bool wal_enabled = !registry_db_in_memory;
    std::atomic<bool> wal_sync_worker_stopped = true;
    std::optional<dcn::storage::RegistryWalSyncWorker> wal_sync_worker;
    if(wal_enabled)
    {
        // create wal sync worker
        wal_sync_worker_stopped.store(false, std::memory_order_release);
        wal_sync_worker.emplace(io_context, registry,  registry_wal_sync_interval);
        
        // start wal sync worker
        asio::co_spawn(
            io_context,
            wal_sync_worker->run(),
            [&wal_sync_worker_stopped](std::exception_ptr exception_ptr)
            {
                wal_sync_worker_stopped.store(true, std::memory_order_release);
                dcn::utils::logException(exception_ptr, "Registry WAL sync worker failed");
            }
        );
        spdlog::info("Registry WAL sync worker enabled with interval {}ms", registry_wal_sync_interval.count());
    }
    else
    {
        spdlog::info("Registry WAL sync worker disabled for in-memory SQLite database");
    }

    dcn::async::SignalWorker signal_worker(
        io_context,
        _configureShutdownSignals(),

        // graceful shutdown
        [&server,
         &wal_sync_worker,
         &wal_sync_worker_stopped,
         &registry,
         wal_enabled]() -> asio::awaitable<void>
        {
            return _runGracefulShutdown(
                server,
                wal_sync_worker,
                wal_sync_worker_stopped,
                registry,
                wal_enabled);
        },

        // immediate shutdown
        [&wal_sync_worker]()
        {
            _runImmediateShutdown(wal_sync_worker);
        }
    );
    
    signal_worker.start();

    asio::co_spawn(
        io_context,
        _runStartupAndListen(
            registry,
            evm,
            server,
            cfg),
        [&io_context](std::exception_ptr exception_ptr)
        {
            if(!exception_ptr)
            {
                return;
            }

            dcn::utils::logException(exception_ptr, "Startup/listen coroutine failed");
            spdlog::critical("Stopping io_context due to startup/listen failure");
            io_context.stop();
        });

    try
    {
        io_context.run();
    }catch(std::exception & e)
    {
        spdlog::error("Error: {}", e.what());
    }
    catch(...)
    {
        spdlog::error("Unknown error");
    }
    

    spdlog::debug("Program finished");
    return 0;
}
