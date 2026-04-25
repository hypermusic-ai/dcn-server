#include "decentralised_art.hpp"
#include <exception>
#include <string_view>

#ifndef Solidity_SOLC_EXECUTABLE
    #error "Solidity_SOLC_EXECUTABLE is not defined"
#endif

static bool _createDirectories(const dcn::config::Config & cfg)
{
    std::error_code creation_dir_ec;

    // Create storage directory if it doesn't exist
    if(std::filesystem::exists(cfg.storage_path) == false)
    {
        std::filesystem::create_directory(cfg.storage_path, creation_dir_ec);
        if(creation_dir_ec)
        {
            spdlog::error(
                "Failed to create storage directory '{}': {}",
                cfg.storage_path.string(),
                creation_dir_ec.message());
            return false;
        }
    }

    // Create connectors directory if it doesn't exist
    if(std::filesystem::exists(cfg.storage_path / "connectors") == false)
    {
        std::filesystem::create_directory(cfg.storage_path / "connectors", creation_dir_ec);
        if(creation_dir_ec)
        {
            spdlog::error(
                "Failed to create connectors directory '{}': {}",
                (cfg.storage_path / "connectors").string(),
                creation_dir_ec.message());
            return false;
        }
    }

    // Create connectors build directory if it doesn't exist
    if(std::filesystem::exists(cfg.storage_path / "connectors" / "build") == false)
    {
        std::filesystem::create_directory(cfg.storage_path / "connectors" / "build", creation_dir_ec);
        if(creation_dir_ec)
        {
            spdlog::error(
                "Failed to create connectors build directory '{}': {}",
                (cfg.storage_path / "connectors" / "build").string(),
                creation_dir_ec.message());
            return false;
        }
    }

    // Create transformations directory if it doesn't exist
    if(std::filesystem::exists(cfg.storage_path / "transformations") == false)
    {
        std::filesystem::create_directory(cfg.storage_path / "transformations", creation_dir_ec);
        if(creation_dir_ec)
        {
            spdlog::error(
                "Failed to create transformations directory '{}': {}",
                (cfg.storage_path / "transformations").string(),
                creation_dir_ec.message());
            return false;
        }
    }

    // Create transformations build directory if it doesn't exist
    if(std::filesystem::exists(cfg.storage_path / "transformations" / "build") == false)
    {
        std::filesystem::create_directory(cfg.storage_path / "transformations" / "build", creation_dir_ec);
        if(creation_dir_ec)
        {
            spdlog::error(
                "Failed to create transformations build directory '{}': {}",
                (cfg.storage_path / "transformations" / "build").string(),
                creation_dir_ec.message());
            return false;
        }
    }

    // Create conditions directory if it doesn't exist
    if(std::filesystem::exists(cfg.storage_path / "conditions") == false)
    {
        std::filesystem::create_directory(cfg.storage_path / "conditions", creation_dir_ec);
        if(creation_dir_ec)
        {
            spdlog::error(
                "Failed to create conditions directory '{}': {}",
                (cfg.storage_path / "conditions").string(),
                creation_dir_ec.message());
            return false;
        }
    }

    // Create conditions build directory if it doesn't exist
    if(std::filesystem::exists(cfg.storage_path / "conditions" / "build") == false)
    {
        std::filesystem::create_directory(cfg.storage_path / "conditions" / "build", creation_dir_ec);
        if(creation_dir_ec)
        {
            spdlog::error(
                "Failed to create conditions build directory '{}': {}",
                (cfg.storage_path / "conditions" / "build").string(),
                creation_dir_ec.message());
            return false;
        }
    }

    // Create registry DB directory if it doesn't exist
    if(!cfg.registry_db.parent_path().empty() && std::filesystem::exists(cfg.registry_db.parent_path()) == false)
    {
        std::filesystem::create_directories(cfg.registry_db.parent_path(), creation_dir_ec);
        if(creation_dir_ec)
        {
            spdlog::error(
                "Failed to create registry DB directory '{}': {}",
                cfg.registry_db.parent_path().string(),
                creation_dir_ec.message());
            return false;
        }
    }

    // Create events DB directory if it doesn't exist
    if(!cfg.events_db.parent_path().empty() && std::filesystem::exists(cfg.events_db.parent_path()) == false)
    {
        std::filesystem::create_directories(cfg.events_db.parent_path(), creation_dir_ec);
        if(creation_dir_ec)
        {
            spdlog::error(
                "Failed to create events DB directory '{}': {}",
                cfg.events_db.parent_path().string(),
                creation_dir_ec.message());
            return false;
        }
    }

    // Create events archive directory if it doesn't exist
    if(!cfg.events_archive_root.empty() && std::filesystem::exists(cfg.events_archive_root) == false)
    {
        std::filesystem::create_directories(cfg.events_archive_root, creation_dir_ec);
        if(creation_dir_ec)
        {
            spdlog::error(
                "Failed to create events archive directory '{}': {}",
                cfg.events_archive_root.string(),
                creation_dir_ec.message());
            return false;
        }
    }

    return true;
}

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
    dcn::registry::Registry & registry,
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
    std::optional<dcn::storage::sqlite::WalSyncWorker> & wal_sync_worker,
    std::atomic<bool> & wal_sync_worker_stopped,
    dcn::registry::Registry & registry,
    bool wal_enabled,
    dcn::events::EventRuntime & events_runtime)
{
    spdlog::info("Decentralised Art server stopping...");
    co_await events_runtime.stop();
    spdlog::info("Events runtime stopped");
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
        const auto stats = co_await registry.checkpointWal(dcn::storage::sqlite::WalCheckpointMode::TRUNCATE);
        if(!stats.ok)
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

static void _runImmediateShutdown(
    std::optional<dcn::storage::sqlite::WalSyncWorker> & wal_sync_worker,
    dcn::events::EventRuntime & events_runtime)
{
    events_runtime.requestStop();
    spdlog::info("Events runtime stop requested");

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
    arg_parser.addArg<bool>("--chain-local-source", "Use in-process EVM as chain event source (no RPC)");
    arg_parser.addArg<std::filesystem::path>("--registry-db", "SQLite path for registry storage");
    arg_parser.addArg<unsigned int>("--registry-wal-sync-ms", "Interval in milliseconds for periodic SQLite WAL passive checkpoints");
    arg_parser.addArg<std::filesystem::path>("--events-db", "SQLite path for events hot storage");
    arg_parser.addArg<std::filesystem::path>("--events-archive-root", "Directory for archived monthly events shards");
    arg_parser.addArg<unsigned int>("--events-chain-id", "Chain id used for events ingestion and feed projection");
    arg_parser.addArg<unsigned int>("--events-hot-window-days", "Retention window in days for hot events storage");
    arg_parser.addArg<unsigned int>("--events-projector-ms", "Interval in milliseconds for projector loop when idle");
    arg_parser.addArg<unsigned int>("--events-archive-ms", "Interval in milliseconds for archive maintenance loop");
    arg_parser.addArg<unsigned int>("--events-reorg-window-blocks", "Rolling block window size for reorg reconciliation");
    arg_parser.addArg<unsigned int>("--events-outbox-retention-days", "Retention window in days for replay outbox rows");
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
    if(const auto start_block_arg = arg_parser.getArg<unsigned int>("--chain-start-block"))
    {
        cfg.chain_ingestion.start_block = static_cast<std::uint64_t>(*start_block_arg);
    }

    cfg.chain_ingestion.rpc_url = arg_parser.getArg<std::string>("--chain-rpc").value_or("");
    cfg.chain_ingestion.registry_address = arg_parser.getArg<std::string>("--chain-registry").value_or("");
    cfg.chain_ingestion.use_local_evm_source = arg_parser.getArg<bool>("--chain-local-source").value_or(false);

    const bool has_chain_rpc = !cfg.chain_ingestion.rpc_url.empty();
    const bool has_chain_registry = !cfg.chain_ingestion.registry_address.empty();
    if(cfg.chain_ingestion.use_local_evm_source && (has_chain_rpc || has_chain_registry))
    {
        spdlog::error("--chain-local-source cannot be combined with --chain-rpc/--chain-registry");
        return 1;
    }

    if(!cfg.chain_ingestion.use_local_evm_source && (has_chain_rpc != has_chain_registry))
    {
        spdlog::error("Both --chain-rpc and --chain-registry must be provided together");
        return 1;
    }

    if(cfg.chain_ingestion.use_local_evm_source)
    {
        cfg.chain_ingestion.enabled = true;
        cfg.chain_ingestion.rpc_url.clear();
        cfg.chain_ingestion.registry_address.clear();
    }
    else if(has_chain_registry)
    {
        const auto registry_addr_res = evmc::from_hex<dcn::chain::Address>(cfg.chain_ingestion.registry_address);
        if(!registry_addr_res)
        {
            spdlog::error("Invalid --chain-registry address");
            return 1;
        }

        cfg.chain_ingestion.registry_address = evmc::hex(*registry_addr_res);
        cfg.chain_ingestion.enabled = true;
    }
    else
    {
        cfg.chain_ingestion.enabled = false;
    }

    cfg.loader_batch_connectors = arg_parser.getArg<unsigned int>("--loader-batch-connectors").value_or(1000);
    cfg.loader_batch_transformations = arg_parser.getArg<unsigned int>("--loader-batch-transformations").value_or(5000);
    cfg.loader_batch_conditions = arg_parser.getArg<unsigned int>("--loader-batch-conditions").value_or(5000);

    cfg.registry_wal_sync_ms = arg_parser.getArg<unsigned int>("--registry-wal-sync-ms").value_or(30000);

    const std::chrono::milliseconds registry_wal_sync_interval(cfg.registry_wal_sync_ms);

    cfg.registry_db = arg_parser.getArg<std::filesystem::path>("--registry-db").value_or(
        cfg.storage_path / "registry.sqlite"
    );

    cfg.events_db = arg_parser.getArg<std::filesystem::path>("--events-db").value_or(
        cfg.storage_path / "events" / "events_hot.sqlite"
    );
    cfg.events_archive_root = arg_parser.getArg<std::filesystem::path>("--events-archive-root").value_or(
        cfg.storage_path / "events" / "archive"
    );
    cfg.events_chain_id = arg_parser.getArg<unsigned int>("--events-chain-id").value_or(1);
    cfg.events_hot_window_days = arg_parser.getArg<unsigned int>("--events-hot-window-days").value_or(90);
    cfg.events_projector_interval_ms = arg_parser.getArg<unsigned int>("--events-projector-ms").value_or(200);
    cfg.events_archive_interval_ms = arg_parser.getArg<unsigned int>("--events-archive-ms").value_or(30000);
    cfg.events_reorg_window_blocks = arg_parser.getArg<unsigned int>("--events-reorg-window-blocks").value_or(2048);
    cfg.events_outbox_retention_days = arg_parser.getArg<unsigned int>("--events-outbox-retention-days").value_or(7);

    spdlog::info("Current working path: {}", std::filesystem::current_path().string());

    // solidity check
    const auto solc_path = cfg.bin_path / TOSTRING(Solidity_SOLC_EXECUTABLE);

    spdlog::info(std::format("Path to solidity solc compiler : {}", solc_path.string()));

    const auto [exit_code, solc_version_out] = dcn::native::runProcess(solc_path.string(), {"--version"});
    spdlog::info(std::format("Solc info:\n{}", solc_version_out));
    
    const auto pt_path = cfg.bin_path.parent_path() / "pt";
    spdlog::info(std::format("Path to PT framework : {}", pt_path.string()));

    const bool registry_db_in_memory = cfg.registry_db.empty() || cfg.registry_db == ":memory:";

    // create directories
    if(!_createDirectories(cfg))
    {
        return 1;
    }

    asio::io_context io_context;

    dcn::registry::Registry registry(io_context, cfg.registry_db.string());

    dcn::auth::AuthManager auth_manager(io_context);

    dcn::evm::EVM evm(io_context, EVMC_SHANGHAI, solc_path, pt_path);

    dcn::server::Server server(io_context, {asio::ip::tcp::v4(), asio::ip::port_type(cfg.port)});

    server.setIdleInterval(5000ms);

    dcn::events::EventRuntime events_runtime(
        io_context,
        dcn::events::EventRuntimeConfig{
            .hot_db_path = cfg.events_db,
            .archive_root = cfg.events_archive_root,
            .chain_id = static_cast<int>(cfg.events_chain_id),
            .ingestion_enabled = cfg.chain_ingestion.enabled,
            .use_local_evm_source = cfg.chain_ingestion.use_local_evm_source,
            .local_evm = cfg.chain_ingestion.use_local_evm_source ? &evm : nullptr,
            .rpc_url = cfg.chain_ingestion.rpc_url,
            .registry_address = cfg.chain_ingestion.registry_address,
            .start_block = cfg.chain_ingestion.start_block.has_value()
                ? std::optional<std::int64_t>(static_cast<std::int64_t>(*cfg.chain_ingestion.start_block))
                : std::nullopt,
            .poll_interval_ms = cfg.chain_ingestion.poll_interval_ms,
            .confirmations = cfg.chain_ingestion.confirmations,
            .block_batch_size = cfg.chain_ingestion.block_batch_size,
            .hot_window_days = static_cast<std::size_t>(cfg.events_hot_window_days),
            .reorg_window_blocks = static_cast<std::size_t>(cfg.events_reorg_window_blocks),
            .outbox_retention_ms = static_cast<std::int64_t>(cfg.events_outbox_retention_days) * 24LL * 60LL * 60LL * 1000LL,
            .projector_interval_ms = cfg.events_projector_interval_ms,
            .archive_interval_ms = cfg.events_archive_interval_ms
        });
    
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
    server.addRoute({dcn::http::Method::OPTIONS, "/accounts?limit=<uint>&after=<~string>"}, dcn::OPTIONS_accounts);
    server.addRoute({dcn::http::Method::HEAD, "/accounts?limit=<uint>&after=<~string>"}, dcn::HEAD_accounts, std::ref(registry));
    server.addRoute({dcn::http::Method::GET, "/accounts?limit=<uint>&after=<~string>"}, dcn::GET_accounts, std::ref(registry));

    server.addRoute({dcn::http::Method::OPTIONS, "/formats?limit=<uint>&after=<~string>"}, dcn::OPTIONS_formats);
    server.addRoute({dcn::http::Method::HEAD, "/formats?limit=<uint>&after=<~string>"}, dcn::HEAD_formats, std::ref(registry));
    server.addRoute({dcn::http::Method::GET, "/formats?limit=<uint>&after=<~string>"}, dcn::GET_formats, std::ref(registry));
    server.addRoute({dcn::http::Method::OPTIONS, "/format/<string>?limit=<uint>&after=<~string>"}, dcn::OPTIONS_format);
    server.addRoute({dcn::http::Method::GET, "/format/<string>?limit=<uint>&after=<~string>"}, dcn::GET_format, std::ref(registry));

    server.addRoute({dcn::http::Method::OPTIONS, "/feed?limit=<uint>&before=<~string>&type=<~string>&include_unfinalized=<~uint>"}, dcn::OPTIONS_feed);
    server.addRoute({dcn::http::Method::GET, "/feed?limit=<uint>&before=<~string>&type=<~string>&include_unfinalized=<~uint>"}, dcn::GET_feed, std::ref(events_runtime));
    server.addRoute({dcn::http::Method::OPTIONS, "/feed/stream?since_seq=<~uint>&limit=<~uint>"}, dcn::OPTIONS_feedStream);
    server.addRoute({dcn::http::Method::GET, "/feed/stream?since_seq=<~uint>&limit=<~uint>"}, dcn::GET_feedStream, std::ref(events_runtime));

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

    if(!dcn::loader::ensurePTBuildVersion(cfg.storage_path))
    {
        spdlog::error("Failed to prepare PT Solidity build cache");
        return 1;
    }

    events_runtime.start();
    spdlog::info(
        "Events runtime started (ingestion_enabled={}, local_source={}, chain_id={}, hot_db='{}')",
        events_runtime.ingestionEnabled(),
        cfg.chain_ingestion.use_local_evm_source,
        cfg.events_chain_id,
        cfg.events_db.string());

    const bool wal_enabled = !registry_db_in_memory;
    std::atomic<bool> wal_sync_worker_stopped = true;
    std::optional<dcn::storage::sqlite::WalSyncWorker> wal_sync_worker;
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
         &events_runtime,
         wal_enabled]() -> asio::awaitable<void>
        {
            return _runGracefulShutdown(
                server,
                wal_sync_worker,
                wal_sync_worker_stopped,
                registry,
                wal_enabled,
                events_runtime);
        },

        // immediate shutdown
        [&wal_sync_worker, &events_runtime]()
        {
            _runImmediateShutdown(wal_sync_worker, events_runtime);
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
