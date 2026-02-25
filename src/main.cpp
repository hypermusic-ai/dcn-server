#include "decentralised_art.hpp"

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
    arg_parser.addArg("-h", dcn::cmd::CommandLineArgDef::NArgs::Zero, dcn::cmd::CommandLineArgDef::Type::Bool, "Display help message and exit");
    arg_parser.addArg("--help", dcn::cmd::CommandLineArgDef::NArgs::Zero, dcn::cmd::CommandLineArgDef::Type::Bool, "Display help message and exit");
    arg_parser.addArg("--version", dcn::cmd::CommandLineArgDef::NArgs::Zero, dcn::cmd::CommandLineArgDef::Type::Bool, "Display version and exit");
    arg_parser.addArg("--port", dcn::cmd::CommandLineArgDef::NArgs::One, dcn::cmd::CommandLineArgDef::Type::Int, "Port to listen on");
    arg_parser.addArg("--mainnet-rpc", dcn::cmd::CommandLineArgDef::NArgs::One, dcn::cmd::CommandLineArgDef::Type::String, "Ethereum JSON-RPC endpoint URL used for event sync");
    arg_parser.addArg("--mainnet-registry", dcn::cmd::CommandLineArgDef::NArgs::One, dcn::cmd::CommandLineArgDef::Type::String, "PT registry proxy address on mainnet");
    arg_parser.addArg("--mainnet-start-block", dcn::cmd::CommandLineArgDef::NArgs::One, dcn::cmd::CommandLineArgDef::Type::Int, "Optional first block for event sync when no local cursor exists");
    arg_parser.addArg("--mainnet-poll-ms", dcn::cmd::CommandLineArgDef::NArgs::One, dcn::cmd::CommandLineArgDef::Type::Int, "Mainnet poll interval in milliseconds");
    arg_parser.addArg("--mainnet-confirmations", dcn::cmd::CommandLineArgDef::NArgs::One, dcn::cmd::CommandLineArgDef::Type::Int, "Finality confirmation depth");
    arg_parser.addArg("--mainnet-batch-size", dcn::cmd::CommandLineArgDef::NArgs::One, dcn::cmd::CommandLineArgDef::Type::Int, "Max number of blocks fetched per eth_getLogs request");

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

    const asio::ip::port_type port = arg_parser.getArg<std::vector<int>>("--port").value_or(std::vector<int>{dcn::DEFAULT_PORT}).at(0);

    dcn::mainnet::IngestionConfig mainnet_ingestion_cfg;
    mainnet_ingestion_cfg.storage_path = cfg.storage_path;

    const auto mainnet_poll_ms_arg = arg_parser.getArg<std::vector<int>>("--mainnet-poll-ms").value_or(std::vector<int>{5000}).at(0);
    const auto mainnet_confirmations_arg = arg_parser.getArg<std::vector<int>>("--mainnet-confirmations").value_or(std::vector<int>{12}).at(0);
    const auto mainnet_batch_size_arg = arg_parser.getArg<std::vector<int>>("--mainnet-batch-size").value_or(std::vector<int>{500}).at(0);

    if(mainnet_poll_ms_arg <= 0 || mainnet_confirmations_arg < 0 || mainnet_batch_size_arg <= 0)
    {
        spdlog::error("Invalid mainnet sync numeric options");
        return 1;
    }

    mainnet_ingestion_cfg.poll_interval_ms = static_cast<std::uint64_t>(mainnet_poll_ms_arg);
    mainnet_ingestion_cfg.confirmations = static_cast<std::uint64_t>(mainnet_confirmations_arg);
    mainnet_ingestion_cfg.block_batch_size = static_cast<std::uint64_t>(mainnet_batch_size_arg);

    if(const auto mainnet_start_block_arg = arg_parser.getArg<std::vector<int>>("--mainnet-start-block"))
    {
        if(mainnet_start_block_arg->at(0) < 0)
        {
            spdlog::error("Mainnet start block cannot be negative");
            return 1;
        }
        mainnet_ingestion_cfg.start_block = static_cast<std::uint64_t>(mainnet_start_block_arg->at(0));
    }

    const auto mainnet_rpc_arg = arg_parser.getArg<std::vector<std::string>>("--mainnet-rpc");
    const auto mainnet_registry_arg = arg_parser.getArg<std::vector<std::string>>("--mainnet-registry");

    if((mainnet_rpc_arg.has_value() && !mainnet_registry_arg.has_value()) ||
        (!mainnet_rpc_arg.has_value() && mainnet_registry_arg.has_value()))
    {
        spdlog::error("Both --mainnet-rpc and --mainnet-registry must be provided together");
        return 1;
    }

    if(mainnet_rpc_arg && mainnet_registry_arg)
    {
        const auto registry_addr_res = evmc::from_hex<dcn::evm::Address>(mainnet_registry_arg->at(0));
        if(!registry_addr_res)
        {
            spdlog::error("Invalid --mainnet-registry address");
            return 1;
        }

        mainnet_ingestion_cfg.enabled = true;
        mainnet_ingestion_cfg.rpc_url = mainnet_rpc_arg->at(0);
        mainnet_ingestion_cfg.registry_address = *registry_addr_res;

        spdlog::info(
            "Mainnet sync enabled. Registry={}, poll={}ms, confirmations={}, batch={}",
            mainnet_registry_arg->at(0),
            mainnet_ingestion_cfg.poll_interval_ms,
            mainnet_ingestion_cfg.confirmations,
            mainnet_ingestion_cfg.block_batch_size);
    }

    spdlog::info("Current working path: {}", std::filesystem::current_path().string());

    // solidity check
    const auto solc_path = cfg.bin_path / TOSTRING(Solidity_SOLC_EXECUTABLE);

    spdlog::info(std::format("Path to solidity solc compiler : {}", solc_path.string()));

    const auto [exit_code, solc_version_out] = dcn::native::runProcess(solc_path.string(), {"--version"});
    spdlog::info(std::format("Solc info:\n{}", solc_version_out));
    
    const auto pt_path = cfg.bin_path.parent_path() / "pt";
    spdlog::info(std::format("Path to PT framework : {}", pt_path.string()));

    asio::io_context io_context;

    dcn::registry::Registry registry(io_context);

    dcn::auth::AuthManager auth_manager(io_context);

    dcn::evm::EVM evm(io_context, EVMC_SHANGHAI, solc_path, pt_path);

    dcn::server::Server server(io_context, {asio::ip::tcp::v4(), port});

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

    server.addRoute({dcn::http::Method::OPTIONS, "/account/<string>?limit=<uint>&page=<uint>"}, dcn::OPTIONS_accountInfo);
    server.addRoute({dcn::http::Method::GET,    "/account/<string>?limit=<uint>&page=<uint>"},  dcn::GET_accountInfo, std::ref(registry));

    server.addRoute({dcn::http::Method::HEAD, "/particle/<string>/<~string>"},       dcn::HEAD_particle, std::ref(registry));
    server.addRoute({dcn::http::Method::OPTIONS, "/particle/<string>/<~string>"},    dcn::OPTIONS_particle);
    server.addRoute({dcn::http::Method::GET,     "/particle/<string>/<~string>"},    dcn::GET_particle, std::ref(registry), std::ref(evm));
    server.addRoute({dcn::http::Method::POST,    "/particle"},                       dcn::POST_particle, std::ref(auth_manager), std::ref(registry), std::ref(evm), std::cref(cfg));

    server.addRoute({dcn::http::Method::HEAD, "/feature/<string>/<~string>"},       dcn::HEAD_feature, std::ref(registry));
    server.addRoute({dcn::http::Method::OPTIONS, "/feature/<string>/<~string>"},    dcn::OPTIONS_feature);
    server.addRoute({dcn::http::Method::GET,     "/feature/<string>/<~string>"},    dcn::GET_feature, std::ref(registry), std::ref(evm));
    server.addRoute({dcn::http::Method::POST,    "/feature"},                       dcn::POST_feature, std::ref(auth_manager), std::ref(registry), std::ref(evm), std::cref(cfg));

    server.addRoute({dcn::http::Method::HEAD, "/transformation/<string>/<~string>"},    dcn::HEAD_transformation, std::ref(registry));
    server.addRoute({dcn::http::Method::OPTIONS, "/transformation/<string>/<~string>"}, dcn::OPTIONS_transformation);
    server.addRoute({dcn::http::Method::GET,     "/transformation/<string>/<~string>"}, dcn::GET_transformation, std::ref(registry), std::ref(evm));
    server.addRoute({dcn::http::Method::POST,    "/transformation"},                    dcn::POST_transformation, std::ref(auth_manager), std::ref(registry), std::ref(evm), std::cref(cfg));

    server.addRoute({dcn::http::Method::HEAD, "/condition/<string>/<~string>"},    dcn::HEAD_condition, std::ref(registry));
    server.addRoute({dcn::http::Method::OPTIONS, "/condition/<string>/<~string>"}, dcn::OPTIONS_condition);
    server.addRoute({dcn::http::Method::GET,     "/condition/<string>/<~string>"}, dcn::GET_condition, std::ref(registry), std::ref(evm));
    server.addRoute({dcn::http::Method::POST,    "/condition"},                    dcn::POST_condition, std::ref(auth_manager), std::ref(registry), std::ref(evm), std::cref(cfg));

    server.addRoute({dcn::http::Method::OPTIONS, "/execute"},   dcn::OPTIONS_execute);
    server.addRoute({dcn::http::Method::POST, "/execute"},      dcn::POST_execute, std::cref(auth_manager), std::ref(evm));

    // create directories
    std::filesystem::create_directory(cfg.storage_path);
    std::filesystem::create_directory(cfg.storage_path / "particles");
    std::filesystem::create_directory(cfg.storage_path / "particles" / "build");
    std::filesystem::create_directory(cfg.storage_path / "features");
    std::filesystem::create_directory(cfg.storage_path / "features" / "build");
    std::filesystem::create_directory(cfg.storage_path / "transformations");
    std::filesystem::create_directory(cfg.storage_path / "transformations" / "build");
    std::filesystem::create_directory(cfg.storage_path / "conditions");
    std::filesystem::create_directory(cfg.storage_path / "conditions" / "build");

    if(!dcn::loader::ensurePTBuildVersion(cfg.storage_path))
    {
        spdlog::error("Failed to prepare PT Solidity build cache");
        return 1;
    }

    asio::co_spawn(io_context, 
        (dcn::loader::loadStoredTransformations(evm, registry, cfg.storage_path)  &&
        dcn::loader::loadStoredConditions(evm, registry, cfg.storage_path)), 
        [&io_context, &registry, &evm, &server, &cfg, &mainnet_ingestion_cfg](std::exception_ptr, std::tuple<bool, bool>)
        {
            // transformation and condition loaded  
            asio::co_spawn(io_context, dcn::loader::loadStoredFeatures(evm, registry, cfg.storage_path), 
                [&io_context, &registry, &evm, &server, &cfg, &mainnet_ingestion_cfg](std::exception_ptr, bool)
                {
                    // features loaded
                    asio::co_spawn(io_context, dcn::loader::loadStoredParticles(evm, registry, cfg.storage_path), 
                [&io_context, &server, &registry, &mainnet_ingestion_cfg](std::exception_ptr, bool)
                        {
                            // particles loaded
                            if(mainnet_ingestion_cfg.enabled)
                            {
                                asio::co_spawn(io_context, dcn::mainnet::runEventIngestion(mainnet_ingestion_cfg, registry), asio::detached);
                            }
                            asio::co_spawn(io_context, server.listen(), asio::detached);
                        }
                    );
                }
            );
        }
    );

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
