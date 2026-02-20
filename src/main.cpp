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
    
    const auto ico = dcn::file::loadBinaryFile(cfg.resources_path / "media" / "img" / "DCN.ico");
    
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

    if(ico)
    {
        server.addRoute({dcn::http::Method::HEAD, "/favicon.ico"},      dcn::HEAD_serveFile);
        server.addRoute({dcn::http::Method::OPTIONS, "/favicon.ico"},   dcn::OPTIONS_serveFile);
        server.addRoute({dcn::http::Method::GET, "/favicon.ico"},       dcn::GET_serveBinaryFile, "image/x-icon", std::cref(ico.value()));
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
        [&io_context, &registry, &evm, &server, &cfg](std::exception_ptr, std::tuple<bool, bool>)
        {
            // transformation and condition loaded  
            asio::co_spawn(io_context, dcn::loader::loadStoredFeatures(evm, registry, cfg.storage_path), 
                [&io_context, &registry, &evm, &server, &cfg](std::exception_ptr, bool)
                {
                    // features loaded
                    asio::co_spawn(io_context, dcn::loader::loadStoredParticles(evm, registry, cfg.storage_path), 
                [&io_context, &server](std::exception_ptr, bool)
                        {
                            // particles loaded
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
