#include "decentralised_art.hpp"

#ifndef Solidity_SOLC_EXECUTABLE
    #error "Solidity_SOLC_EXECUTABLE is not defined"
#endif

static void _configureLogger()
{
    std::filesystem::create_directories(dcn::getLogsPath());

    // Create sinks
    auto console_sink = std::make_shared<spdlog::sinks::stdout_color_sink_mt>();
    const std::string log_name = dcn::utils::currentTimestamp() + "-DCNServer.log";
    auto file_sink = std::make_shared<spdlog::sinks::basic_file_sink_mt>(
        (dcn::getLogsPath() / log_name).string(), true);

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
    dcn::setBinPath(std::filesystem::path(argv[0]).parent_path());
    
    _configureLogger();

    spdlog::info("{}", dcn::utils::getAsciiLogo());

    const std::string build_timestamp = dcn::utils::loadBuildTimestamp(dcn::getBinPath() / "build_timestamp");
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
    auto solc_path = std::filesystem::path(
        dcn::getBinPath() / TOSTRING(Solidity_SOLC_EXECUTABLE));

    spdlog::info(std::format("Path to solidity solc compiler : {}", solc_path.string()));

    const auto [exit_code, solc_version_out] = dcn::native::runProcess(solc_path.string(), {"--version"});
    spdlog::info(std::format("Solc info:\n{}", solc_version_out));
    
    asio::io_context io_context;

    dcn::Registry registry(io_context);

    dcn::AuthManager auth_manager(io_context);

    dcn::EVM evm(io_context, EVMC_SHANGHAI, solc_path);

    dcn::Server server(io_context, {asio::ip::tcp::v4(), port});

    server.setIdleInterval(5000ms);
    
    const auto ico = dcn::loadBinaryFile(std::filesystem::path{"media"} / "img" / "DCN.ico");

    const auto simple_form = dcn::loadTextFile(std::filesystem::path{"html"} / "simple_form.html");
    const auto simple_form_js = dcn::loadTextFile(std::filesystem::path{"js"} / "simple_form.js");
    const auto simple_form_css = dcn::loadTextFile(std::filesystem::path{"styles"} / "simple_form.css");

    if(simple_form && simple_form_js && simple_form_css)
    {
        server.addRoute({dcn::http::Method::HEAD, "/"},     dcn::HEAD_ServeFile);
        server.addRoute({dcn::http::Method::OPTIONS, "/"},  dcn::OPTIONS_ServeFile);
        server.addRoute({dcn::http::Method::GET, "/"},      dcn::GET_ServeFile, "text/html; charset=utf-8", std::cref(simple_form.value()));

        server.addRoute({dcn::http::Method::HEAD, "/js/simple_form.js"},    dcn::HEAD_ServeFile);
        server.addRoute({dcn::http::Method::OPTIONS, "/js/simple_form.js"}, dcn::OPTIONS_ServeFile);
        server.addRoute({dcn::http::Method::GET, "/js/simple_form.js"},     dcn::GET_ServeFile, "text/javascript; charset=utf-8", std::cref(simple_form_js.value()));
    
        server.addRoute({dcn::http::Method::HEAD, "/styles/simple_form.css"},       dcn::HEAD_ServeFile);
        server.addRoute({dcn::http::Method::OPTIONS, "/styles/simple_form.css"},    dcn::OPTIONS_ServeFile);
        server.addRoute({dcn::http::Method::GET, "/styles/simple_form.css"},        dcn::GET_ServeFile, "text/css; charset=utf-8", std::cref(simple_form_css.value()));
    }

    if(ico)
    {
        server.addRoute({dcn::http::Method::HEAD, "/favicon.ico"},      dcn::HEAD_ServeFile);
        server.addRoute({dcn::http::Method::OPTIONS, "/favicon.ico"},   dcn::OPTIONS_ServeFile);
        server.addRoute({dcn::http::Method::GET, "/favicon.ico"},       dcn::GET_ServeBinaryFile, "image/x-icon", std::cref(ico.value()));
    }
    
    server.addRoute({dcn::http::Method::GET, "/version"},   dcn::GET_version, std::cref(build_timestamp));

    server.addRoute({dcn::http::Method::GET, "/nonce/<string>"},    dcn::GET_nonce, std::ref(auth_manager));

    server.addRoute({dcn::http::Method::OPTIONS, "/auth"},  dcn::OPTIONS_auth);
    server.addRoute({dcn::http::Method::POST, "/auth"},     dcn::POST_auth, std::ref(auth_manager));

    server.addRoute({dcn::http::Method::OPTIONS, "/refresh"},   dcn::OPTIONS_refresh);
    server.addRoute({dcn::http::Method::POST, "/refresh"},      dcn::POST_refresh, std::ref(auth_manager));

    server.addRoute({dcn::http::Method::OPTIONS, "/account/<string>?limit=<uint>&page=<uint>"}, dcn::OPTIONS_accountInfo);
    server.addRoute({dcn::http::Method::GET,    "/account/<string>?limit=<uint>&page=<uint>"},  dcn::GET_accountInfo, std::ref(registry));

    server.addRoute({dcn::http::Method::HEAD, "/feature/<string>/<~string>"},       dcn::HEAD_feature, std::ref(registry));
    server.addRoute({dcn::http::Method::OPTIONS, "/feature/<string>/<~string>"},    dcn::OPTIONS_feature);
    server.addRoute({dcn::http::Method::GET,     "/feature/<string>/<~string>"},    dcn::GET_feature, std::ref(registry), std::ref(evm));
    server.addRoute({dcn::http::Method::POST,    "/feature"},                       dcn::POST_feature, std::ref(auth_manager), std::ref(registry), std::ref(evm));


    server.addRoute({dcn::http::Method::HEAD, "/transformation/<string>/<~string>"},    dcn::HEAD_transformation, std::ref(registry));
    server.addRoute({dcn::http::Method::OPTIONS, "/transformation/<string>/<~string>"}, dcn::OPTIONS_transformation);
    server.addRoute({dcn::http::Method::GET,     "/transformation/<string>/<~string>"}, dcn::GET_transformation, std::ref(registry), std::ref(evm));
    server.addRoute({dcn::http::Method::POST,    "/transformation"},                    dcn::POST_transformation, std::ref(auth_manager), std::ref(registry), std::ref(evm));

    //server.addRoute({dcn::http::Method::GET, "/condition"},                          dcn::GET_condition);
    //server.addRoute({dcn::http::Method::POST, "/condition"},                         dcn::POST_condition);
    server.addRoute({dcn::http::Method::OPTIONS, "/execute/<string>/<uint>/<~[<(<uint>;<uint>)>]>"},    dcn::OPTIONS_execute);
    server.addRoute({dcn::http::Method::GET, "/execute/<string>/<uint>/<~[<(<uint>;<uint>)>]>"},        dcn::GET_execute, std::cref(auth_manager), std::cref(registry), std::ref(evm));

    // create directories
    std::filesystem::create_directory(dcn::getStoragePath());
    std::filesystem::create_directory(dcn::getStoragePath() / "features");
    std::filesystem::create_directory(dcn::getStoragePath() / "features" / "build");
    std::filesystem::create_directory(dcn::getStoragePath() / "transformations");
    std::filesystem::create_directory(dcn::getStoragePath() / "transformations" / "build");

    asio::co_spawn(io_context, dcn::loadStoredTransformations(evm, registry), 
        [&io_context, &registry, &evm, &server](std::exception_ptr, bool){
                asio::co_spawn(io_context, dcn::loadStoredFeatures(evm, registry), 
                [&io_context, &server](std::exception_ptr, bool){
                    asio::co_spawn(io_context, server.listen(), asio::detached);
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