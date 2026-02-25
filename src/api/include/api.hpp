#pragma once
#include <ranges>

#include "native.h"
#include <asio.hpp>
#include <asio/experimental/awaitable_operators.hpp>
using namespace asio::experimental::awaitable_operators;

#include <spdlog/spdlog.h>

#include "utils.hpp"
#include "config.hpp"
#include "route.hpp"
#include "parser.hpp"
#include "pt.hpp"
#include "auth.hpp"
#include "evm.hpp"
#include "file.hpp"
#include "version.hpp"
#include "loader.hpp"

namespace dcn
{
    /**
     * @brief Helper function to handle authentication.
     */
    asio::awaitable<std::expected<evm::Address, auth::AuthError>> authenticate(const http::Request & request, const auth::AuthManager & auth_manager);


    /**
     * @brief Handles GET requests for the version endpoint.
     * 
     * @param request The incoming HTTP request
     * @param route_args Route arguments
     * @param query_args Query arguments
     * @param build_timestamp Build timestamp
     * @return An HTTP response
     */
    asio::awaitable<http::Response> GET_version(const http::Request & request, std::vector<server::RouteArg> route_args, server::QueryArgsList query_args, const std::string & build_timestamp);


    /**
     * @brief Handles HEAD requests for a file.
     * 
     * @param request The incoming HTTP request
     * @param route_args Route arguments
     * @param query_args Query arguments
     * @return An HTTP response
     */
    asio::awaitable<http::Response> HEAD_serveFile(const http::Request & request, std::vector<server::RouteArg> route_args, server::QueryArgsList query_args);

    /**
     * @brief Handles OPTIONS requests for a file by returning a response with CORS headers.
     * 
     * @param request The incoming HTTP request
     * @param route_args Route arguments
     * @param query_args Query arguments
     * @return An HTTP response
     */
    asio::awaitable<http::Response> OPTIONS_serveFile(const http::Request & request, std::vector<server::RouteArg> route_args, server::QueryArgsList query_args);

    /**
     * @brief Handles GET requests for a file.
     * 
     * @param request The incoming HTTP request
     * @param route_args Route arguments
     * @param query_args Query arguments
     * @param mime_type MIME type of the file
     * @param file_content Content of the file
     * @return An HTTP response
     */
    asio::awaitable<http::Response> GET_serveFile(const http::Request & request, std::vector<server::RouteArg> route_args, server::QueryArgsList query_args, const std::string mime_type, const std::string & file_content);

    /**
     * @brief Handles GET requests for a binary file.
     * 
     * @param request The incoming HTTP request
     * @param route_args Route arguments
     * @param query_args Query arguments
     * @param mime_type MIME type of the file
     * @param file_content Content of the file
     * @return An HTTP response
     */
    asio::awaitable<http::Response> GET_serveBinaryFile(const http::Request & request, std::vector<server::RouteArg> route_args, server::QueryArgsList query_args, const std::string mime_type, const std::vector<std::byte> & file_content);

    /**
     * @brief Handle a GET request to /auth/nonce
     * 
     * @param request The incoming HTTP request
     * @param route_args Route arguments
     * @param query_args Query arguments
     * @param auth_manager Auth manager instance
     * @return An HTTP response
     */
    asio::awaitable<http::Response> GET_nonce(const http::Request & request, std::vector<server::RouteArg> route_args, server::QueryArgsList query_args, auth::AuthManager & auth_manager);


    /**
     * @brief Handles a OPTIONS request to /auth
     * 
     * @param request The incoming HTTP request
     * @param route_args Route arguments
     * @param query_args Query arguments
     * @return An HTTP response
     */
    asio::awaitable<http::Response> OPTIONS_auth(const http::Request & request, std::vector<server::RouteArg> route_args, server::QueryArgsList query_args);
    
    /**
     * @brief Handles a POST request to /auth
     * 
     * @param request The incoming HTTP request
     * @param route_args Route arguments
     * @param query_args Query arguments
     * @param auth_manager Auth manager instance
     * @return An HTTP response
     */
    asio::awaitable<http::Response> POST_auth(const http::Request & request, std::vector<server::RouteArg> route_args, server::QueryArgsList query_args, auth::AuthManager & auth_manager);
    

    /**
     * @brief Handles a OPTIONS request to /account
     * 
     * @param request The incoming HTTP request
     * @param route_args Route arguments
     * @param query_args Query arguments
     * @return An HTTP response
     */
    asio::awaitable<http::Response> OPTIONS_accountInfo(const http::Request & request, std::vector<server::RouteArg> route_args, server::QueryArgsList query_args);

    /**
     * @brief Handles a GET request to /account
     * 
     * @param request The incoming HTTP request
     * @param route_args Route arguments
     * @param query_args Query arguments
     * @param registry Registry instance
     * @return An HTTP response
     */
    asio::awaitable<http::Response> GET_accountInfo(const http::Request & request, std::vector<server::RouteArg> route_args, server::QueryArgsList query_args, registry::Registry & registry);


    /**
     * @brief Handles HEAD requests for the particle endpoint.
     *
     * @param request The incoming HTTP request
     * @param route_args Route arguments
     * @param query_args Query arguments
     * @param registry Registry instance for retrieving particles
     * @return An HTTP response
    */
    asio::awaitable<http::Response> HEAD_particle(const http::Request & request, std::vector<server::RouteArg> route_args, server::QueryArgsList query_args, registry::Registry & registry);
    
    /**
     * @brief Handles OPTIONS requests by returning a response with CORS headers.
     *
     * @param request The incoming HTTP request
     * @param route_args Route arguments
     * @param query_args Query arguments
     * @return An HTTP response with CORS headers
     */
    asio::awaitable<http::Response> OPTIONS_particle(const http::Request & request, std::vector<server::RouteArg> route_args, server::QueryArgsList query_args);

    /**
     * @brief Handle a GET request to /particles
     * 
     * @param request The incoming HTTP request
     * @param route_args Route arguments
     * @param query_args Query arguments
     * @param registry Registry instance for retrieving particles
     * @return An HTTP response
     */
    asio::awaitable<http::Response> GET_particle(const http::Request & request, std::vector<server::RouteArg> route_args, server::QueryArgsList query_args, registry::Registry & registry, evm::EVM & evm);

    /**
     * @brief Handle a POST request to /particles
     *
     * @param request The incoming HTTP request
     * @param route_args Route arguments
     * @param query_args Query arguments
     * @param registry Registry instance for retrieving particles
     * @return An HTTP response
     */
    asio::awaitable<http::Response> POST_particle(const http::Request & request, std::vector<server::RouteArg> route_args, server::QueryArgsList query_args,
        auth::AuthManager & auth_manager, registry::Registry & registry, evm::EVM & evm, const config::Config & config);



    /**
     * @brief Handles HEAD requests for the feature endpoint.
     *
     * @param request The incoming HTTP request
     * @param route_args Route arguments
     * @param query_args Query arguments
     * @param registry Registry instance for retrieving features
     * @return An HTTP response
    */
    asio::awaitable<http::Response> HEAD_feature(const http::Request & request, std::vector<server::RouteArg> route_args, server::QueryArgsList query_args, registry::Registry & registry);
    
    /**
     * @brief Handles OPTIONS requests by returning a response with CORS headers.
     *
     * @param request The incoming HTTP request
     * @param route_args Route arguments
     * @param query_args Query arguments
     * @return An HTTP response with CORS headers
     */
    asio::awaitable<http::Response> OPTIONS_feature(const http::Request & request, std::vector<server::RouteArg> route_args, server::QueryArgsList query_args);

    /**
     * @brief Handle a GET request to /features
     * 
     * @param request The incoming HTTP request
     * @param route_args Route arguments
     * @param query_args Query arguments
     * @param registry Registry instance for retrieving features
     * @return An HTTP response
     */
    asio::awaitable<http::Response> GET_feature(const http::Request & request, std::vector<server::RouteArg> route_args, server::QueryArgsList query_args, registry::Registry & registry, evm::EVM & evm);

    /**
     * @brief Handle a POST request to /features
     *
     * @param request The incoming HTTP request
     * @param route_args Route arguments
     * @param query_args Query arguments
     * @param registry Registry instance for retrieving features
     * @return An HTTP response
     */
    asio::awaitable<http::Response> POST_feature(const http::Request & request, std::vector<server::RouteArg> route_args, server::QueryArgsList query_args,
        auth::AuthManager & auth_manager, registry::Registry & registry, evm::EVM & evm, const config::Config & config);


    /**
     * @brief Handles HEAD requests for the transformation endpoint.
     *
     * @param request The incoming HTTP request
     * @param route_args Route arguments
     * @param query_args Query arguments
     * @param registry Registry instance for retrieving transformations
     * @return An HTTP response
     */
    asio::awaitable<http::Response> HEAD_transformation(const http::Request & request, std::vector<server::RouteArg> route_args, server::QueryArgsList query_args, registry::Registry & registry);

    /**
     * @brief Handles OPTIONS requests for the transformation endpoint by returning a response with CORS headers.
     *
     * @param request The incoming HTTP request
     * @param route_args Route arguments
     * @param query_args Query arguments
     * @return An HTTP response with CORS headers for the transformation endpoint
     */
    asio::awaitable<http::Response> OPTIONS_transformation(const http::Request & request, std::vector<server::RouteArg> route_args, server::QueryArgsList query_args);

    /**
     * @brief Handles GET requests for the transformation endpoint.
     *
     * Retrieves a transformation
     *
     * @param request The incoming HTTP request
     * @param route_args Route arguments
     * @param query_args Query arguments
     * @param registry Registry instance for retrieving transformations
     * @param evm EVM instance
     * @return An HTTP response
     */
    asio::awaitable<http::Response> GET_transformation(const http::Request & request, std::vector<server::RouteArg> route_args, server::QueryArgsList query_args, registry::Registry & registry, evm::EVM & evm);

    /**
     * @brief Handles POST requests for the transformation endpoint.
     *
     * Verifies the access token, then adds a new transformation to the registry.
     *
     * @param request The incoming HTTP request
     * @param route_args Route arguments
     * @param query_args Query arguments
     * @param auth_manager Authentication manager instance for verifying access tokens
     * @param registry Registry instance for adding transformations
     * @param evm EVM instance
     * @return An HTTP response
     */
    asio::awaitable<http::Response> POST_transformation(const http::Request & request, std::vector<server::RouteArg> route_args, server::QueryArgsList query_args,
        auth::AuthManager & auth_manager, registry::Registry & registry, evm::EVM & evm, const config::Config & config);


    /**
     * @brief Handles HEAD requests for the condition endpoint.
     *
     * @param request The incoming HTTP request
     * @param route_args Route arguments
     * @param query_args Query arguments
     * @param registry Registry instance for retrieving conditions
     * @return An HTTP response
     */
    asio::awaitable<http::Response> HEAD_condition(const http::Request & request,  std::vector<server::RouteArg> route_args, server::QueryArgsList query_args, registry::Registry & registry);

    /**
     * @brief Handles OPTIONS requests for the condition endpoint by returning a response with CORS headers.
     *
     * @param request The incoming HTTP request
     * @param route_args Route arguments
     * @param query_args Query arguments
     * @return An HTTP response
     */
    asio::awaitable<http::Response> OPTIONS_condition(const http::Request & request, std::vector<server::RouteArg> route_args, server::QueryArgsList query_args);

    /**
     * @brief Handles GET requests for the condition endpoint.
     *
     * Retrieves a condition
     *
     * @param request The incoming HTTP request
     * @param route_args Route arguments
     * @param query_args Query arguments
     * @param registry Registry instance for retrieving conditions
     * @param evm EVM instance
     * @return An HTTP response
     */
    asio::awaitable<http::Response> GET_condition(const http::Request & request, std::vector<server::RouteArg> route_args, server::QueryArgsList query_args, registry::Registry & registry, evm::EVM & evm);

    /**
     * @brief Handles POST requests for the condition endpoint.
     *
     * Verifies the access token, then adds a new condition to the registry.
     *
     * @param request The incoming HTTP request
     * @param route_args Route arguments
     * @param query_args Query arguments
     * @param auth_manager Authentication manager instance for verifying access tokens
     * @param registry Registry instance for adding conditions
     * @param evm EVM instance
     * @return An HTTP response
     */
    asio::awaitable<http::Response> POST_condition(const http::Request & request, std::vector<server::RouteArg> route_args, server::QueryArgsList query_args, 
        auth::AuthManager & auth_manager, registry::Registry & registry, evm::EVM & evm, const config::Config & config);

    /**
     * @brief Handles OPTIONS requests for the execute endpoint by returning a response with CORS headers.
     *
     * @param request The incoming HTTP request
     * @param route_args Route arguments
     * @param query_args Query arguments
     * @return An HTTP response
     */
    asio::awaitable<http::Response> OPTIONS_execute(const http::Request & request, std::vector<server::RouteArg> route_args, server::QueryArgsList query_args);

    /**
     * @brief Handles POST requests for the execute endpoint.
     *
     * Verifies the access token, then executes a runner transaction.
     *
     * @param request The incoming HTTP request
     * @param route_args Route arguments
     * @param query_args Query arguments
     * @param auth_manager Authentication manager instance for verifying access tokens
     * @param evm EVM instance
     * @return An HTTP response
     */
    asio::awaitable<http::Response> POST_execute(const http::Request & request, std::vector<server::RouteArg> route_args, server::QueryArgsList query_args, const auth::AuthManager & auth_manager, evm::EVM & evm);
}