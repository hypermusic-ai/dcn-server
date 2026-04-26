#pragma once
#include <ranges>

#include <spdlog/spdlog.h>

#include "async.hpp"
#include "utils.hpp"
#include "config.hpp"
#include "route.hpp"
#include "parser.hpp"
#include "pt.hpp"
#include "auth.hpp"
#include "chain.hpp"
#include "evm.hpp"
#include "version.hpp"
#include "loader.hpp"
#include "events.hpp"

namespace dcn
{
    /**
     * @brief Helper function to handle authentication.
     */
    asio::awaitable<std::expected<chain::Address, auth::AuthError>> authenticate(const http::Request & request, const auth::AuthManager & auth_manager);


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
     * @brief Handles a OPTIONS request to /formats?limit=<uint>&after=<~string>
     *
     * @param request The incoming HTTP request
     * @param route_args Route arguments
     * @param query_args Query arguments
     * @return An HTTP response
     */
    asio::awaitable<http::Response> OPTIONS_formats(const http::Request & request, std::vector<server::RouteArg> route_args, server::QueryArgsList query_args);

    /**
     * @brief Handles a HEAD request to /formats?limit=<uint>&after=<~string>
     *
     * Mirrors GET /formats validation and returns status-only metadata.
     *
     * @param request The incoming HTTP request
     * @param route_args Route arguments
     * @param query_args Query arguments
     * @param registry Registry instance
     * @return An HTTP response
     */
    asio::awaitable<http::Response> HEAD_formats(const http::Request & request, std::vector<server::RouteArg> route_args, server::QueryArgsList query_args, registry::Registry & registry);

    /**
     * @brief Handles a GET request to /formats?limit=<uint>&after=<~string>
     *
     * Returns a paginated list of all format hashes.
     *
     * @param request The incoming HTTP request
     * @param route_args Route arguments
     * @param query_args Query arguments
     * @param registry Registry instance
     * @return An HTTP response
     */
    asio::awaitable<http::Response> GET_formats(const http::Request & request, std::vector<server::RouteArg> route_args, server::QueryArgsList query_args, registry::Registry & registry);

    /**
     * @brief Handles a OPTIONS request to /format/<hash>?limit=<uint>&after=<~string>
     *
     * @param request The incoming HTTP request
     * @param route_args Route arguments
     * @param query_args Query arguments
     * @return An HTTP response
     */
    asio::awaitable<http::Response> OPTIONS_format(const http::Request & request, std::vector<server::RouteArg> route_args, server::QueryArgsList query_args);

    /**
     * @brief Handles a GET request to /format/<hash>?limit=<uint>&after=<~string>
     *
     * Returns a paginated list of connectors that belong to the given format hash.
     *
     * @param request The incoming HTTP request
     * @param route_args Route arguments
     * @param query_args Query arguments
     * @param registry Registry instance
     * @return An HTTP response
     */
    asio::awaitable<http::Response> GET_format(const http::Request & request, std::vector<server::RouteArg> route_args, server::QueryArgsList query_args, registry::Registry & registry);

    /**
     * @brief Handles a OPTIONS request to /accounts?limit=<uint>&after=<~string>
     *
     * @param request The incoming HTTP request
     * @param route_args Route arguments
     * @param query_args Query arguments
     * @return An HTTP response
     */
    asio::awaitable<http::Response> OPTIONS_accounts(const http::Request & request, std::vector<server::RouteArg> route_args, server::QueryArgsList query_args);

    /**
     * @brief Handles a HEAD request to /accounts?limit=<uint>&after=<~string>
     *
     * Mirrors GET /accounts validation and returns status-only metadata.
     *
     * @param request The incoming HTTP request
     * @param route_args Route arguments
     * @param query_args Query arguments
     * @param registry Registry instance
     * @return An HTTP response
     */
    asio::awaitable<http::Response> HEAD_accounts(const http::Request & request, std::vector<server::RouteArg> route_args, server::QueryArgsList query_args, registry::Registry & registry);

    /**
     * @brief Handles a GET request to /accounts?limit=<uint>&after=<~string>
     *
     * Returns a paginated list of all unique account owners.
     *
     * @param request The incoming HTTP request
     * @param route_args Route arguments
     * @param query_args Query arguments
     * @param registry Registry instance
     * @return An HTTP response
     */
    asio::awaitable<http::Response> GET_accounts(const http::Request & request, std::vector<server::RouteArg> route_args, server::QueryArgsList query_args, registry::Registry & registry);

    /**
     * @brief Handles OPTIONS requests to /feed?limit=<uint>&before=<~string>&type=<~string>&include_unfinalized=<~uint>
     */
    asio::awaitable<http::Response> OPTIONS_feed(const http::Request & request, std::vector<server::RouteArg> route_args, server::QueryArgsList query_args);

    /**
     * @brief Handles GET requests to /feed?limit=<uint>&before=<~string>&type=<~string>&include_unfinalized=<~uint>
     */
    asio::awaitable<http::Response> GET_feed(
        const http::Request & request,
        std::vector<server::RouteArg> route_args,
        server::QueryArgsList query_args,
        events::EventRuntime & events_runtime);

    /**
     * @brief Handles OPTIONS requests to /feed/stream?since_seq=<~uint>&limit=<~uint>
     */
    asio::awaitable<http::Response> OPTIONS_feedStream(const http::Request & request, std::vector<server::RouteArg> route_args, server::QueryArgsList query_args);

    /**
     * @brief Builds the SSE replay body for /feed/stream.
     *
     * Returns the full SSE-framed string for the initial replay: a leading
     * `min_available_seq` comment, one `data:` frame per delta, and a trailing
     * `stream_meta` frame describing pagination state. Used both by the
     * streaming handler (initial flush after headers) and by the unit tests
     * (which want a deterministic snapshot they can parse offline).
     */
    std::string buildFeedStreamSseReplay(
        events::EventRuntime & events_runtime,
        const events::StreamQuery & query);

    /**
     * @brief Handles GET requests to /feed/stream?since_seq=<~uint>&limit=<~uint>
     *
     * Streaming handler — owns the socket for the request lifetime. Writes the
     * SSE response head, flushes an initial replay via buildFeedStreamSseReplay,
     * then polls EventRuntime for new deltas and emits SSE frames until the
     * client disconnects, sending periodic `:keepalive` comments to keep the
     * connection (and the per-connection watchdog) alive when there are no new
     * events.
     */
    asio::awaitable<void> GET_feedStream(
        asio::ip::tcp::socket & sock,
        const http::Request & request,
        std::vector<server::RouteArg> route_args,
        server::QueryArgsList query_args,
        std::chrono::steady_clock::time_point & deadline,
        events::EventRuntime & events_runtime);


    /**
     * @brief Handles HEAD requests for the connector endpoint.
     *
     * @param request The incoming HTTP request
     * @param route_args Route arguments
     * @param query_args Query arguments
     * @param registry Registry instance for retrieving connectors
     * @return An HTTP response
    */
    asio::awaitable<http::Response> HEAD_connector(const http::Request & request, std::vector<server::RouteArg> route_args, server::QueryArgsList query_args, registry::Registry & registry);
    
    /**
     * @brief Handles OPTIONS requests by returning a response with CORS headers.
     *
     * @param request The incoming HTTP request
     * @param route_args Route arguments
     * @param query_args Query arguments
     * @return An HTTP response with CORS headers
     */
    asio::awaitable<http::Response> OPTIONS_connector(const http::Request & request, std::vector<server::RouteArg> route_args, server::QueryArgsList query_args);

    /**
     * @brief Handle a GET request to /connectors
     * 
     * @param request The incoming HTTP request
     * @param route_args Route arguments
     * @param query_args Query arguments
     * @param registry Registry instance for retrieving connectors
     * @return An HTTP response
     */
    asio::awaitable<http::Response> GET_connector(
        const http::Request & request,
        std::vector<server::RouteArg> route_args,
        server::QueryArgsList query_args,
        registry::Registry & registry);

    /**
     * @brief Handle a POST request to /connectors
     *
     * @param request The incoming HTTP request
     * @param route_args Route arguments
     * @param query_args Query arguments
     * @param registry Registry instance for retrieving connectors
     * @return An HTTP response
     */
    asio::awaitable<http::Response> POST_connector(const http::Request & request, std::vector<server::RouteArg> route_args, server::QueryArgsList query_args,
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
    asio::awaitable<http::Response> GET_transformation(
        const http::Request & request,
        std::vector<server::RouteArg> route_args,
        server::QueryArgsList query_args,
        registry::Registry & registry);

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
    asio::awaitable<http::Response> GET_condition(
        const http::Request & request,
        std::vector<server::RouteArg> route_args,
        server::QueryArgsList query_args,
        registry::Registry & registry);

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
    asio::awaitable<http::Response> POST_execute(
        const http::Request & request,
        std::vector<server::RouteArg> route_args,
        server::QueryArgsList query_args,
        const auth::AuthManager & auth_manager,
        registry::Registry & registry,
        evm::EVM & evm,
        const config::Config & config);
}
