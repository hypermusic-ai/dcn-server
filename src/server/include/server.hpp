#pragma once

#include <chrono>
using namespace std::chrono_literals;

#include "native.h"
#include <asio.hpp>
#include <asio/experimental/awaitable_operators.hpp>
using namespace asio::experimental::awaitable_operators;

#include <spdlog/spdlog.h>

#include "utils.hpp"
#include "http.hpp"
#include "route.hpp"

namespace dcn
{
    /**
     * @brief Type representing a route handler function.
     */
    using HandlerDefinition = std::function<asio::awaitable<dcn::http::Response>(const dcn::http::Request &, std::vector<RouteArg>, QueryArgsList)>;

    // TODO: Add a concept to check if the handler is callable with the expected arguments
    // template<class F, class ... Args>
    // concept HandlerCallable = requires(F handler, Args... args)
    // {
    //     { t(args...) } -> std::same_as<asio::awaitable<dcn::http::Response>>;
    // };

    /**
     * @brief A class representing a server for handling HTTP requests.
     * 
     * This class is responsible for listening for incoming TCP connections on a specified port, 
     * handling incoming HTTP requests, and executing route handlers based on the request method and path.
     */
    class Server
    {
        public:
        	Server(asio::io_context & io_context, asio::ip::tcp::endpoint endpoint);
            ~Server() = default;

            /**
             * Listens for incoming TCP connections on the specified port.
             *
             * This function sets up the server to listen for connections using the 
             * provided acceptor. It dispatches the listening operation to the server's 
             * strand to ensure thread safety. The server continues to listen until the 
             * `close` flag is set to true. For each incoming connection, it spawns a 
             * new coroutine to handle the connection without blocking the server strand.
             * 
             * The function also manages an idle timeout using a watchdog, which ensures 
             * that the server does not remain idle beyond the specified `idle_interval`.
             */
          	asio::awaitable<void> listen();

            /**
             * @brief Adds a route to the server with a specified HTTP method and path.
             * @param route The route key containing the HTTP method and path.
             * @param handler The handler function to be called when the route is matched.
             */
            template<class F>
            void addRoute(RouteKey route, F && handler)
            {
                _router.addRoute(std::move(route), RouteHandlerFunc(
                    HandlerDefinition(std::move(handler)))
                );
            }

            /**
             * @brief Adds a route to the server with a specified HTTP method and path.
             * @param route The route key containing the HTTP method and path.
             * @param handler The handler function to be called when the route is matched.
             * @param binded_args Additional arguments to bind to the handler function.
             */
            template<class F, class ... Args>
            void addRoute(RouteKey route, F && handler, Args&&... binded_args)
            {
                _router.addRoute(std::move(route), RouteHandlerFunc(
                    HandlerDefinition(
                        std::bind(handler, _1, _2, _3, std::forward<Args>(binded_args)...)
                    )
                ));
            }
            
            /**
             * @brief Set the idle interval after which the server will close the connection.
             * @param idle_interval The idle interval in milliseconds.
             * If no requests are received from the client during this interval, the server will close the connection.
             */
            void setIdleInterval(std::chrono::milliseconds idle_interval);

            /**
             * @brief Closes the server gracefully.
             * 
             * This function sets the `_close` flag to true, signaling the server to stop
             * accepting new connections and begin shutdown procedures. The closure
             * operation is dispatched to the server's strand to ensure thread safety.
             * It logs a debug message indicating that a close request has been initiated.
             */
            asio::awaitable<void> close();

        protected:

            /**
             * Handles a new connection by reading data from the socket and processing it
             * until there is no more data to read or the idle interval has been reached.
             * @param sock The socket to read from.
             * @return An awaitable that resolves when the connection has been closed.
             */
            asio::awaitable<void> handleConnection(asio::ip::tcp::socket sock);

            /**
             * @brief Reads data from the given socket and processes incoming HTTP requests.
             * 
             * This function asynchronously reads data from the provided TCP socket until the connection is closed or 
             * an error occurs. It parses the incoming data into HTTP requests, finds the appropriate route handler 
             * and executes it. The resulting HTTP response is then sent back through the socket. The deadline is 
             * updated for each read operation to enforce a timeout for client inactivity.
             * 
             * @param sock The TCP socket to read data from.
             * @param deadline The time point by which the read operation should complete.
             */
            asio::awaitable<void> readData(asio::ip::tcp::socket & sock, std::chrono::steady_clock::time_point & deadline);

            /**
             * @brief Asynchronously writes data to a TCP socket.
             * 
             * This function sends the provided message over the specified socket using 
             * asynchronous write operations. The function yields control until the entire 
             * message is sent. It's designed to work within an awaitable context and is 
             * part of the server's coroutine-based handling of connections.
             * 
             * @param sock The TCP socket to write the message to.
             * @param message The message to be sent over the socket.
             */
            asio::awaitable<void> writeData(asio::ip::tcp::socket & sock, std::string message);

        private:
            asio::io_context & _io_context;
            asio::strand<asio::io_context::executor_type> _strand;
            bool _close;

            asio::ip::tcp::acceptor _acceptor;
            Router _router;

            std::chrono::milliseconds _idle_interval;
    };
}