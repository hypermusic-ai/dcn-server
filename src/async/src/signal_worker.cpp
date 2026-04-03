#include "signal_worker.hpp"

#include <exception>
#include <system_error>
#include <utility>

#include <spdlog/spdlog.h>


namespace dcn::async
{
    SignalWorker::SignalWorker(
        asio::io_context & io_context,
        std::vector<int> signal_ids,
        std::function<asio::awaitable<void>()> graceful_close_handler,
        std::function<void()> close_handler)
        : _io_context(io_context)
        , _signal_set(io_context)
        , _graceful_close_handler(std::move(graceful_close_handler))
        , _close_handler(std::move(close_handler))
    {
        for(const int signal_id : signal_ids)
        {
            std::error_code signal_ec;
            _signal_set.add(signal_id, signal_ec);
            if(signal_ec)
            {
                spdlog::debug(
                    "Skipping unsupported shutdown signal {}: {}",
                    signal_id,
                    signal_ec.message());
            }
            else
            {
                ++_registered_signal_count;
            }
        }

        if(_registered_signal_count == 0)
        {
            spdlog::warn("No shutdown signals registered; graceful signal shutdown is disabled");
        }
    }

    void SignalWorker::start()
    {
        if(_started)
        {
            spdlog::warn("Signal worker already started");
            return;
        }
        _started = true;

        if(_registered_signal_count == 0)
        {
            return;
        }

        armSignalWait();
    }

    void SignalWorker::armSignalWait()
    {
        _signal_set.async_wait(
            [this](const std::error_code & ec, const int signal_number)
            {
                if(ec)
                {
                    if(ec != asio::error::operation_aborted)
                    {
                        spdlog::warn("Signal handler error: {}", ec.message());
                    }
                    return;
                }

                if(!_shutdown_started.exchange(true))
                {
                    spdlog::info("Received signal {}. Starting graceful shutdown.", signal_number);

                    armSignalWait();
                    if(!_graceful_close_handler)
                    {
                        spdlog::warn("No graceful close handler configured. Stopping io_context.");
                        _io_context.stop();
                        return;
                    }

                    try
                    {
                        asio::co_spawn(
                            _io_context,
                            _graceful_close_handler(),
                            // graceful shutdown finished
                            [io_context = &_io_context](std::exception_ptr exception_ptr)
                            {
                                if(exception_ptr)
                                {
                                    try
                                    {
                                        std::rethrow_exception(exception_ptr);
                                    }
                                    catch(const std::exception & e)
                                    {
                                        spdlog::error("Graceful shutdown failed: {}", e.what());
                                    }
                                    catch(...)
                                    {
                                        spdlog::error("Graceful shutdown failed with unknown error");
                                    }
                                }

                                spdlog::info("Stopping io_context");
                                io_context->stop();
                                spdlog::info("io_context stopped");
                            });
                    }
                    catch(const std::exception & e)
                    {
                        spdlog::error("Failed to start graceful shutdown handler: {}", e.what());
                        _io_context.stop();
                    }
                    catch(...)
                    {
                        spdlog::error("Failed to start graceful shutdown handler with unknown error");
                        _io_context.stop();
                    }
                    return;
                }

                spdlog::warn("Received signal {} during shutdown. Forcing immediate stop.", signal_number);

                if(_close_handler)
                {
                    try
                    {
                        _close_handler();
                    }
                    catch(const std::exception & e)
                    {
                        spdlog::error("Immediate close handler failed: {}", e.what());
                    }
                    catch(...)
                    {
                        spdlog::error("Immediate close handler failed with unknown error");
                    }
                }
                else
                {
                    spdlog::warn("No immediate close handler configured");
                }
                
                spdlog::info("Stopping io_context");
                _io_context.stop();
                spdlog::info("io_context stopped");
            });
    }
}
