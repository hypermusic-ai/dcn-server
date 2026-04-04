#pragma once

#include <atomic>
#include <functional>
#include <vector>

#include "native.h"
#include <asio.hpp>

namespace dcn::async
{
    class SignalWorker
    {
        public:
            SignalWorker(
                asio::io_context & io_context,
                std::vector<int> signal_ids,
                std::function<asio::awaitable<void>()> graceful_close_handler,
                std::function<void()> close_handler);
            
            ~SignalWorker() = default;

            SignalWorker(const SignalWorker &) = delete;
            SignalWorker & operator=(const SignalWorker &) = delete;
            SignalWorker(SignalWorker &&) = delete;
            SignalWorker & operator=(SignalWorker &&) = delete;

            void start();

        private:
            void armSignalWait();

            asio::io_context & _io_context;
            asio::signal_set _signal_set;

            std::function<asio::awaitable<void>()> _graceful_close_handler;
            std::function<void()> _close_handler;

            std::atomic<bool> _shutdown_started = false;
            std::size_t _registered_signal_count = 0;
            bool _started = false;
    };
}
