#pragma once

#include <cstddef>
#include <cstdint>
#include <string_view>

#include "native.h"
#include <asio.hpp>

#include "event_projector.hpp"
#include "sqlite_hot_store.hpp"
#include "registry.hpp"

namespace dcn::registry
{
    /**
     * @brief Materialises finalised registry events from the hot-store changelog into the
     *        Registry database, using an independent change_seq cursor persisted in the
     *        registry DB.  Implements events::IEventProjector so it can be driven by the
     *        existing EventsRuntime projection loop.
     *
     * Cursor policy:
     *   - cursor is loaded from the registry DB at construction and persisted after each
     *     batch (only if at least one row was consumed or skipped).
     *   - non-finalized rows (observed/safe/removed) are skipped; their change_seq is
     *     still advanced past because they will re-surface with a higher change_seq when
     *     they reach "finalized".
     *
     * Failure policy (per _materializeOne):
     *   - success / already present → cursor advanced.
     *   - failure (undecodable log, divergence, add rejection) → cursor is left before
     *     the row and the row is retried on subsequent projectBatch passes, so transient
     *     failures (e.g. busy registry DB) heal on their own. After
     *     MAX_MATERIALIZE_ATTEMPTS consecutive failing passes the row is poison-skipped
     *     with an error log carrying its block/log identifiers: a permanently parked
     *     cursor would also freeze the prune watermark for every projector and grow the
     *     hot store without bound.
     */
    class RegistryProjector final : public events::IEventProjector
    {
        public:
            // ponytail: fixed retry budget (~1s at the default 200ms projector interval);
            // make it configurable if a real transient failure ever needs longer to heal.
            static constexpr std::size_t MAX_MATERIALIZE_ATTEMPTS = 5;

            RegistryProjector(
                events::SQLiteHotStore & store,
                Registry & registry,
                const asio::strand<asio::io_context::executor_type> & write_strand);

            std::string_view id() const override;

            std::int64_t cursor() const override { return _cursor; }

            asio::awaitable<std::size_t> projectBatch(std::size_t limit, std::int64_t now_ms) override;

        private:
            // Returns true  → row materialized (or already present); advance past it.
            // Returns false → materialization failed; projectBatch retries the row on
            //                 later passes and poison-skips it after
            //                 MAX_MATERIALIZE_ATTEMPTS consecutive failures.
            asio::awaitable<bool> _materializeOne(const events::ChangeRecord & row);

            events::SQLiteHotStore & _store;
            Registry & _registry;
            asio::strand<asio::io_context::executor_type> _write_strand;
            std::int64_t _cursor{0};

            // Consecutive-failure tracking for the row currently at the cursor head.
            std::int64_t _failing_seq{0};
            std::size_t _failure_count{0};
    };
}
