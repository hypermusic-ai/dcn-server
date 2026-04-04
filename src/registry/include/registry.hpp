#pragma once

#include <cstdint>
#include <list>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include <absl/container/flat_hash_map.h>
#include <absl/container/flat_hash_set.h>
#include <spdlog/spdlog.h>

#include "async.hpp"
#include "pt.hpp"
#include "address.hpp"
#include "format_hash.hpp"
#include "registry_store.hpp"

namespace dcn::storage
{
    using ScalarLabel = dcn::chain::ScalarLabel;

    template<typename KeyT, typename ValueT>
    struct LruCache
    {
        struct Entry
        {
            ValueT value;
            typename std::list<KeyT>::iterator order_it;
        };

        const char * name = "unnamed-cache";
        std::size_t capacity = 0;
        std::list<KeyT> order;
        absl::flat_hash_map<KeyT, Entry> entries;
    };

    class Registry
    {
        public:
            Registry() = delete;
            Registry(asio::io_context & io_context, std::string sqlite_path = ":memory:");

            Registry(const Registry&) = delete;
            Registry& operator=(const Registry&) = delete;

            ~Registry() = default;

            asio::awaitable<bool> add(chain::Address address, ConnectorRecord connector);
            asio::awaitable<bool> add(chain::Address address, TransformationRecord transformation);
            asio::awaitable<bool> add(chain::Address address, ConditionRecord condition);

            asio::awaitable<bool> addConnector(chain::Address address, ConnectorRecord connector);
            asio::awaitable<bool> addConnectorsBatch(
                std::vector<std::pair<chain::Address, ConnectorRecord>> connectors,
                bool all_or_nothing = true);
            asio::awaitable<std::optional<ConnectorRecordHandle>> getConnectorRecordHandle(
                const std::string & name) const;
            asio::awaitable<std::optional<evmc::bytes32>> getFormatHash(const std::string& name) const;
            asio::awaitable<std::size_t> getFormatConnectorNamesCount(const evmc::bytes32 & format_hash) const;
            asio::awaitable<NameCursorPage> getFormatConnectorNamesCursor(
                const evmc::bytes32 & format_hash,
                const std::optional<NameCursor> & after,
                std::size_t limit) const;
            asio::awaitable<std::optional<std::vector<ScalarLabel>>> getScalarLabelsByFormatHash(const evmc::bytes32 & format_hash) const;

            asio::awaitable<bool> addTransformation(chain::Address address, TransformationRecord transformation);
            asio::awaitable<bool> addTransformationsBatch(
                std::vector<std::pair<chain::Address, TransformationRecord>> transformations,
                bool all_or_nothing = true);
            asio::awaitable<std::optional<TransformationRecordHandle>> getTransformationRecordHandle(
                const std::string & name) const;

            asio::awaitable<bool> addCondition(chain::Address address, ConditionRecord condition);
            asio::awaitable<bool> addConditionsBatch(
                std::vector<std::pair<chain::Address, ConditionRecord>> conditions,
                bool all_or_nothing = true);
            asio::awaitable<std::optional<ConditionRecordHandle>> getConditionRecordHandle(
                const std::string & name) const;

            asio::awaitable<NameCursorPage> getOwnedConnectorsCursor(
                const chain::Address & owner,
                const std::optional<NameCursor> & after,
                std::size_t limit) const;
            asio::awaitable<NameCursorPage> getOwnedTransformationsCursor(
                const chain::Address & owner,
                const std::optional<NameCursor> & after,
                std::size_t limit) const;
            asio::awaitable<NameCursorPage> getOwnedConditionsCursor(
                const chain::Address & owner,
                const std::optional<NameCursor> & after,
                std::size_t limit) const;

            asio::awaitable<bool> checkpointWal(WalCheckpointMode mode) const;

        private:
            static constexpr std::size_t kHotCacheCapacity = 1024;

            asio::strand<asio::io_context::executor_type> _strand;
            std::unique_ptr<IRegistryStore> _store;
            mutable LruCache<std::string, std::optional<ConnectorRecordHandle>> _connector_record_cache;
            mutable LruCache<std::string, std::optional<evmc::bytes32>> _format_hash_cache;
            mutable LruCache<std::string, std::optional<TransformationRecordHandle>> _transformation_record_cache;
            mutable LruCache<std::string, std::optional<ConditionRecordHandle>> _condition_record_cache;
    };
}


