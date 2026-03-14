#pragma once

#include <optional>
#include <string>

#include "native.h"
#include <asio.hpp>
#include <absl/container/flat_hash_map.h>
#include <absl/container/flat_hash_set.h>
#include <spdlog/spdlog.h>

#include "utils.hpp"
#include "pt.hpp"
#include "address.hpp"

namespace dcn::registry
{
    class Registry
    {
        public:
            Registry() = delete;
            Registry(asio::io_context & io_context);

            Registry(const Registry&) = delete;
            Registry& operator=(const Registry&) = delete;

            ~Registry() = default;

            asio::awaitable<bool> add(chain::Address address, ConnectorRecord connector);
            asio::awaitable<bool> add(chain::Address address, TransformationRecord transformation);
            asio::awaitable<bool> add(chain::Address address, ConditionRecord condition);

            asio::awaitable<bool> addConnector(chain::Address address, ConnectorRecord connector);
            asio::awaitable<std::optional<Connector>> getNewestConnector(const std::string& name) const;
            asio::awaitable<std::optional<Connector>> getConnector(const std::string& name, const chain::Address & address) const;

            asio::awaitable<bool> addTransformation(chain::Address address, TransformationRecord transformation);
            asio::awaitable<std::optional<Transformation>> getNewestTransformation(const std::string& name) const;
            asio::awaitable<std::optional<Transformation>> getTransformation(const std::string& name, const chain::Address & address) const;

            asio::awaitable<bool> addCondition(chain::Address address, ConditionRecord condition);
            asio::awaitable<std::optional<Condition>> getNewestCondition(const std::string& name) const;
            asio::awaitable<std::optional<Condition>> getCondition(const std::string& name, const chain::Address & address) const;

            asio::awaitable<absl::flat_hash_set<std::string>> getOwnedConnectors(const chain::Address & address) const;
            asio::awaitable<absl::flat_hash_set<std::string>> getOwnedTransformations(const chain::Address & address) const;
            asio::awaitable<absl::flat_hash_set<std::string>> getOwnedConditions(const chain::Address & address) const;

        protected:
            asio::awaitable<bool> containsConnectorBucket(const std::string& name) const;
            asio::awaitable<bool> containsTransformationBucket(const std::string& name) const;
            asio::awaitable<bool> containsConditionBucket(const std::string& name) const;

            asio::awaitable<bool> isConnectorBucketEmpty(const std::string& name) const;
            asio::awaitable<bool> isTransformationBucketEmpty(const std::string& name) const;
            asio::awaitable<bool> isConditionBucketEmpty(const std::string& name) const;

        private:
            asio::strand<asio::io_context::executor_type> _strand;

            absl::flat_hash_map<std::string, chain::Address> _newest_connector;
            absl::flat_hash_map<std::string, chain::Address> _newest_transformation;
            absl::flat_hash_map<std::string, chain::Address> _newest_condition;

            absl::flat_hash_map<std::string, absl::flat_hash_map<chain::Address, ConnectorRecord>> _connectors;
            absl::flat_hash_map<std::string, absl::flat_hash_map<chain::Address, TransformationRecord>> _transformations;
            absl::flat_hash_map<std::string, absl::flat_hash_map<chain::Address, ConditionRecord>> _conditions;

            absl::flat_hash_map<chain::Address, absl::flat_hash_set<std::string>> _owned_connectors;
            absl::flat_hash_map<chain::Address, absl::flat_hash_set<std::string>> _owned_transformations;
            absl::flat_hash_map<chain::Address, absl::flat_hash_set<std::string>> _owned_conditions;
    };
}
