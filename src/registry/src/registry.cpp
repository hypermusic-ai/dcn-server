#include "registry.hpp"

namespace dcn::registry
{   
    Registry::Registry( asio::io_context & io_context)
    : _strand(asio::make_strand(io_context))
    {

    }

    asio::awaitable<bool> Registry::containsConnectorBucket(const std::string& name) const 
    {
        co_await utils::ensureOnStrand(_strand);

        co_return _connectors.contains(name);
    }

    asio::awaitable<bool> Registry::isConnectorBucketEmpty(const std::string& name) const 
    {
        co_await utils::ensureOnStrand(_strand);

        if(co_await containsConnectorBucket(name) == false)co_return true;
        co_return _connectors.at(name).empty();
    }

    asio::awaitable<bool> Registry::containsFeatureBucket(const std::string& name) const 
    {
        co_await utils::ensureOnStrand(_strand);

        co_return _features.contains(name);
    }

    asio::awaitable<bool> Registry::isFeatureBucketEmpty(const std::string& name) const 
    {
        co_await utils::ensureOnStrand(_strand);

        if(co_await containsFeatureBucket(name) == false)co_return true;
        co_return _features.at(name).empty();
    }

    asio::awaitable<bool> Registry::containsTransformationBucket(const std::string& name) const 
    {
        co_await utils::ensureOnStrand(_strand);

        co_return _transformations.contains(name);
    }

    asio::awaitable<bool> Registry::isTransformationBucketEmpty(const std::string& name) const 
    {
        co_await utils::ensureOnStrand(_strand);

        if(co_await containsTransformationBucket(name) == false)co_return true;
        co_return _transformations.at(name).empty();
    }

    asio::awaitable<bool> Registry::containsConditionBucket(const std::string& name) const 
    {
        co_await utils::ensureOnStrand(_strand);

        co_return _conditions.contains(name);
    }

    asio::awaitable<bool> Registry::isConditionBucketEmpty(const std::string& name) const 
    {
        co_await utils::ensureOnStrand(_strand);

        if(co_await containsConditionBucket(name) == false)co_return true;
        co_return _conditions.at(name).empty();
    }

    asio::awaitable<bool> Registry::addConnector(chain::Address address, ConnectorRecord record)
    {
        if(record.connector().name().empty())
        {
            spdlog::error("Connector name is empty");
            co_return false;
        }

        co_await utils::ensureOnStrand(_strand);

        if(! co_await containsConnectorBucket(record.connector().name()))
        {
            spdlog::debug("Connector bucket `{}` does not exists, creating new one ... ", record.connector().name());
            _connectors.try_emplace(record.connector().name(), absl::flat_hash_map<chain::Address, ConnectorRecord>());
        }       

        if(_connectors.at(record.connector().name()).contains(address))
        {
            spdlog::error("Connector `{}` of this signature already exists", record.connector().name());
            co_return false;
        }

        // check if root feature exists
        if(! co_await containsFeatureBucket(record.connector().feature_name()))
        {
            spdlog::error("Cannot find feature `{}` used in connector `{}`", record.connector().feature_name(), record.connector().name());
            co_return false;
        }

        if(co_await isFeatureBucketEmpty(record.connector().feature_name()))
        {
            spdlog::error("Cannot find feature `{}` used in connector `{}`", record.connector().feature_name(), record.connector().name());
            co_return false;
        }

        const auto feature_res = co_await getNewestFeature(record.connector().feature_name());
        if(!feature_res)
        {
            spdlog::error("Cannot fetch feature `{}` used in connector `{}`", record.connector().feature_name(), record.connector().name());
            co_return false;
        }

        const std::uint32_t feature_dimensions_count = static_cast<std::uint32_t>(feature_res->dimensions_size());

        // check if composites exist
        for(const auto & [dim_id, composite] : record.connector().composites())
        {
            if(dim_id >= feature_dimensions_count)
            {
                spdlog::error("Composite dimension `{}` out of range for connector `{}`", dim_id, record.connector().name());
                co_return false;
            }

            if(composite.empty())
            {
                spdlog::error("Composite name is empty at dimension `{}` for connector `{}`", dim_id, record.connector().name());
                co_return false;
            }

            if(! co_await containsConnectorBucket(composite))
            {
                spdlog::error("Cannot find connector `{}` used in connector `{}`", composite, record.connector().name());
                co_return false;
            }
            if(co_await isConnectorBucketEmpty(composite))
            {
                spdlog::error("Cannot find connector `{}` used in connector `{}`", composite, record.connector().name());
                co_return false;
            }
        }

        std::optional<chain::Address> owner_res = evmc::from_hex<chain::Address>(record.owner());
        if(!owner_res)
        {
            spdlog::error("Failed to parse owner address");
            co_return false;
        }

        _owned_connectors[*owner_res].emplace(record.connector().name());

        _newest_connector[record.connector().name()] = address;
        _connectors.at(record.connector().name()).try_emplace(std::move(address), std::move(record));

        co_return true;
    }

    asio::awaitable<std::optional<Connector>> Registry::getNewestConnector(const std::string& name) const
    {
        co_await utils::ensureOnStrand(_strand);

        if(_newest_connector.contains(name) == false)co_return std::nullopt;
        co_return (co_await getConnector(name, _newest_connector.at(name)));
    }

    asio::awaitable<std::optional<Connector>> Registry::getConnector(const std::string& name, const chain::Address & address) const
    {
        co_await utils::ensureOnStrand(_strand);

        auto bucket_it = _connectors.find(name);
        if(bucket_it == _connectors.end()) 
        {
            co_return std::nullopt;
        }
        auto it = bucket_it->second.find(address);
        if(it == bucket_it->second.end()) 
        {
            co_return std::nullopt;
        }
        co_return it->second.connector();
    }

    asio::awaitable<bool> Registry::addFeature(chain::Address address, FeatureRecord record)
    {
        if(record.feature().name().empty())
        {
            spdlog::error("Feature name is empty");
            co_return false;
        }

        co_await utils::ensureOnStrand(_strand);

        if(! co_await containsFeatureBucket(record.feature().name()))
        {
            spdlog::debug("Feature bucket `{}` does not exists, creating new one ... ", record.feature().name());
            _features.try_emplace(record.feature().name(), absl::flat_hash_map<chain::Address, FeatureRecord>());
        }       

        if(_features.at(record.feature().name()).contains(address))
        {
            spdlog::error("Feature `{}` of this signature already exists", record.feature().name());
            co_return false;
        }

        // check if transformations exists
        for(const Dimension & dimension : record.feature().dimensions())
        {
            for(const auto & transformation : dimension.transformations())
            {
                if(! co_await containsTransformationBucket(transformation.name()))
                {
                    spdlog::error("Cannot find transformation `{}` used in feature `{}`", transformation.name(), record.feature().name());
                    co_return false;
                }
                if(co_await isTransformationBucketEmpty(transformation.name()))
                {
                    spdlog::error("Cannot find transformation `{}` used in feature `{}`", transformation.name(), record.feature().name());
                    co_return false;
                }
            }
        }

        std::optional<chain::Address> owner_res = evmc::from_hex<chain::Address>(record.owner());
        if(!owner_res)
        {
            spdlog::error("Failed to parse owner address");
            co_return false;
        }

        _owned_features[*owner_res].emplace(record.feature().name());

        _newest_feature[record.feature().name()] = address;
        _features.at(record.feature().name()).try_emplace(std::move(address), std::move(record));

        co_return true;
    }

    asio::awaitable<std::optional<Feature>> Registry::getNewestFeature(const std::string& name) const
    {
        co_await utils::ensureOnStrand(_strand);

        if(_newest_feature.contains(name) == false)co_return std::nullopt;
        co_return (co_await getFeature(name, _newest_feature.at(name)));
    }

    asio::awaitable<std::optional<Feature>> Registry::getFeature(const std::string& name, const chain::Address & address) const
    {
        co_await utils::ensureOnStrand(_strand);

        auto bucket_it = _features.find(name);
        if(bucket_it == _features.end()) 
        {
            co_return std::nullopt;
        }
        auto it = bucket_it->second.find(address);
        if(it == bucket_it->second.end()) 
        {
            co_return std::nullopt;
        }
        co_return it->second.feature();
    }


    asio::awaitable<bool> Registry::addTransformation(chain::Address address, TransformationRecord record)
    {
        if(record.transformation().name().empty())
        {
            spdlog::error("Transformation name is empty");
            co_return false;
        }

        co_await utils::ensureOnStrand(_strand);

        if(!co_await containsTransformationBucket(record.transformation().name())) 
        {
            spdlog::debug("Transformation bucket `{}` does not exists, creating new one ... ", record.transformation().name());
            _transformations.try_emplace(record.transformation().name(), absl::flat_hash_map<chain::Address, TransformationRecord>());
        }

        if(_transformations.at(record.transformation().name()).contains(address))
        {
            spdlog::error("Transformation `{}` of this signature already exists", record.transformation().name());
            co_return false;
        }
        
        std::optional<chain::Address> owner_res = evmc::from_hex<chain::Address>(record.owner());
        if(!owner_res)
        {
            spdlog::error("Failed to parse owner address");
            co_return false;
        }

        _owned_transformations[*owner_res].emplace(record.transformation().name());

        _newest_transformation[record.transformation().name()] = address;
        _transformations.at(record.transformation().name()).try_emplace(std::move(address), std::move(record));

        co_return true;
    }

    asio::awaitable<std::optional<Transformation>> Registry::getNewestTransformation(const std::string& name) const
    {
        co_await utils::ensureOnStrand(_strand);

        if(_newest_transformation.contains(name) == false)co_return std::nullopt;
        co_return (co_await getTransformation(name, _newest_transformation.at(name)));
    }

    asio::awaitable<std::optional<Transformation>> Registry::getTransformation(const std::string& name, const chain::Address & address) const
    {
        co_await utils::ensureOnStrand(_strand);

        auto bucket_it = _transformations.find(name);
        if(bucket_it == _transformations.end()) 
        {
            co_return std::nullopt;
        }
        auto it = bucket_it->second.find(address);
        if(it == bucket_it->second.end()) 
        {
            co_return std::nullopt;
        }
        co_return it->second.transformation();
    }


    asio::awaitable<bool> Registry::addCondition(chain::Address address, ConditionRecord record)
    {
        if(record.condition().name().empty())
        {
            spdlog::error("Condition name is empty");
            co_return false;
        }

        co_await utils::ensureOnStrand(_strand);

        if(!co_await containsConditionBucket(record.condition().name())) 
        {
            spdlog::debug("Condition bucket `{}` does not exists, creating new one ... ", record.condition().name());
            _conditions.try_emplace(record.condition().name(), absl::flat_hash_map<chain::Address, ConditionRecord>());
        }

        if(_conditions.at(record.condition().name()).contains(address))
        {
            spdlog::error("Condition `{}` of this signature already exists", record.condition().name());
            co_return false;
        }
        
        std::optional<chain::Address> owner_res = evmc::from_hex<chain::Address>(record.owner());
        if(!owner_res)
        {
            spdlog::error("Failed to parse owner address");
            co_return false;
        }

        _owned_conditions[*owner_res].emplace(record.condition().name());

        _newest_condition[record.condition().name()] = address;
        _conditions.at(record.condition().name()).try_emplace(std::move(address), std::move(record));

        co_return true;
    }

    asio::awaitable<std::optional<Condition>> Registry::getNewestCondition(const std::string& name) const
    {
        co_await utils::ensureOnStrand(_strand);

        if(_newest_condition.contains(name) == false)co_return std::nullopt;
        co_return (co_await getCondition(name, _newest_condition.at(name)));
    }

    asio::awaitable<std::optional<Condition>> Registry::getCondition(const std::string& name, const chain::Address & address) const
    {
        co_await utils::ensureOnStrand(_strand);

        auto bucket_it = _conditions.find(name);
        if(bucket_it == _conditions.end()) 
        {
            co_return std::nullopt;
        }
        auto it = bucket_it->second.find(address);
        if(it == bucket_it->second.end()) 
        {
            co_return std::nullopt;
        }
        co_return it->second.condition();
    }

    asio::awaitable<absl::flat_hash_set<std::string>> Registry::getOwnedConnectors(const chain::Address & address) const
    {
        co_await utils::ensureOnStrand(_strand);

        if(_owned_connectors.contains(address) == false)co_return absl::flat_hash_set<std::string>{};
        co_return _owned_connectors.at(address);
    }

    asio::awaitable<absl::flat_hash_set<std::string>> Registry::getOwnedFeatures(const chain::Address & address) const
    {
        co_await utils::ensureOnStrand(_strand);

        if(_owned_features.contains(address) == false)co_return absl::flat_hash_set<std::string>{};
        co_return _owned_features.at(address);
    }

    asio::awaitable<absl::flat_hash_set<std::string>> Registry::getOwnedTransformations(const chain::Address & address) const
    {
        co_await utils::ensureOnStrand(_strand);

        if(_owned_transformations.contains(address) == false)co_return absl::flat_hash_set<std::string>{};
        co_return _owned_transformations.at(address);
    }

    asio::awaitable<absl::flat_hash_set<std::string>> Registry::getOwnedConditions(const chain::Address & address) const
    {
        co_await utils::ensureOnStrand(_strand);

        if(_owned_conditions.contains(address) == false)co_return absl::flat_hash_set<std::string>{};
        co_return _owned_conditions.at(address);
    }



    asio::awaitable<bool> Registry::add(chain::Address address, ConnectorRecord connector)
    {
        return addConnector(address, std::move(connector));
    }

    asio::awaitable<bool> Registry::add(chain::Address address, FeatureRecord feature)
    {
        return addFeature(address, std::move(feature));
    }

    asio::awaitable<bool> Registry::add(chain::Address address, TransformationRecord transformation)
    {
        return addTransformation(address, std::move(transformation));
    }

    asio::awaitable<bool> Registry::add(chain::Address address, ConditionRecord condition)
    {
        return addCondition(address, std::move(condition));
    }
}