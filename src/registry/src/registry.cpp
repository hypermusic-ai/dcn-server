#include "registry.hpp"

namespace dcn
{   
    Registry::Registry( asio::io_context & io_context)
    : _strand(asio::make_strand(io_context))
    {

    }

    asio::awaitable<bool> Registry::checkIfSubFeaturesExist(const Feature & feature) const
    {
        co_await utils::ensureOnStrand(_strand);

        for(const Dimension & dimension : feature.dimensions())
        {
            const std::string & subfeature_name = dimension.feature_name();
            if(co_await isFeatureBucketEmpty(subfeature_name))co_return false;

            const auto & sub_feature_node = _features.at(subfeature_name).at(_newest_feature.at(subfeature_name));
            co_return (co_await checkIfSubFeaturesExist(sub_feature_node.feature()));
        }
        co_return true;
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


    asio::awaitable<bool> Registry::addFeature(evm::Address address, FeatureRecord record)
    {
        if(record.feature().name().empty())
        {
            spdlog::error("Feature name is empty");
            co_return false;
        }

        co_await utils::ensureOnStrand(_strand);

        if(co_await checkIfSubFeaturesExist(record.feature()) == false)
        {
            spdlog::error("Cannot find subfeatures for feature `{}`", record.feature().name());
            co_return false;
        }

        if(! co_await containsFeatureBucket(record.feature().name()))
        {
            spdlog::debug("Feature bucket `{}` does not exists, creating new one ... ", record.feature().name());
            _features.try_emplace(record.feature().name(), absl::flat_hash_map<evm::Address, FeatureRecord>());
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

        static const std::filesystem::path out_dir = getStoragePath() / "features";
        if(std::filesystem::exists(out_dir) == false)
        {
            spdlog::error("Directory {} does not exists", out_dir.string());
            co_return false;
        }

        const auto parsing_result = parse::parseToJson(record, parse::use_protobuf);
        if(!parsing_result)
        {
            spdlog::error("Failed to parse feature `{}`", record.feature().name());
            co_return false;
        }

        std::optional<evm::Address> owner_res = evmc::from_hex<evm::Address>(record.owner());
        if(!owner_res)
        {
            spdlog::error("Failed to parse owner address");
            co_return false;
        }

        std::ofstream output_file(out_dir / (record.feature().name() + ".json"), std::ios::out | std::ios::trunc);

        output_file << *parsing_result;
        output_file.close();

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

    asio::awaitable<std::optional<Feature>> Registry::getFeature(const std::string& name, const evm::Address & address) const
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


    asio::awaitable<bool> Registry::addTransformation(evm::Address address, TransformationRecord record)
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
            _transformations.try_emplace(record.transformation().name(), absl::flat_hash_map<evm::Address, TransformationRecord>());
        }

        if(_transformations.at(record.transformation().name()).contains(address))
        {
            spdlog::error("Transformation `{}` of this signature already exists", record.transformation().name());
            co_return false;
        }
        
        static const std::filesystem::path out_dir = getStoragePath() / "transformations";
        if(std::filesystem::exists(out_dir) == false)
        {
            spdlog::error("Directory {} does not exists", out_dir.string());
            co_return false;
        }

        const auto parsing_result = parse::parseToJson(record, parse::use_protobuf);
        if(!parsing_result)
        {
            spdlog::error("Failed to parse transformation `{}`", record.transformation().name());
            co_return false;
        }

        std::optional<evm::Address> owner_res = evmc::from_hex<evm::Address>(record.owner());
        if(!owner_res)
        {
            spdlog::error("Failed to parse owner address");
            co_return false;
        }

        std::ofstream output_file(out_dir / (record.transformation().name() + ".json"), std::ios::out | std::ios::trunc);

        output_file << *parsing_result;
        output_file.close();

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

    asio::awaitable<std::optional<Transformation>> Registry::getTransformation(const std::string& name, const evm::Address & address) const
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


    asio::awaitable<bool> Registry::addCondition(evm::Address address, Condition condition)
    {
        co_await utils::ensureOnStrand(_strand);

        if(! co_await containsConditionBucket(condition.name())) 
        {
            spdlog::debug("Condition bucket `{}` does not exists, creating new one ... ", condition.name());
            _conditions.try_emplace(condition.name(), absl::flat_hash_map<evm::Address, Node<Condition>>());
        }


        if(_conditions.at(condition.name()).contains(address))
        {
            spdlog::error("Condition `{}` of this signature already exists", condition.name());
            co_return false;
        }

        _newest_condition[condition.name()] = address;
        _conditions.at(condition.name())    
            .try_emplace(
                std::move(address),
                Node<Condition>{std::move(condition)});
        co_return true;
    }

    asio::awaitable<std::optional<Condition>> Registry::getNewestCondition(const std::string& name) const
    {
        co_await utils::ensureOnStrand(_strand);

        if(_newest_condition.contains(name) == false)co_return std::nullopt;
        co_return (co_await getCondition(name, _newest_condition.at(name)));
    }

    asio::awaitable<std::optional<Condition>> Registry::getCondition(const std::string& name, const evm::Address& address) const
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
        co_return it->second.value;
    }

    asio::awaitable<absl::flat_hash_set<std::string>> Registry::getOwnedFeatures(const evm::Address & address) const
    {
        co_await utils::ensureOnStrand(_strand);

        if(_owned_features.contains(address) == false)co_return absl::flat_hash_set<std::string>{};
        co_return _owned_features.at(address);
    }

    asio::awaitable<absl::flat_hash_set<std::string>> Registry::getOwnedTransformations(const evm::Address & address) const
    {
        co_await utils::ensureOnStrand(_strand);

        if(_owned_transformations.contains(address) == false)co_return absl::flat_hash_set<std::string>{};
        co_return _owned_transformations.at(address);
    }
}