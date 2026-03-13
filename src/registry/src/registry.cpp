#include "registry.hpp"

#include <charconv>
#include <functional>
#include <limits>
#include <system_error>

namespace dcn::registry
{
    Registry::Registry(asio::io_context & io_context)
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
        if(co_await containsConnectorBucket(name) == false)
        {
            co_return true;
        }
        co_return _connectors.at(name).empty();
    }

    asio::awaitable<bool> Registry::containsTransformationBucket(const std::string& name) const
    {
        co_await utils::ensureOnStrand(_strand);
        co_return _transformations.contains(name);
    }

    asio::awaitable<bool> Registry::isTransformationBucketEmpty(const std::string& name) const
    {
        co_await utils::ensureOnStrand(_strand);
        if(co_await containsTransformationBucket(name) == false)
        {
            co_return true;
        }
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
        if(co_await containsConditionBucket(name) == false)
        {
            co_return true;
        }
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

        if(!co_await containsConnectorBucket(record.connector().name()))
        {
            spdlog::debug("Connector bucket `{}` does not exists, creating new one ... ", record.connector().name());
            _connectors.try_emplace(record.connector().name(), absl::flat_hash_map<chain::Address, ConnectorRecord>());
        }

        if(_connectors.at(record.connector().name()).contains(address))
        {
            spdlog::error("Connector `{}` of this signature already exists", record.connector().name());
            co_return false;
        }

        if(record.connector().dimensions_size() <= 0)
        {
            spdlog::error("Connector `{}` has zero dimensions", record.connector().name());
            co_return false;
        }

        for(const Dimension & dimension : record.connector().dimensions())
        {
            for(const auto & transformation : dimension.transformations())
            {
                if(transformation.name().empty())
                {
                    spdlog::error("Connector `{}` contains unnamed transformation", record.connector().name());
                    co_return false;
                }

                if(!co_await containsTransformationBucket(transformation.name()))
                {
                    spdlog::error(
                        "Cannot find transformation `{}` used in connector `{}`",
                        transformation.name(),
                        record.connector().name());
                    co_return false;
                }
                if(co_await isTransformationBucketEmpty(transformation.name()))
                {
                    spdlog::error(
                        "Cannot find transformation `{}` used in connector `{}`",
                        transformation.name(),
                        record.connector().name());
                    co_return false;
                }
            }
        }

        const std::uint32_t dimensions_count = static_cast<std::uint32_t>(record.connector().dimensions_size());
        for(std::uint32_t dim_id = 0; dim_id < dimensions_count; ++dim_id)
        {
            const Dimension & dimension = record.connector().dimensions(static_cast<int>(dim_id));
            const std::string & composite = dimension.composite();
            if(composite.empty())
            {
                if(dimension.bindings_size() != 0)
                {
                    spdlog::error(
                        "Connector `{}` has bindings on scalar dimension {}",
                        record.connector().name(),
                        dim_id);
                    co_return false;
                }
                continue;
            }

            if(!co_await containsConnectorBucket(composite))
            {
                spdlog::error("Cannot find connector `{}` used in connector `{}`", composite, record.connector().name());
                co_return false;
            }

            if(co_await isConnectorBucketEmpty(composite))
            {
                spdlog::error("Cannot find connector `{}` used in connector `{}`", composite, record.connector().name());
                co_return false;
            }

            for(const auto & [slot, binding_target] : dimension.bindings())
            {
                if(binding_target.empty())
                {
                    spdlog::error(
                        "Connector `{}` has empty binding target at dim {} slot `{}`",
                        record.connector().name(),
                        dim_id,
                        slot);
                    co_return false;
                }

                if(!co_await containsConnectorBucket(binding_target))
                {
                    spdlog::error(
                        "Cannot find connector `{}` used in binding at connector `{}` dim {} slot `{}`",
                        binding_target,
                        record.connector().name(),
                        dim_id,
                        slot);
                    co_return false;
                }

                if(co_await isConnectorBucketEmpty(binding_target))
                {
                    spdlog::error(
                        "Cannot find connector `{}` used in binding at connector `{}` dim {} slot `{}`",
                        binding_target,
                        record.connector().name(),
                        dim_id,
                        slot);
                    co_return false;
                }
            }
        }

        if(!record.connector().condition_name().empty())
        {
            const std::string & condition_name = record.connector().condition_name();
            if(!co_await containsConditionBucket(condition_name) || co_await isConditionBucketEmpty(condition_name))
            {
                spdlog::error(
                    "Cannot find condition `{}` used in connector `{}`",
                    condition_name,
                    record.connector().name());
                co_return false;
            }
        }

        const auto resolveConnector = [&](const std::string & name) -> const Connector*
        {
            if(name == record.connector().name())
            {
                return &record.connector();
            }

            auto newest_it = _newest_connector.find(name);
            if(newest_it == _newest_connector.end())
            {
                return nullptr;
            }

            auto bucket_it = _connectors.find(name);
            if(bucket_it == _connectors.end())
            {
                return nullptr;
            }

            auto connector_it = bucket_it->second.find(newest_it->second);
            if(connector_it == bucket_it->second.end())
            {
                return nullptr;
            }

            return &connector_it->second.connector();
        };

        enum class VisitState : std::uint8_t
        {
            VISITING,
            DONE
        };

        absl::flat_hash_map<std::string, VisitState> visit_state;
        absl::flat_hash_map<std::string, std::uint32_t> open_slots_cache;

        std::function<std::optional<std::uint32_t>(const std::string&)> computeOpenSlots;
        computeOpenSlots = [&](const std::string & connector_name) -> std::optional<std::uint32_t>
        {
            if(const auto cache_it = open_slots_cache.find(connector_name); cache_it != open_slots_cache.end())
            {
                return cache_it->second;
            }

            if(const auto state_it = visit_state.find(connector_name);
                state_it != visit_state.end() && state_it->second == VisitState::VISITING)
            {
                spdlog::error("Connector dependency cycle detected at `{}`", connector_name);
                return std::nullopt;
            }

            const Connector * connector = resolveConnector(connector_name);
            if(connector == nullptr)
            {
                spdlog::error("Cannot resolve connector definition `{}` while validating `{}`", connector_name, record.connector().name());
                return std::nullopt;
            }

            visit_state[connector_name] = VisitState::VISITING;

            std::uint64_t open_slots = 0;
            for(std::uint32_t dim_id = 0; dim_id < static_cast<std::uint32_t>(connector->dimensions_size()); ++dim_id)
            {
                const Dimension & dimension = connector->dimensions(static_cast<int>(dim_id));
                const std::string & composite_name = dimension.composite();
                if(composite_name.empty())
                {
                    if(dimension.bindings_size() != 0)
                    {
                        spdlog::error(
                            "Connector `{}` has bindings on scalar dimension {}",
                            connector_name,
                            dim_id);
                        return std::nullopt;
                    }

                    open_slots += 1;
                    continue;
                }

                const auto child_open_slots_opt = computeOpenSlots(composite_name);
                if(!child_open_slots_opt)
                {
                    return std::nullopt;
                }
                const std::uint32_t child_open_slots = *child_open_slots_opt;
                open_slots += child_open_slots;

                absl::flat_hash_set<std::uint32_t> bound_slot_ids;
                for(const auto & [slot_str, binding_target] : dimension.bindings())
                {
                    if(binding_target.empty())
                    {
                        spdlog::error(
                            "Connector `{}` has empty binding target at dim {} slot `{}`",
                            connector_name,
                            dim_id,
                            slot_str);
                        return std::nullopt;
                    }

                    std::uint32_t slot_id = 0;
                    const auto [ptr, ec] = std::from_chars(
                        slot_str.data(),
                        slot_str.data() + slot_str.size(),
                        slot_id,
                        10);
                    if(ec != std::errc{} || ptr != (slot_str.data() + slot_str.size()))
                    {
                        spdlog::error(
                            "Connector `{}` has invalid binding slot id `{}` at dim {}",
                            connector_name,
                            slot_str,
                            dim_id);
                        return std::nullopt;
                    }

                    if(!bound_slot_ids.insert(slot_id).second)
                    {
                        spdlog::error(
                            "Connector `{}` has duplicate binding slot id {} at dim {}",
                            connector_name,
                            slot_id,
                            dim_id);
                        return std::nullopt;
                    }

                    if(slot_id >= child_open_slots)
                    {
                        spdlog::error(
                            "Connector `{}` binding slot {} is out of range for child `{}` (open slots: {})",
                            connector_name,
                            slot_id,
                            composite_name,
                            child_open_slots);
                        return std::nullopt;
                    }

                    if(resolveConnector(binding_target) == nullptr)
                    {
                        spdlog::error(
                            "Cannot resolve binding target `{}` in connector `{}` dim {} slot {}",
                            binding_target,
                            connector_name,
                            dim_id,
                            slot_id);
                        return std::nullopt;
                    }

                    if(!computeOpenSlots(binding_target))
                    {
                        return std::nullopt;
                    }
                }

                open_slots -= static_cast<std::uint64_t>(bound_slot_ids.size());
            }

            if(open_slots > std::numeric_limits<std::uint32_t>::max())
            {
                spdlog::error(
                    "Connector `{}` exceeds supported open slots range",
                    connector_name);
                return std::nullopt;
            }

            visit_state[connector_name] = VisitState::DONE;
            open_slots_cache.emplace(connector_name, static_cast<std::uint32_t>(open_slots));
            return static_cast<std::uint32_t>(open_slots);
        };

        if(!computeOpenSlots(record.connector().name()))
        {
            spdlog::error("Connector `{}` failed slot/binding validation", record.connector().name());
            co_return false;
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

        if(_newest_connector.contains(name) == false)
        {
            co_return std::nullopt;
        }
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

        if(_newest_transformation.contains(name) == false)
        {
            co_return std::nullopt;
        }
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

        if(_newest_condition.contains(name) == false)
        {
            co_return std::nullopt;
        }
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

        if(_owned_connectors.contains(address) == false)
        {
            co_return absl::flat_hash_set<std::string>{};
        }
        co_return _owned_connectors.at(address);
    }

    asio::awaitable<absl::flat_hash_set<std::string>> Registry::getOwnedTransformations(const chain::Address & address) const
    {
        co_await utils::ensureOnStrand(_strand);

        if(_owned_transformations.contains(address) == false)
        {
            co_return absl::flat_hash_set<std::string>{};
        }
        co_return _owned_transformations.at(address);
    }

    asio::awaitable<absl::flat_hash_set<std::string>> Registry::getOwnedConditions(const chain::Address & address) const
    {
        co_await utils::ensureOnStrand(_strand);

        if(_owned_conditions.contains(address) == false)
        {
            co_return absl::flat_hash_set<std::string>{};
        }
        co_return _owned_conditions.at(address);
    }

    asio::awaitable<bool> Registry::add(chain::Address address, ConnectorRecord connector)
    {
        return addConnector(address, std::move(connector));
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
