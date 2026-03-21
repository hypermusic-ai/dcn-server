#include "registry.hpp"
#include <algorithm>
#include <charconv>
#include <cstdint>
#include <limits>
#include <system_error>
#include <vector>

namespace dcn::registry
{
    namespace
    {
        using _ConnectorBuckets =
            absl::flat_hash_map<std::string, absl::flat_hash_map<chain::Address, ConnectorRecord>>;
        using _TransformationBuckets =
            absl::flat_hash_map<std::string, absl::flat_hash_map<chain::Address, TransformationRecord>>;
        using _ConditionBuckets =
            absl::flat_hash_map<std::string, absl::flat_hash_map<chain::Address, ConditionRecord>>;

        enum class _VisitState : std::uint8_t
        {
            VISITING,
            DONE
        };

        // Create bucket lazily so add paths can assume .at(name) is valid.
        template <typename TBuckets>
        static void _ensureBucket(
            const char * bucket_type_name,
            const std::string & bucket_name,
            TBuckets & buckets)
        {
            if(buckets.contains(bucket_name))
            {
                return;
            }

            spdlog::debug("{} bucket `{}` does not exists, creating new one ... ", bucket_type_name, bucket_name);
            buckets.try_emplace(bucket_name, typename TBuckets::mapped_type{});
        }

        // Validate that all referenced transformations exist and have at least one version.
        static bool _validateConnectorTransformations(
            const Connector & connector,
            const _TransformationBuckets & transformations)
        {
            for(const Dimension & dimension : connector.dimensions())
            {
                for(const auto & transformation : dimension.transformations())
                {
                    if(transformation.name().empty())
                    {
                        spdlog::error("Connector `{}` contains unnamed transformation", connector.name());
                        return false;
                    }

                    const auto transformation_it = transformations.find(transformation.name());
                    if(transformation_it == transformations.end() || transformation_it->second.empty())
                    {
                        spdlog::error(
                            "Cannot find transformation `{}` used in connector `{}`",
                            transformation.name(),
                            connector.name());
                        return false;
                    }
                }
            }

            return true;
        }

        // Validate that optional connector condition exists and has at least one version.
        static bool _validateConnectorCondition(
            const Connector & connector,
            const _ConditionBuckets & conditions)
        {
            if(connector.condition_name().empty())
            {
                return true;
            }

            const std::string & condition_name = connector.condition_name();
            const auto condition_it = conditions.find(condition_name);
            if(condition_it != conditions.end() && !condition_it->second.empty())
            {
                return true;
            }

            spdlog::error(
                "Cannot find condition `{}` used in connector `{}`",
                condition_name,
                connector.name());
            return false;
        }

        // Parse canonical decimal slot id used in binding maps.
        static std::optional<std::uint32_t> _parseSlotId(const std::string & slot_str)
        {
            std::uint32_t slot_id = 0;
            const auto [ptr, ec] =
                std::from_chars(slot_str.data(), slot_str.data() + slot_str.size(), slot_id, 10);
            if(ec != std::errc{} || ptr != (slot_str.data() + slot_str.size()))
            {
                return std::nullopt;
            }

            return slot_id;
        }

        // Resolve connector definition by name.
        // `pending_connector` is the connector currently being added and not yet persisted,
        // so recursion can still resolve references to the in-flight definition.
        static const Connector * _resolveConnector(
            const std::string & connector_name,
            const Connector & pending_connector,
            const absl::flat_hash_map<std::string, chain::Address> & newest_connector,
            const _ConnectorBuckets & connectors)
        {
            if(connector_name == pending_connector.name())
            {
                return &pending_connector;
            }

            auto newest_it = newest_connector.find(connector_name);
            if(newest_it == newest_connector.end())
            {
                return nullptr;
            }

            auto bucket_it = connectors.find(connector_name);
            if(bucket_it == connectors.end())
            {
                return nullptr;
            }

            auto connector_it = bucket_it->second.find(newest_it->second);
            if(connector_it == bucket_it->second.end())
            {
                return nullptr;
            }

            return &connector_it->second.connector();
        }

        // Compute connector open-slot count with cycle detection and memoization.
        // `root_connector_name` is threaded through recursion only for top-level error context.
        static std::optional<std::uint32_t> _computeOpenSlots(
            const std::string & connector_name,
            const std::string & root_connector_name,
            const Connector & pending_connector,
            const absl::flat_hash_map<std::string, chain::Address> & newest_connector,
            const _ConnectorBuckets & connectors,
            absl::flat_hash_map<std::string, _VisitState> & visit_state,
            absl::flat_hash_map<std::string, std::uint32_t> & open_slots_cache)
        {
            if(const auto cache_it = open_slots_cache.find(connector_name); cache_it != open_slots_cache.end())
            {
                return cache_it->second;
            }

            if(const auto state_it = visit_state.find(connector_name);
                state_it != visit_state.end() && state_it->second == _VisitState::VISITING)
            {
                spdlog::error("Connector dependency cycle detected at `{}`", connector_name);
                return std::nullopt;
            }

            const Connector * connector =
                _resolveConnector(connector_name, pending_connector, newest_connector, connectors);
            if(connector == nullptr)
            {
                spdlog::error(
                    "Cannot resolve connector definition `{}` while validating `{}`",
                    connector_name,
                    root_connector_name);
                return std::nullopt;
            }

            visit_state[connector_name] = _VisitState::VISITING;

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

                if(_resolveConnector(composite_name, pending_connector, newest_connector, connectors) == nullptr)
                {
                    spdlog::error(
                        "Cannot resolve composite connector `{}` in connector `{}` dim {}",
                        composite_name,
                        connector_name,
                        dim_id);
                    return std::nullopt;
                }

                const auto child_open_slots_opt = _computeOpenSlots(
                    composite_name,
                    root_connector_name,
                    pending_connector,
                    newest_connector,
                    connectors,
                    visit_state,
                    open_slots_cache);
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

                    const auto slot_id_opt = _parseSlotId(slot_str);
                    if(!slot_id_opt)
                    {
                        spdlog::error(
                            "Connector `{}` has invalid binding slot id `{}` at dim {}",
                            connector_name,
                            slot_str,
                            dim_id);
                        return std::nullopt;
                    }
                    const std::uint32_t slot_id = *slot_id_opt;

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

                    if(_resolveConnector(binding_target, pending_connector, newest_connector, connectors) == nullptr)
                    {
                        spdlog::error(
                            "Cannot resolve binding target `{}` in connector `{}` dim {} slot {}",
                            binding_target,
                            connector_name,
                            dim_id,
                            slot_id);
                        return std::nullopt;
                    }

                    const auto target_open_slots_opt = _computeOpenSlots(
                        binding_target,
                        root_connector_name,
                        pending_connector,
                        newest_connector,
                        connectors,
                        visit_state,
                        open_slots_cache);
                    if(!target_open_slots_opt)
                    {
                        return std::nullopt;
                    }

                    open_slots += static_cast<std::uint64_t>(*target_open_slots_opt);
                }

                open_slots -= static_cast<std::uint64_t>(bound_slot_ids.size());
            }

            if(open_slots > std::numeric_limits<std::uint32_t>::max())
            {
                spdlog::error("Connector `{}` exceeds supported open slots range", connector_name);
                return std::nullopt;
            }

            visit_state[connector_name] = _VisitState::DONE;
            open_slots_cache.emplace(connector_name, static_cast<std::uint32_t>(open_slots));
            return static_cast<std::uint32_t>(open_slots);
        }

        // Resolve produced scalar entries with cycle detection and memoization.
        // `root_connector_name` is threaded through recursion only for top-level error context.
        static std::optional<dcn::chain::ResolvedScalarEntries> _computeScalarEntries(
            const std::string & connector_name,
            const std::string & root_connector_name,
            const Connector & pending_connector,
            const absl::flat_hash_map<std::string, chain::Address> & newest_connector,
            const _ConnectorBuckets & connectors,
            absl::flat_hash_map<std::string, _VisitState> & scalar_visit_state,
            absl::flat_hash_map<std::string, dcn::chain::ResolvedScalarEntries> & scalar_entries_cache)
        {
            if(const auto cache_it = scalar_entries_cache.find(connector_name); cache_it != scalar_entries_cache.end())
            {
                return cache_it->second;
            }

            if(const auto state_it = scalar_visit_state.find(connector_name);
                state_it != scalar_visit_state.end() &&
                state_it->second == _VisitState::VISITING)
            {
                spdlog::error(
                    "Connector dependency cycle detected while resolving scalars at `{}`",
                    connector_name);
                return std::nullopt;
            }

            const Connector * connector =
                _resolveConnector(connector_name, pending_connector, newest_connector, connectors);
            if(connector == nullptr)
            {
                spdlog::error(
                    "Cannot resolve connector definition `{}` while resolving scalars for `{}`",
                    connector_name,
                    root_connector_name);
                return std::nullopt;
            }

            scalar_visit_state[connector_name] = _VisitState::VISITING;

            dcn::chain::ResolvedScalarEntries connector_scalar_entries;
            for(std::uint32_t dim_id = 0; dim_id < static_cast<std::uint32_t>(connector->dimensions_size()); ++dim_id)
            {
                const Dimension & dimension = connector->dimensions(static_cast<int>(dim_id));
                const std::string & composite_name = dimension.composite();
                const evmc::bytes32 dim_path_hash = dcn::chain::dimPathHash(dim_id);
                if(composite_name.empty())
                {
                    if(dimension.bindings_size() != 0)
                    {
                        spdlog::error(
                            "Connector `{}` has bindings on scalar dimension {} while resolving scalar slots",
                            connector_name,
                            dim_id);
                        return std::nullopt;
                    }

                    const evmc::bytes32 scalar_hash = dcn::chain::keccakString(connector_name);
                    connector_scalar_entries.hash_entries.push_back(dcn::chain::ScalarHashEntry{
                        .scalar_hash = scalar_hash,
                        .path_hash = dim_path_hash
                    });
                    connector_scalar_entries.display_entries.push_back(ScalarLabel{
                        .scalar = connector_name,
                        // Tail-only path rule: local scalar tail is this dimension id.
                        .path_hash = dim_path_hash,
                        .tail_id = dim_id
                    });
                    continue;
                }

                auto child_scalar_entries_opt = _computeScalarEntries(
                    composite_name,
                    root_connector_name,
                    pending_connector,
                    newest_connector,
                    connectors,
                    scalar_visit_state,
                    scalar_entries_cache);
                if(!child_scalar_entries_opt)
                {
                    return std::nullopt;
                }
                const dcn::chain::ResolvedScalarEntries & child_scalar_entries = *child_scalar_entries_opt;

                if(child_scalar_entries.hash_entries.size() != child_scalar_entries.display_entries.size())
                {
                    spdlog::error(
                        "Connector `{}` child `{}` has inconsistent scalar-entry cache",
                        connector_name,
                        composite_name);
                    return std::nullopt;
                }

                if(
                    child_scalar_entries.display_entries.size() >
                    static_cast<std::size_t>(std::numeric_limits<std::uint32_t>::max()))
                {
                    spdlog::error(
                        "Connector `{}` child `{}` scalar labels exceed uint32 range",
                        connector_name,
                        composite_name);
                    return std::nullopt;
                }

                absl::flat_hash_map<std::uint32_t, std::string> binding_by_slot;
                for(const auto & [slot_str, binding_target] : dimension.bindings())
                {
                    if(binding_target.empty())
                    {
                        spdlog::error(
                            "Connector `{}` has empty binding target at dim {} slot `{}` while resolving scalar slots",
                            connector_name,
                            dim_id,
                            slot_str);
                        return std::nullopt;
                    }

                    const auto slot_id_opt = _parseSlotId(slot_str);
                    if(!slot_id_opt)
                    {
                        spdlog::error(
                            "Connector `{}` has invalid binding slot id `{}` at dim {} while resolving scalar slots",
                            connector_name,
                            slot_str,
                            dim_id);
                        return std::nullopt;
                    }
                    const std::uint32_t slot_id = *slot_id_opt;

                    if(slot_id >= child_scalar_entries.display_entries.size())
                    {
                        spdlog::error(
                            "Connector `{}` binding slot {} is out of range for child `{}` while resolving scalar labels",
                            connector_name,
                            slot_id,
                            composite_name);
                        return std::nullopt;
                    }

                    if(!binding_by_slot.try_emplace(slot_id, binding_target).second)
                    {
                        spdlog::error(
                            "Connector `{}` has duplicate binding slot id {} at dim {} while resolving scalar slots",
                            connector_name,
                            slot_id,
                            dim_id);
                        return std::nullopt;
                    }
                }

                for(std::size_t slot_index = 0; slot_index < child_scalar_entries.display_entries.size(); ++slot_index)
                {
                    const std::uint32_t slot_id = static_cast<std::uint32_t>(slot_index);

                    const auto binding_it = binding_by_slot.find(slot_id);
                    if(binding_it == binding_by_slot.end())
                    {
                        // Tail-only path rule: do not prefix child tail by parent path.
                        connector_scalar_entries.hash_entries.push_back(child_scalar_entries.hash_entries[slot_index]);
                        connector_scalar_entries.display_entries.push_back(
                            child_scalar_entries.display_entries[slot_index]);
                        continue;
                    }

                    auto binding_scalar_entries_opt = _computeScalarEntries(
                        binding_it->second,
                        root_connector_name,
                        pending_connector,
                        newest_connector,
                        connectors,
                        scalar_visit_state,
                        scalar_entries_cache);
                    if(!binding_scalar_entries_opt)
                    {
                        return std::nullopt;
                    }
                    const dcn::chain::ResolvedScalarEntries & binding_scalar_entries =
                        *binding_scalar_entries_opt;

                    if(binding_scalar_entries.hash_entries.size() != binding_scalar_entries.display_entries.size())
                    {
                        spdlog::error(
                            "Connector `{}` binding `{}` has inconsistent scalar-entry cache",
                            connector_name,
                            binding_it->second);
                        return std::nullopt;
                    }

                    for(std::size_t i = 0; i < binding_scalar_entries.display_entries.size(); ++i)
                    {
                        // Tail-only path rule: bound scalar keeps its own tail path.
                        connector_scalar_entries.hash_entries.push_back(binding_scalar_entries.hash_entries[i]);
                        connector_scalar_entries.display_entries.push_back(
                            binding_scalar_entries.display_entries[i]);
                    }
                }
            }

            scalar_visit_state[connector_name] = _VisitState::DONE;
            auto [cache_it, inserted] =
                scalar_entries_cache.try_emplace(connector_name, std::move(connector_scalar_entries));
            (void)inserted;
            return cache_it->second;
        }

        // Resolve scalar entries for the connector currently being added.
        static std::optional<dcn::chain::ResolvedScalarEntries> _resolveRootScalarEntries(
            const Connector & root_connector,
            const absl::flat_hash_map<std::string, chain::Address> & newest_connector,
            const _ConnectorBuckets & connectors)
        {
            absl::flat_hash_map<std::string, _VisitState> scalar_visit_state;
            absl::flat_hash_map<std::string, dcn::chain::ResolvedScalarEntries> scalar_entries_cache;
            auto root_scalar_entries_opt = _computeScalarEntries(
                root_connector.name(),
                root_connector.name(),
                root_connector,
                newest_connector,
                connectors,
                scalar_visit_state,
                scalar_entries_cache);

            if(
                !root_scalar_entries_opt ||
                root_scalar_entries_opt->hash_entries.empty() ||
                root_scalar_entries_opt->hash_entries.size() !=
                    root_scalar_entries_opt->display_entries.size())
            {
                return std::nullopt;
            }

            return root_scalar_entries_opt;
        }

        // Validate slot/binding graph for the connector currently being added.
        static bool _validateConnectorOpenSlots(
            const Connector & root_connector,
            const absl::flat_hash_map<std::string, chain::Address> & newest_connector,
            const _ConnectorBuckets & connectors)
        {
            absl::flat_hash_map<std::string, _VisitState> visit_state;
            absl::flat_hash_map<std::string, std::uint32_t> open_slots_cache;

            return _computeOpenSlots(
                       root_connector.name(),
                       root_connector.name(),
                       root_connector,
                       newest_connector,
                       connectors,
                       visit_state,
                       open_slots_cache)
                .has_value();
        }

        // Sort labels into a stable canonical order before storing/ comparing by format hash.
        static std::vector<ScalarLabel> _canonicalizeScalarLabels(const std::vector<ScalarLabel> & labels)
        {
            std::vector<ScalarLabel> canonical_scalar_labels;
            canonical_scalar_labels.reserve(labels.size());
            for(const ScalarLabel & scalar_label : labels)
            {
                canonical_scalar_labels.push_back(ScalarLabel{
                    .scalar = scalar_label.scalar,
                    .path_hash = scalar_label.path_hash,
                    .tail_id = scalar_label.tail_id
                });
            }

            std::sort(
                canonical_scalar_labels.begin(),
                canonical_scalar_labels.end(),
                [](const ScalarLabel & lhs, const ScalarLabel & rhs)
                {
                    if(dcn::chain::lessBytes32(lhs.path_hash, rhs.path_hash))
                    {
                        return true;
                    }
                    if(dcn::chain::lessBytes32(rhs.path_hash, lhs.path_hash))
                    {
                        return false;
                    }
                    if(lhs.scalar != rhs.scalar)
                    {
                        return lhs.scalar < rhs.scalar;
                    }
                    return lhs.tail_id < rhs.tail_id;
                });

            return canonical_scalar_labels;
        }

        // Byte-accurate equality check for canonical scalar label sets.
        static bool _scalarLabelsEqual(
            const std::vector<ScalarLabel> & lhs,
            const std::vector<ScalarLabel> & rhs)
        {
            if(lhs.size() != rhs.size())
            {
                return false;
            }

            for(std::size_t i = 0; i < lhs.size(); ++i)
            {
                if(
                    lhs[i].scalar != rhs[i].scalar ||
                    !dcn::chain::equalBytes32(lhs[i].path_hash, rhs[i].path_hash) ||
                    lhs[i].tail_id != rhs[i].tail_id)
                {
                    return false;
                }
            }

            return true;
        }

        // Parse owner hex string once and centralize parse error logging.
        static std::optional<chain::Address> _tryParseOwnerAddress(const std::string & owner)
        {
            const std::optional<chain::Address> owner_res = evmc::from_hex<chain::Address>(owner);
            if(!owner_res)
            {
                spdlog::error("Failed to parse owner address");
                return std::nullopt;
            }

            return *owner_res;
        }
    }

    Registry::Registry(asio::io_context & io_context)
        : _strand(asio::make_strand(io_context))
    {
    }

    asio::awaitable<bool> Registry::addConnector(chain::Address address, ConnectorRecord record)
    {
        const Connector & connector = record.connector();
        const std::string connector_name = connector.name();
        if(connector_name.empty())
        {
            spdlog::error("Connector name is empty");
            co_return false;
        }

        co_await utils::ensureOnStrand(_strand);
        _ensureBucket("Connector", connector_name, _connectors);

        if(_connectors.at(connector_name).contains(address))
        {
            spdlog::error("Connector `{}` of this signature already exists", connector_name);
            co_return false;
        }

        if(connector.dimensions_size() <= 0)
        {
            spdlog::error("Connector `{}` has zero dimensions", connector_name);
            co_return false;
        }

        if(!_validateConnectorTransformations(connector, _transformations))
        {
            co_return false;
        }

        if(!_validateConnectorCondition(connector, _conditions))
        {
            co_return false;
        }

        if(!_validateConnectorOpenSlots(connector, _newest_connector, _connectors))
        {
            spdlog::error("Connector `{}` failed slot/binding validation", connector_name);
            co_return false;
        }

        const auto root_scalar_entries_opt =
            _resolveRootScalarEntries(connector, _newest_connector, _connectors);
        if(!root_scalar_entries_opt)
        {
            spdlog::error(
                "Connector `{}` produced invalid scalar entries while computing format hash",
                connector_name);
            co_return false;
        }

        // Format hash is a multiset hash over produced scalar-tail-path labels.
        // Do not deduplicate: multiplicity must affect the final hash.
        const evmc::bytes32 format_hash =
            dcn::chain::computeFormatHash(root_scalar_entries_opt->hash_entries);
        const std::vector<ScalarLabel> canonical_scalar_labels =
            _canonicalizeScalarLabels(root_scalar_entries_opt->display_entries);

        const auto owner_opt = _tryParseOwnerAddress(record.owner());
        if(!owner_opt)
        {
            co_return false;
        }
        const chain::Address owner = *owner_opt;

        _owned_connectors[owner].emplace(connector_name);
        _newest_connector[connector_name] = address;
        _connectors.at(connector_name).try_emplace(address, std::move(record));
        _format_by_connector[address] = format_hash;
        _connectors_by_format[format_hash].emplace(address);

        const auto scalar_labels_it = _scalar_labels_by_format.find(format_hash);
        if(scalar_labels_it == _scalar_labels_by_format.end())
        {
            _scalar_labels_by_format.emplace(format_hash, canonical_scalar_labels);
        }
        else if(!_scalarLabelsEqual(scalar_labels_it->second, canonical_scalar_labels))
        {
            spdlog::warn(
                "Different scalar-label multisets encountered for the same format hash; keeping first inserted representation");
        }

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

    asio::awaitable<std::optional<std::string>> Registry::getConnectorName(const chain::Address & address) const
    {
        co_await utils::ensureOnStrand(_strand);

        for(const auto & [name, bucket] : _connectors)
        {
            if(bucket.contains(address))
            {
                co_return name;
            }
        }

        co_return std::nullopt;
    }

    asio::awaitable<std::optional<evmc::bytes32>> Registry::getNewestFormatHash(const std::string& name) const
    {
        co_await utils::ensureOnStrand(_strand);

        if(_newest_connector.contains(name) == false)
        {
            co_return std::nullopt;
        }

        co_return co_await getFormatHash(name, _newest_connector.at(name));
    }

    asio::awaitable<std::optional<evmc::bytes32>> Registry::getFormatHash(const std::string& name, const chain::Address & address) const
    {
        co_await utils::ensureOnStrand(_strand);

        auto bucket_it = _connectors.find(name);
        if(bucket_it == _connectors.end() || bucket_it->second.contains(address) == false)
        {
            co_return std::nullopt;
        }

        auto format_it = _format_by_connector.find(address);
        if(format_it == _format_by_connector.end())
        {
            co_return std::nullopt;
        }

        co_return format_it->second;
    }

    asio::awaitable<absl::flat_hash_set<chain::Address>> Registry::getConnectorsByFormatHash(const evmc::bytes32 & format_hash) const
    {
        co_await utils::ensureOnStrand(_strand);

        if(_connectors_by_format.contains(format_hash) == false)
        {
            co_return absl::flat_hash_set<chain::Address>{};
        }

        co_return _connectors_by_format.at(format_hash);
    }

    asio::awaitable<std::optional<std::vector<ScalarLabel>>> Registry::getScalarLabelsByFormatHash(const evmc::bytes32 & format_hash) const
    {
        co_await utils::ensureOnStrand(_strand);

        const auto it = _scalar_labels_by_format.find(format_hash);
        if(it == _scalar_labels_by_format.end())
        {
            co_return std::nullopt;
        }

        co_return it->second;
    }

    asio::awaitable<bool> Registry::addTransformation(chain::Address address, TransformationRecord record)
    {
        const std::string transformation_name = record.transformation().name();
        if(transformation_name.empty())
        {
            spdlog::error("Transformation name is empty");
            co_return false;
        }

        co_await utils::ensureOnStrand(_strand);
        _ensureBucket("Transformation", transformation_name, _transformations);

        if(_transformations.at(transformation_name).contains(address))
        {
            spdlog::error("Transformation `{}` of this signature already exists", transformation_name);
            co_return false;
        }

        const auto owner_opt = _tryParseOwnerAddress(record.owner());
        if(!owner_opt)
        {
            co_return false;
        }
        const chain::Address owner = *owner_opt;

        _owned_transformations[owner].emplace(transformation_name);
        _newest_transformation[transformation_name] = address;
        _transformations.at(transformation_name).try_emplace(std::move(address), std::move(record));

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
        const std::string condition_name = record.condition().name();
        if(condition_name.empty())
        {
            spdlog::error("Condition name is empty");
            co_return false;
        }

        co_await utils::ensureOnStrand(_strand);
        _ensureBucket("Condition", condition_name, _conditions);

        if(_conditions.at(condition_name).contains(address))
        {
            spdlog::error("Condition `{}` of this signature already exists", condition_name);
            co_return false;
        }

        const auto owner_opt = _tryParseOwnerAddress(record.owner());
        if(!owner_opt)
        {
            co_return false;
        }
        const chain::Address owner = *owner_opt;

        _owned_conditions[owner].emplace(condition_name);
        _newest_condition[condition_name] = address;
        _conditions.at(condition_name).try_emplace(std::move(address), std::move(record));

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
