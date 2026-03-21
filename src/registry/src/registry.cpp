#include "registry.hpp"
#include <algorithm>
#include <charconv>
#include <cstdint>
#include <cstring>
#include <limits>
#include <system_error>
#include <vector>
#include <format>

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

        // Shared connector-graph references used while resolving recursive dependencies.
        struct _ConnectorGraphContext
        {
            const Connector & pending_connector;
            const absl::flat_hash_map<std::string, chain::Address> & newest_connector;
            const _ConnectorBuckets & connectors;
        };

        // Traversal state for recursive open-slot computation.
        struct _OpenSlotsContext
        {
            const std::string & root_connector_name;
            const _ConnectorGraphContext & graph;
            absl::flat_hash_map<std::string, _VisitState> & visit_state;
            absl::flat_hash_map<std::string, std::uint32_t> & open_slots_cache;
        };

        // Traversal state for recursive scalar-entry computation.
        struct _ScalarEntriesContext
        {
            const std::string & root_connector_name;
            const _ConnectorGraphContext & graph;
            absl::flat_hash_map<std::string, _VisitState> & scalar_visit_state;
            absl::flat_hash_map<std::string, dcn::chain::ResolvedScalarEntries> & scalar_entries_cache;
        };

        // Aggregated slot impact produced by applying bindings to a child connector.
        struct _OpenSlotBindingEffect
        {
            std::uint64_t added_open_slots = 0;
            std::size_t bound_slot_count = 0;
        };

        // Compact scalar-label statistics used for warning diagnostics.
        struct _ScalarLabelSummary
        {
            std::size_t labels_count = 0;
            std::size_t unique_scalars_count = 0;
            std::size_t unique_scalar_tail_pairs_count = 0;
        };

        // Lazily create a named bucket when first needed.
        template <typename TBuckets>
        static void _ensureBucket(
            const char * bucket_type_name,
            const std::string & bucket_name,
            TBuckets & buckets);

        // Validate that all transformations referenced by a connector are resolvable.
        static bool _validateConnectorTransformations(
            const Connector & connector,
            const _TransformationBuckets & transformations);

        // Validate that the optional condition referenced by a connector is resolvable.
        static bool _validateConnectorCondition(
            const Connector & connector,
            const _ConditionBuckets & conditions);

        // Resolve connector definition from the in-flight connector plus persisted registry state.
        static const Connector * _resolveConnector(
            const std::string & connector_name,
            const _ConnectorGraphContext & graph_context);

        // Lookup format hash for an existing (name, address) connector registration.
        static std::optional<evmc::bytes32> _lookupConnectorFormatHash(
            const std::string & name,
            const chain::Address & address,
            const _ConnectorBuckets & connectors,
            const absl::flat_hash_map<chain::Address, evmc::bytes32> & format_by_connector);

        // Return sorted connector addresses for a format hash, rebuilding cache when dirty.
        static const std::vector<chain::Address> & _getSortedFormatConnectors(
            const evmc::bytes32 & format_hash,
            const absl::flat_hash_map<evmc::bytes32, absl::flat_hash_set<chain::Address>> & connectors_by_format,
            absl::flat_hash_map<evmc::bytes32, std::vector<chain::Address>> & sorted_connectors_by_format,
            absl::flat_hash_set<evmc::bytes32> & dirty_sorted_connectors_by_format);

        // Parse a decimal slot id and reject malformed/overflowing values.
        static std::optional<std::uint32_t> _parseSlotId(const std::string & slot_str);

        // Compute open slots for a connector with memoization and cycle detection.
        static std::optional<std::uint32_t> _computeOpenSlots(
            const std::string & connector_name,
            _OpenSlotsContext & context);

        // Compute produced scalar entries for a connector with memoization and cycle detection.
        static std::optional<dcn::chain::ResolvedScalarEntries> _computeScalarEntries(
            const std::string & connector_name,
            _ScalarEntriesContext & context);

        // Compute how bindings modify the open-slot contribution of a composite dimension.
        static std::optional<_OpenSlotBindingEffect> _computeOpenSlotBindingEffect(
            const std::string & connector_name,
            const std::string & composite_name,
            const Dimension & dimension,
            std::uint32_t dim_id,
            std::uint32_t child_open_slots,
            _OpenSlotsContext & context);

        // Append a local scalar dimension entry for the current connector.
        static void _appendLocalScalarEntry(
            const std::string & connector_name,
            std::uint32_t dim_id,
            dcn::chain::ResolvedScalarEntries & connector_scalar_entries);

        // Validate child scalar-entry cache consistency before merging.
        static bool _validateChildScalarEntries(
            const std::string & connector_name,
            const std::string & composite_name,
            const dcn::chain::ResolvedScalarEntries & child_scalar_entries);

        // Validate bound-target scalar-entry cache consistency before merging.
        static bool _validateBindingScalarEntries(
            const std::string & connector_name,
            const std::string & binding_target,
            const dcn::chain::ResolvedScalarEntries & binding_scalar_entries);

        // Build slot->binding map for scalar resolution and validate slot ids.
        static std::optional<absl::flat_hash_map<std::uint32_t, std::string>>
        _buildBindingBySlotForScalarResolution(
            const std::string & connector_name,
            const std::string & composite_name,
            const Dimension & dimension,
            std::uint32_t dim_id,
            std::size_t child_slot_count);

        // Resolve and append scalar entries produced by a binding target connector.
        static bool _appendBindingScalarEntries(
            const std::string & connector_name,
            const std::string & binding_target,
            _ScalarEntriesContext & context,
            dcn::chain::ResolvedScalarEntries & connector_scalar_entries);

        // Resolve all scalar entries for the root connector being added.
        static std::optional<dcn::chain::ResolvedScalarEntries> _resolveRootScalarEntries(
            const Connector & root_connector,
            const absl::flat_hash_map<std::string, chain::Address> & newest_connector,
            const _ConnectorBuckets & connectors);

        // Validate open-slot consistency for the root connector being added.
        static bool _validateConnectorOpenSlots(
            const Connector & root_connector,
            const absl::flat_hash_map<std::string, chain::Address> & newest_connector,
            const _ConnectorBuckets & connectors);

        // Return a deterministically ordered copy of scalar labels.
        static std::vector<ScalarLabel> _canonicalizeScalarLabels(const std::vector<ScalarLabel> & labels);

        // Compare canonical scalar-label collections for byte-accurate equality.
        static bool _scalarLabelsEqual(
            const std::vector<ScalarLabel> & lhs,
            const std::vector<ScalarLabel> & rhs);

        // Produce compact scalar-label metrics for warning logs.
        static _ScalarLabelSummary _summarizeScalarLabels(const std::vector<ScalarLabel> & labels);

        // Parse connector owner string into an address if valid.
        static std::optional<chain::Address> _tryParseOwnerAddress(const std::string & owner);

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

            spdlog::debug("{} bucket `{}` does not exist, creating new one ... ", bucket_type_name, bucket_name);
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

        // Lookup format hash for a concrete (name, address) connector registration.
        // Caller must ensure synchronization (strand) before invoking.
        static std::optional<evmc::bytes32> _lookupConnectorFormatHash(
            const std::string & name,
            const chain::Address & address,
            const _ConnectorBuckets & connectors,
            const absl::flat_hash_map<chain::Address, evmc::bytes32> & format_by_connector)
        {
            auto bucket_it = connectors.find(name);
            if(bucket_it == connectors.end() || bucket_it->second.contains(address) == false)
            {
                return std::nullopt;
            }

            auto format_it = format_by_connector.find(address);
            if(format_it == format_by_connector.end())
            {
                return std::nullopt;
            }

            return format_it->second;
        }

        // Keep paging deterministic while avoiding O(n) insertion into a sorted vector on every add.
        // Source of truth is `_connectors_by_format`; this function lazily rebuilds sorted cache.
        static const std::vector<chain::Address> & _getSortedFormatConnectors(
            const evmc::bytes32 & format_hash,
            const absl::flat_hash_map<evmc::bytes32, absl::flat_hash_set<chain::Address>> & connectors_by_format,
            absl::flat_hash_map<evmc::bytes32, std::vector<chain::Address>> & sorted_connectors_by_format,
            absl::flat_hash_set<evmc::bytes32> & dirty_sorted_connectors_by_format)
        {
            const auto connectors_it = connectors_by_format.find(format_hash);
            if(connectors_it == connectors_by_format.end())
            {
                static const std::vector<chain::Address> empty_addresses;
                return empty_addresses;
            }

            auto [sorted_it, inserted] = sorted_connectors_by_format.try_emplace(format_hash);
            std::vector<chain::Address> & sorted_connectors = sorted_it->second;
            const bool rebuild_cache =
                inserted ||
                dirty_sorted_connectors_by_format.contains(format_hash) ||
                sorted_connectors.size() != connectors_it->second.size();
            if(rebuild_cache)
            {
                sorted_connectors.clear();
                sorted_connectors.reserve(connectors_it->second.size());
                for(const chain::Address & connector_address : connectors_it->second)
                {
                    sorted_connectors.push_back(connector_address);
                }

                std::sort(sorted_connectors.begin(), sorted_connectors.end());
                dirty_sorted_connectors_by_format.erase(format_hash);
            }

            return sorted_connectors;
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
            const _ConnectorGraphContext & graph_context)
        {
            if(connector_name == graph_context.pending_connector.name())
            {
                return &graph_context.pending_connector;
            }

            auto newest_it = graph_context.newest_connector.find(connector_name);
            if(newest_it == graph_context.newest_connector.end())
            {
                return nullptr;
            }

            auto bucket_it = graph_context.connectors.find(connector_name);
            if(bucket_it == graph_context.connectors.end())
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

        static std::optional<_OpenSlotBindingEffect> _computeOpenSlotBindingEffect(
            const std::string & connector_name,
            const std::string & composite_name,
            const Dimension & dimension,
            std::uint32_t dim_id,
            std::uint32_t child_open_slots,
            _OpenSlotsContext & context)
        {
            absl::flat_hash_set<std::uint32_t> bound_slot_ids;
            _OpenSlotBindingEffect effect{};
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

                if(_resolveConnector(binding_target, context.graph) == nullptr)
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
                    context);
                if(!target_open_slots_opt)
                {
                    return std::nullopt;
                }

                effect.added_open_slots += static_cast<std::uint64_t>(*target_open_slots_opt);
            }

            effect.bound_slot_count = bound_slot_ids.size();
            return effect;
        }

        static void _appendLocalScalarEntry(
            const std::string & connector_name,
            std::uint32_t dim_id,
            dcn::chain::ResolvedScalarEntries & connector_scalar_entries)
        {
            const evmc::bytes32 dim_path_hash = dcn::chain::dimPathHash(dim_id);
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
        }

        static bool _validateChildScalarEntries(
            const std::string & connector_name,
            const std::string & composite_name,
            const dcn::chain::ResolvedScalarEntries & child_scalar_entries)
        {
            if(child_scalar_entries.hash_entries.size() != child_scalar_entries.display_entries.size())
            {
                spdlog::error(
                    "Connector `{}` child `{}` has inconsistent scalar-entry cache",
                    connector_name,
                    composite_name);
                return false;
            }

            if(
                child_scalar_entries.display_entries.size() >
                static_cast<std::size_t>(std::numeric_limits<std::uint32_t>::max()))
            {
                spdlog::error(
                    "Connector `{}` child `{}` scalar labels exceed uint32 range",
                    connector_name,
                    composite_name);
                return false;
            }

            return true;
        }

        static bool _validateBindingScalarEntries(
            const std::string & connector_name,
            const std::string & binding_target,
            const dcn::chain::ResolvedScalarEntries & binding_scalar_entries)
        {
            if(binding_scalar_entries.hash_entries.size() != binding_scalar_entries.display_entries.size())
            {
                spdlog::error(
                    "Connector `{}` binding `{}` has inconsistent scalar-entry cache",
                    connector_name,
                    binding_target);
                return false;
            }

            return true;
        }

        static std::optional<absl::flat_hash_map<std::uint32_t, std::string>>
        _buildBindingBySlotForScalarResolution(
            const std::string & connector_name,
            const std::string & composite_name,
            const Dimension & dimension,
            std::uint32_t dim_id,
            std::size_t child_slot_count)
        {
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

                if(slot_id >= child_slot_count)
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

            return binding_by_slot;
        }

        static bool _appendBindingScalarEntries(
            const std::string & connector_name,
            const std::string & binding_target,
            _ScalarEntriesContext & context,
            dcn::chain::ResolvedScalarEntries & connector_scalar_entries)
        {
            auto binding_scalar_entries_opt = _computeScalarEntries(
                binding_target,
                context);
            if(!binding_scalar_entries_opt)
            {
                return false;
            }
            const dcn::chain::ResolvedScalarEntries & binding_scalar_entries =
                *binding_scalar_entries_opt;

            if(!_validateBindingScalarEntries(connector_name, binding_target, binding_scalar_entries))
            {
                return false;
            }

            for(std::size_t i = 0; i < binding_scalar_entries.display_entries.size(); ++i)
            {
                // Tail-only path rule: bound scalar keeps its own tail path.
                connector_scalar_entries.hash_entries.push_back(binding_scalar_entries.hash_entries[i]);
                connector_scalar_entries.display_entries.push_back(
                    binding_scalar_entries.display_entries[i]);
            }

            return true;
        }

        // Compute connector open-slot count with cycle detection and memoization.
        // `root_connector_name` is threaded through recursion only for top-level error context.
        static std::optional<std::uint32_t> _computeOpenSlots(
            const std::string & connector_name,
            _OpenSlotsContext & context)
        {
            if(const auto cache_it = context.open_slots_cache.find(connector_name); cache_it != context.open_slots_cache.end())
            {
                return cache_it->second;
            }

            if(const auto state_it = context.visit_state.find(connector_name);
                state_it != context.visit_state.end() && state_it->second == _VisitState::VISITING)
            {
                spdlog::error("Connector dependency cycle detected at `{}`", connector_name);
                return std::nullopt;
            }

            const Connector * connector =
                _resolveConnector(connector_name, context.graph);
            if(connector == nullptr)
            {
                spdlog::error(
                    "Cannot resolve connector definition `{}` while validating `{}`",
                    connector_name,
                    context.root_connector_name);
                return std::nullopt;
            }

            context.visit_state[connector_name] = _VisitState::VISITING;

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

                if(_resolveConnector(composite_name, context.graph) == nullptr)
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
                    context);
                if(!child_open_slots_opt)
                {
                    return std::nullopt;
                }
                const std::uint32_t child_open_slots = *child_open_slots_opt;
                open_slots += child_open_slots;

                const auto binding_effect_opt = _computeOpenSlotBindingEffect(
                    connector_name,
                    composite_name,
                    dimension,
                    dim_id,
                    child_open_slots,
                    context);
                if(!binding_effect_opt)
                {
                    return std::nullopt;
                }

                open_slots += binding_effect_opt->added_open_slots;
                open_slots -= static_cast<std::uint64_t>(binding_effect_opt->bound_slot_count);
            }

            if(open_slots > std::numeric_limits<std::uint32_t>::max())
            {
                spdlog::error("Connector `{}` exceeds supported open slots range", connector_name);
                return std::nullopt;
            }

            context.visit_state[connector_name] = _VisitState::DONE;
            context.open_slots_cache.emplace(connector_name, static_cast<std::uint32_t>(open_slots));
            return static_cast<std::uint32_t>(open_slots);
        }

        // Resolve produced scalar entries with cycle detection and memoization.
        // `root_connector_name` is threaded through recursion only for top-level error context.
        static std::optional<dcn::chain::ResolvedScalarEntries> _computeScalarEntries(
            const std::string & connector_name,
            _ScalarEntriesContext & context)
        {
            if(const auto cache_it = context.scalar_entries_cache.find(connector_name);
                cache_it != context.scalar_entries_cache.end())
            {
                return cache_it->second;
            }

            if(const auto state_it = context.scalar_visit_state.find(connector_name);
                state_it != context.scalar_visit_state.end() &&
                state_it->second == _VisitState::VISITING)
            {
                spdlog::error(
                    "Connector dependency cycle detected while resolving scalars at `{}`",
                    connector_name);
                return std::nullopt;
            }

            const Connector * connector =
                _resolveConnector(connector_name, context.graph);
            if(connector == nullptr)
            {
                spdlog::error(
                    "Cannot resolve connector definition `{}` while resolving scalars for `{}`",
                    connector_name,
                    context.root_connector_name);
                return std::nullopt;
            }

            context.scalar_visit_state[connector_name] = _VisitState::VISITING;

            dcn::chain::ResolvedScalarEntries connector_scalar_entries;
            for(std::uint32_t dim_id = 0; dim_id < static_cast<std::uint32_t>(connector->dimensions_size()); ++dim_id)
            {
                const Dimension & dimension = connector->dimensions(static_cast<int>(dim_id));
                const std::string & composite_name = dimension.composite();
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

                    _appendLocalScalarEntry(connector_name, dim_id, connector_scalar_entries);
                    continue;
                }

                auto child_scalar_entries_opt = _computeScalarEntries(
                    composite_name,
                    context);
                if(!child_scalar_entries_opt)
                {
                    return std::nullopt;
                }
                const dcn::chain::ResolvedScalarEntries & child_scalar_entries = *child_scalar_entries_opt;

                if(!_validateChildScalarEntries(connector_name, composite_name, child_scalar_entries))
                {
                    return std::nullopt;
                }

                auto binding_by_slot_opt = _buildBindingBySlotForScalarResolution(
                    connector_name,
                    composite_name,
                    dimension,
                    dim_id,
                    child_scalar_entries.display_entries.size());
                if(!binding_by_slot_opt)
                {
                    return std::nullopt;
                }
                const absl::flat_hash_map<std::uint32_t, std::string> & binding_by_slot = *binding_by_slot_opt;

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

                    if(!_appendBindingScalarEntries(
                           connector_name,
                           binding_it->second,
                           context,
                           connector_scalar_entries))
                    {
                        return std::nullopt;
                    }
                }
            }

            context.scalar_visit_state[connector_name] = _VisitState::DONE;
            auto [cache_it, inserted] =
                context.scalar_entries_cache.try_emplace(connector_name, std::move(connector_scalar_entries));
            (void)inserted;
            return cache_it->second;
        }

        // Resolve scalar entries for the connector currently being added.
        static std::optional<dcn::chain::ResolvedScalarEntries> _resolveRootScalarEntries(
            const Connector & root_connector,
            const absl::flat_hash_map<std::string, chain::Address> & newest_connector,
            const _ConnectorBuckets & connectors)
        {
            const _ConnectorGraphContext graph_context{
                .pending_connector = root_connector,
                .newest_connector = newest_connector,
                .connectors = connectors,
            };
            absl::flat_hash_map<std::string, _VisitState> scalar_visit_state;
            absl::flat_hash_map<std::string, dcn::chain::ResolvedScalarEntries> scalar_entries_cache;
            _ScalarEntriesContext scalar_entries_context{
                .root_connector_name = root_connector.name(),
                .graph = graph_context,
                .scalar_visit_state = scalar_visit_state,
                .scalar_entries_cache = scalar_entries_cache,
            };
            auto root_scalar_entries_opt = _computeScalarEntries(
                root_connector.name(),
                scalar_entries_context);

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
            const _ConnectorGraphContext graph_context{
                .pending_connector = root_connector,
                .newest_connector = newest_connector,
                .connectors = connectors,
            };
            absl::flat_hash_map<std::string, _VisitState> visit_state;
            absl::flat_hash_map<std::string, std::uint32_t> open_slots_cache;
            _OpenSlotsContext open_slots_context{
                .root_connector_name = root_connector.name(),
                .graph = graph_context,
                .visit_state = visit_state,
                .open_slots_cache = open_slots_cache,
            };

            return _computeOpenSlots(
                       root_connector.name(),
                       open_slots_context)
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

        // Provide a compact summary for scalar-label sets in diagnostics.
        static _ScalarLabelSummary _summarizeScalarLabels(const std::vector<ScalarLabel> & labels)
        {
            absl::flat_hash_set<std::string> scalars;
            absl::flat_hash_set<std::string> scalar_tail_pairs;
            scalars.reserve(labels.size());
            scalar_tail_pairs.reserve(labels.size());

            for(const ScalarLabel & label : labels)
            {
                scalars.emplace(label.scalar);

                std::string scalar_tail_key = label.scalar;
                scalar_tail_key.push_back(':');
                scalar_tail_key += std::to_string(label.tail_id);
                scalar_tail_pairs.emplace(std::move(scalar_tail_key));
            }

            return _ScalarLabelSummary{
                .labels_count = labels.size(),
                .unique_scalars_count = scalars.size(),
                .unique_scalar_tail_pairs_count = scalar_tail_pairs.size(),
            };
        }

        // Parse owner hex string once and centralize parse error logging.
        static std::optional<chain::Address> _tryParseOwnerAddress(const std::string & owner)
        {
            const std::optional<chain::Address> owner_res = evmc::from_hex<chain::Address>(owner);
            if(!owner_res)
            {
                spdlog::error(std::format("Failed to parse owner address from hex string '{}'", owner));
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

        // Connector-address indexed maps are global, so the same address
        // cannot be registered under a different connector name.
        if(const auto existing_name_it = _connector_name_by_address.find(address);
            existing_name_it != _connector_name_by_address.end())
        {
            spdlog::error(
                "Connector address is already registered as `{}`; cannot add `{}`",
                existing_name_it->second,
                connector_name);
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
        _connector_name_by_address[address] = connector_name;
        _format_by_connector[address] = format_hash;
        auto & format_connectors = _connectors_by_format[format_hash];
        const auto [_, inserted] = format_connectors.emplace(address);
        if(inserted)
        {
            _dirty_sorted_connectors_by_format.emplace(format_hash);
        }

        const auto scalar_labels_it = _scalar_labels_by_format.find(format_hash);
        if(scalar_labels_it == _scalar_labels_by_format.end())
        {
            _scalar_labels_by_format.emplace(format_hash, canonical_scalar_labels);
        }
        else if(!_scalarLabelsEqual(scalar_labels_it->second, canonical_scalar_labels))
        {
            const _ScalarLabelSummary stored_summary = _summarizeScalarLabels(scalar_labels_it->second);
            const _ScalarLabelSummary new_summary = _summarizeScalarLabels(canonical_scalar_labels);
            spdlog::warn(
                "Different scalar-label multisets for format hash {} while registering connector '{}' at {} "
                "(stored: labels={}, unique_scalars={}, unique_scalar_tail_pairs={}; "
                "new: labels={}, unique_scalars={}, unique_scalar_tail_pairs={}); "
                "keeping first inserted representation",
                evmc::hex(format_hash),
                connector_name,
                evmc::hex(address),
                stored_summary.labels_count,
                stored_summary.unique_scalars_count,
                stored_summary.unique_scalar_tail_pairs_count,
                new_summary.labels_count,
                new_summary.unique_scalars_count,
                new_summary.unique_scalar_tail_pairs_count);
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

        auto name_it = _connector_name_by_address.find(address);
        if(name_it == _connector_name_by_address.end())
        {
            co_return std::nullopt;
        }

        co_return name_it->second;
    }

    asio::awaitable<std::optional<evmc::bytes32>> Registry::getNewestFormatHash(const std::string& name) const
    {
        co_await utils::ensureOnStrand(_strand);

        const auto newest_it = _newest_connector.find(name);
        if(newest_it == _newest_connector.end())
        {
            co_return std::nullopt;
        }

        co_return _lookupConnectorFormatHash(
            name,
            newest_it->second,
            _connectors,
            _format_by_connector);
    }

    asio::awaitable<std::optional<evmc::bytes32>> Registry::getFormatHash(const std::string& name, const chain::Address & address) const
    {
        co_await utils::ensureOnStrand(_strand);

        co_return _lookupConnectorFormatHash(name, address, _connectors, _format_by_connector);
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

    asio::awaitable<std::size_t> Registry::getFormatConnectorsCount(const evmc::bytes32 & format_hash) const
    {
        co_await utils::ensureOnStrand(_strand);

        const auto connectors_it = _connectors_by_format.find(format_hash);
        if(connectors_it == _connectors_by_format.end())
        {
            co_return 0;
        }

        co_return connectors_it->second.size();
    }

    asio::awaitable<std::vector<chain::Address>> Registry::getFormatConnectorsPage(
        const evmc::bytes32 & format_hash,
        std::size_t offset,
        std::size_t limit)
    {
        co_await utils::ensureOnStrand(_strand);

        if(limit == 0)
        {
            co_return std::vector<chain::Address>{};
        }

        if(_connectors_by_format.contains(format_hash) == false)
        {
            co_return std::vector<chain::Address>{};
        }

        const std::vector<chain::Address> & sorted_connectors = _getSortedFormatConnectors(
            format_hash,
            _connectors_by_format,
            _sorted_connectors_by_format,
            _dirty_sorted_connectors_by_format);
        const std::size_t total = sorted_connectors.size();
        const std::size_t start = std::min(offset, total);
        const std::size_t end = (limit > (total - start)) ? total : (start + limit);

        std::vector<chain::Address> page_addresses;
        page_addresses.reserve(end - start);
        page_addresses.insert(
            page_addresses.end(),
            sorted_connectors.begin() + static_cast<std::ptrdiff_t>(start),
            sorted_connectors.begin() + static_cast<std::ptrdiff_t>(end));
        co_return page_addresses;
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
