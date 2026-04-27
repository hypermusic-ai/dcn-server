#include "events_ingest.hpp"
#include "chain.hpp"
#include "pt.hpp"

namespace dcn::events
{
    PTEventDecoder::PTEventDecoder()
        : _connector_topic(chain::normalizeHex(
              evmc::hex(chain::constructEventTopic("ConnectorAdded(address,address,string,address,uint32,uint32[],string[],"
                                                   "uint32[],uint32[],string[],string,int32[],bytes32)"))))
        , _transformation_topic(chain::normalizeHex(
              evmc::hex(chain::constructEventTopic("TransformationAdded(address,string,address,address,uint32)"))))
        , _condition_topic(chain::normalizeHex(
              evmc::hex(chain::constructEventTopic("ConditionAdded(address,string,address,address,uint32)"))))
    {
    }

    std::optional<DecodedEvent> PTEventDecoder::decode(const RawChainLog& log) const
    {
        if (!log.topics[0].has_value())
        {
            return std::nullopt;
        }

        std::vector<std::string> topics_hex;
        topics_hex.reserve(log.topics.size());
        for (const auto& topic : log.topics)
        {
            if (topic.has_value())
            {
                topics_hex.push_back(*topic);
            }
        }

        if (topics_hex.empty())
        {
            return std::nullopt;
        }

        const std::string topic0 = chain::normalizeHex(topics_hex.front());

        if (topic0 == _connector_topic)
        {
            const auto connector = pt::decodeConnectorAddedEvent(log.data_hex, topics_hex);
            if (!connector)
            {
                return std::nullopt;
            }

            json payload {{"name", connector->name},
                          {"caller", chain::addressToHex(connector->caller)},
                          {"owner", chain::addressToHex(connector->owner)},
                          {"connector_address", chain::addressToHex(connector->connector_address)},
                          {"dimensions_count", connector->dimensions_count},
                          {"condition_name", connector->condition_name},
                          {"condition_args", connector->condition_args},
                          {"format_hash", chain::bytes32ToHex(connector->format_hash)}};

            json composites = json::array();
            for (const auto& [dim_id, composite_name] : connector->composites)
            {
                composites.push_back(json {{"dim_id", dim_id}, {"composite_name", composite_name}});
            }
            payload["composites"] = std::move(composites);

            json bindings = json::array();
            for (const auto& [binding_key, binding_name] : connector->bindings)
            {
                bindings.push_back(
                    json {{"dim_id", binding_key.first}, {"slot_id", binding_key.second}, {"binding_name", binding_name}});
            }
            payload["bindings"] = std::move(bindings);

            return DecodedEvent {.raw = log,
                                 .event_type = EventType::CONNECTOR_ADDED,
                                 .state = log.removed ? EventState::REMOVED : EventState::OBSERVED,
                                 .name = connector->name,
                                 .caller = chain::addressToHex(connector->caller),
                                 .owner = chain::addressToHex(connector->owner),
                                 .entity_address = chain::addressToHex(connector->connector_address),
                                 .args_count = std::nullopt,
                                 .format_hash = chain::bytes32ToHex(connector->format_hash),
                                 .decoded_json = payload.dump(-1, ' ', false, json::error_handler_t::replace)};
        }

        if (topic0 == _transformation_topic)
        {
            const auto transformation = pt::decodeTransformationAddedEvent(log.data_hex, topics_hex);
            if (!transformation)
            {
                return std::nullopt;
            }

            json payload {{"name", transformation->name},
                          {"caller", chain::addressToHex(transformation->caller)},
                          {"owner", chain::addressToHex(transformation->owner)},
                          {"transformation_address", chain::addressToHex(transformation->transformation_address)},
                          {"args_count", transformation->args_count}};

            return DecodedEvent {.raw = log,
                                 .event_type = EventType::TRANSFORMATION_ADDED,
                                 .state = log.removed ? EventState::REMOVED : EventState::OBSERVED,
                                 .name = transformation->name,
                                 .caller = chain::addressToHex(transformation->caller),
                                 .owner = chain::addressToHex(transformation->owner),
                                 .entity_address = chain::addressToHex(transformation->transformation_address),
                                 .args_count = transformation->args_count,
                                 .format_hash = std::nullopt,
                                 .decoded_json = payload.dump(-1, ' ', false, json::error_handler_t::replace)};
        }

        if (topic0 == _condition_topic)
        {
            const auto condition = pt::decodeConditionAddedEvent(log.data_hex, topics_hex);
            if (!condition)
            {
                return std::nullopt;
            }

            json payload {{"name", condition->name},
                          {"caller", chain::addressToHex(condition->caller)},
                          {"owner", chain::addressToHex(condition->owner)},
                          {"condition_address", chain::addressToHex(condition->condition_address)},
                          {"args_count", condition->args_count}};

            return DecodedEvent {.raw = log,
                                 .event_type = EventType::CONDITION_ADDED,
                                 .state = log.removed ? EventState::REMOVED : EventState::OBSERVED,
                                 .name = condition->name,
                                 .caller = chain::addressToHex(condition->caller),
                                 .owner = chain::addressToHex(condition->owner),
                                 .entity_address = chain::addressToHex(condition->condition_address),
                                 .args_count = condition->args_count,
                                 .format_hash = std::nullopt,
                                 .decoded_json = payload.dump(-1, ' ', false, json::error_handler_t::replace)};
        }

        return std::nullopt;
    }
} // namespace dcn::events