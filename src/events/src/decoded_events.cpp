#include <string_view>

#include "decoded_event.hpp"

namespace dcn::events
{


    std::string toString(const EventState state)
    {
        switch(state)
        {
        case EventState::OBSERVED:
            return std::string(OBSERVED_STATE);
        case EventState::SAFE:
            return std::string(SAFE_STATE);
        case EventState::FINALIZED:
            return std::string(FINALIZED_STATE);
        case EventState::REMOVED:
            return std::string(REMOVED_STATE);
        }

        assert(false);
    }

    std::string toString(const EventType type)
    {
        switch(type)
        {
        case EventType::CONNECTOR_ADDED:
            return std::string(CONNECTOR_ADDED_TYPE);
        case EventType::TRANSFORMATION_ADDED:
            return std::string(TRANSFORMATION_ADDED_TYPE);
        case EventType::CONDITION_ADDED:
            return std::string(CONDITION_ADDED_TYPE);
        }

        assert(false);
    }
}

namespace dcn::parse
{
    Result<events::EventState> parseEventState(const std::string & value)
    {
        if(value == events::OBSERVED_STATE)
        {
            return events::EventState::OBSERVED;
        }
        if(value == events::SAFE_STATE)
        {
            return events::EventState::SAFE;
        }
        if(value == events::FINALIZED_STATE)
        {
            return events::EventState::FINALIZED;
        }
        if(value == events::REMOVED_STATE)
        {
            return events::EventState::REMOVED;
        }
        
        return std::unexpected(ParseError{ParseError::Kind::INVALID_VALUE});
    }

    Result<events::EventType> parseEventType(const std::string & value)
    {
        if(value == events::CONNECTOR_ADDED_TYPE)
        {
            return events::EventType::CONNECTOR_ADDED;
        }
        if(value == events::TRANSFORMATION_ADDED_TYPE)
        {
            return events::EventType::TRANSFORMATION_ADDED;
        }
        if(value == events::CONDITION_ADDED_TYPE)
        {
            return events::EventType::CONDITION_ADDED;
        }

        return std::unexpected(ParseError{ParseError::Kind::INVALID_VALUE});
    }

}