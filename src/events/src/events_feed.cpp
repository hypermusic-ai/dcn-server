#include "events_feed.hpp"


namespace dcn::parse
{
    Result<events::CursorKey> parseHistoryCursor(const std::string & cursor)
    {
        std::size_t p0 = cursor.find(':');
        if(p0 == std::string::npos)
        {
            return std::unexpected(ParseError{ParseError::Kind::INVALID_VALUE});
        }

        std::size_t p1 = cursor.find(':', p0 + 1);
        if(p1 == std::string::npos)
        {
            return std::unexpected(ParseError{ParseError::Kind::INVALID_VALUE});
        }

        std::size_t p2 = cursor.find(':', p1 + 1);
        if(p2 == std::string::npos)
        {
            return std::unexpected(ParseError{ParseError::Kind::INVALID_VALUE});
        }

        std::size_t p3 = cursor.find(':', p2 + 1);
        if(p3 == std::string::npos)
        {
            return std::unexpected(ParseError{ParseError::Kind::INVALID_VALUE});
        }

        events::CursorKey key{};
        try
        {
            key.chain_id = std::stoi(cursor.substr(0, p0));
            key.block_number = std::stoll(cursor.substr(p0 + 1, p1 - (p0 + 1)));
            key.tx_index = std::stoll(cursor.substr(p1 + 1, p2 - (p1 + 1)));
            key.log_index = std::stoll(cursor.substr(p2 + 1, p3 - (p2 + 1)));
            key.feed_id = cursor.substr(p3 + 1);
        }
        catch(...)
        {
            return std::unexpected(ParseError{ParseError::Kind::INVALID_VALUE});
        }

        if(key.feed_id.empty())
        {
            return std::unexpected(ParseError{ParseError::Kind::INVALID_VALUE});
        }

        return key;
    }
}