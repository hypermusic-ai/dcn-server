#include "solidity_identifier.hpp"

namespace dcn::pt
{
    bool isValidSolidityIdentifier(std::string_view value)
    {
        if(value.empty())
        {
            return false;
        }

        const auto is_alpha = [](char ch)
        {
            return (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z');
        };

        const auto is_digit = [](char ch)
        {
            return ch >= '0' && ch <= '9';
        };

        const char first = value.front();
        if(!(is_alpha(first) || first == '_'))
        {
            return false;
        }

        for(const char ch : value)
        {
            if(!(is_alpha(ch) || is_digit(ch) || ch == '_'))
            {
                return false;
            }
        }

        return true;
    }
}
