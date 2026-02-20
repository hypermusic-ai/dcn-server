#pragma once

#include <string>

namespace dcn::utils
{
    struct LogoASCII_t{};
    static const LogoASCII_t LogoASCII;

    struct LogoUnicode_t{};
    static const LogoUnicode_t LogoUnicode;

    /**
     * @brief Get the ASCII logo string.
     * 
     * @return The ASCII logo string.
     */
    const std::string & getLogo(LogoASCII_t);

    /**
     * @brief Get the Unicode logo string.
     * 
     * @return The Unicode logo string.
     */
    const std::string & getLogo(LogoUnicode_t);
}