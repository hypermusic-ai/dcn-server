#pragma once

#include <gtest/gtest.h>

#include "decentralised_art.hpp"

using namespace std::chrono_literals;

namespace dcn::tests
{
    class BaseUnitTest : public testing::Test
    {
        public:
            virtual ~BaseUnitTest() = default;

            static void SetUpTestSuite()
            {
                spdlog::set_level(spdlog::level::debug);
            }

            static void TearDownTestSuite()
            {  
            }
    };

    class UnitTest : public BaseUnitTest
    {
        public:
            UnitTest() = default;
    };

    class StressTest : public BaseUnitTest
    {
        public:
            StressTest() = default;
    };
}
