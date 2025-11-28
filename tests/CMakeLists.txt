cmake_minimum_required(VERSION 3.30)

enable_testing()

include(GoogleTest)

set(PROJECT_NAME "DecentralisedArtServerTests")
set(PROJECT_PREFIX "DecentralisedArtServerTests")

project("${PROJECT_NAME}"   
    VERSION "0.0.1"
    DESCRIPTION "Decentralised Art Server tests"
    HOMEPAGE_URL "https://github.com/hypermusic-ai/dcn-server"
    LANGUAGES CXX
)

add_executable("${PROJECT_NAME}"    
    "src/tests.cpp"

    # --- Test files ---
    "src/ut0.cpp"
    "src/router-tests.cpp"
)

target_include_directories("${PROJECT_NAME}"     
    PUBLIC 
        "$<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>"
        "$<INSTALL_INTERFACE:${CMAKE_INSTALL_PREFIX}/include>")

target_link_libraries("${PROJECT_NAME}"
    PRIVATE
        GTest::gtest_main
        spdlog::spdlog
        DecentralisedArtServerLib
)

set_target_properties("${PROJECT_NAME}" PROPERTIES CXX_STANDARD 23)

gtest_discover_tests("${PROJECT_NAME}")
