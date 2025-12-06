cmake_minimum_required(VERSION 3.30)

enable_testing()

include(GoogleTest)
set(CMAKE_MSVC_RUNTIME_LIBRARY "MultiThreaded$<$<CONFIG:Debug>:Debug>")

set(PROJECT_NAME "DecentralisedArtServerInstallPackageTests")
set(PROJECT_PREFIX "DecentralisedArtServerInstallPackageTests")

project("${PROJECT_NAME}"   
    VERSION "0.0.1"
    DESCRIPTION "Decentralised Art Server tests"
    HOMEPAGE_URL "https://github.com/hypermusic-ai/dcn-server"
    LANGUAGES CXX
)

add_executable("${PROJECT_NAME}"    
    "install_package.cpp"
)

target_link_libraries("${PROJECT_NAME}"
    PRIVATE
        GTest::gtest_main
        spdlog::spdlog
        asio
        evmc
        evmone
        protobuf::libprotobuf
        DecentralisedArtServer
)

set_target_properties("${PROJECT_NAME}" PROPERTIES CXX_STANDARD 23)

gtest_discover_tests("${PROJECT_NAME}")
