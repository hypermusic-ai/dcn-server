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

    # --- pt module test files ---
    "src/pt/feature.cpp"
    "src/pt/transformation.cpp"
    "src/pt/condition.cpp"
    "src/pt/particle.cpp"
    "src/pt/execute.cpp"
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

if(APPLE)
    set_target_properties("${PROJECT_NAME}" PROPERTIES BUILD_RPATH "@loader_path")
endif()

# Copy evmone shared library to tests build directory
add_custom_command(TARGET DecentralisedArtServerTests POST_BUILD
  COMMAND ${CMAKE_COMMAND} -E copy_if_different
          "${EVMONE_SHARED_LIBRARY_PATH}"
          "$<TARGET_FILE_DIR:DecentralisedArtServerTests>/${EVMONE_SHARED_LIBRARY}"
  COMMAND ${CMAKE_COMMAND} -E copy_if_different
          "${EVMONE_SHARED_LIBRARY_PATH}"
          "$<TARGET_FILE_DIR:DecentralisedArtServerTests>/libevmone.${EVMONE_MAJOR_VERSION}.${EVMONE_MINOR_VERSION}.dylib"
  VERBATIM
)


gtest_discover_tests("${PROJECT_NAME}")
