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
    "src/pt/initial_deployment.cpp"
    "src/pt/proxy_upgrade.cpp"
    "src/mainnet_ingestion.cpp"
)

target_include_directories("${PROJECT_NAME}"     
    PUBLIC 
        "$<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>"
        "$<INSTALL_INTERFACE:${CMAKE_INSTALL_PREFIX}/include>")

set(DECENTRALISED_ART_TEST_SOLC_PATH_DEFAULT "${SOLC_INSTALL_DIR}/bin/${SOLC_EXE_NAME}")
set(DECENTRALISED_ART_TEST_PT_PATH_DEFAULT "${PT_INSTALL_DIR}")

set(DECENTRALISED_ART_TEST_SOLC_PATH
    "${DECENTRALISED_ART_TEST_SOLC_PATH_DEFAULT}"
    CACHE FILEPATH "Path to the solc executable used by unit tests")
set(DECENTRALISED_ART_TEST_PT_PATH
    "${DECENTRALISED_ART_TEST_PT_PATH_DEFAULT}"
    CACHE PATH "Path to PT Solidity runtime directory used by unit tests")

target_compile_definitions("${PROJECT_NAME}"
    PRIVATE
        DECENTRALISED_ART_TEST_BINARY_DIR="${CMAKE_BINARY_DIR}"
        DECENTRALISED_ART_TEST_SOLC_PATH="${DECENTRALISED_ART_TEST_SOLC_PATH}"
        DECENTRALISED_ART_TEST_PT_PATH="${DECENTRALISED_ART_TEST_PT_PATH}"
)

target_link_libraries("${PROJECT_NAME}"
    PRIVATE
        GTest::gtest_main
        spdlog::spdlog
        DecentralisedArtServerLib
)

set_target_properties("${PROJECT_NAME}" PROPERTIES CXX_STANDARD 23)

if(APPLE)
    set_target_properties("${PROJECT_NAME}" PROPERTIES BUILD_RPATH "@loader_path")
elseif(UNIX)
    set_target_properties("${PROJECT_NAME}" PROPERTIES BUILD_RPATH "$ORIGIN")
endif()

# Copy evmone runtime library to the tests output directory.
if(WIN32)
    add_custom_command(TARGET DecentralisedArtServerTests POST_BUILD
        COMMAND ${CMAKE_COMMAND} -E copy_if_different
            "${EVMONE_SHARED_LIBRARY_PATH}"
            "$<TARGET_FILE_DIR:DecentralisedArtServerTests>/${EVMONE_SHARED_LIBRARY}"
        VERBATIM
    )
elseif(APPLE)
    add_custom_command(TARGET DecentralisedArtServerTests POST_BUILD
        COMMAND ${CMAKE_COMMAND} -E copy_if_different
            "${EVMONE_SHARED_LIBRARY_PATH}"
            "$<TARGET_FILE_DIR:DecentralisedArtServerTests>/${EVMONE_SHARED_LIBRARY}"
        COMMAND ${CMAKE_COMMAND} -E copy_if_different
            "${EVMONE_SHARED_LIBRARY_PATH}"
            "$<TARGET_FILE_DIR:DecentralisedArtServerTests>/libevmone.${EVMONE_MAJOR_VERSION}.${EVMONE_MINOR_VERSION}.dylib"
        VERBATIM
    )
elseif(UNIX)
    add_custom_command(TARGET DecentralisedArtServerTests POST_BUILD
        COMMAND ${CMAKE_COMMAND} -E copy_if_different
            "${EVMONE_SHARED_LIBRARY_PATH}"
            "$<TARGET_FILE_DIR:DecentralisedArtServerTests>/${EVMONE_SHARED_LIBRARY}"
        COMMAND ${CMAKE_COMMAND} -E copy_if_different
            "${EVMONE_SHARED_LIBRARY_PATH}"
            "$<TARGET_FILE_DIR:DecentralisedArtServerTests>/libevmone.so.${EVMONE_MAJOR_VERSION}.${EVMONE_MINOR_VERSION}"
        COMMAND ${CMAKE_COMMAND} -E copy_if_different
            "${EVMONE_SHARED_LIBRARY_PATH}"
            "$<TARGET_FILE_DIR:DecentralisedArtServerTests>/libevmone.so.${EVMONE_MAJOR_VERSION}"
        COMMAND ${CMAKE_COMMAND} -E copy_if_different
            "${EVMONE_SHARED_LIBRARY_PATH}"
            "$<TARGET_FILE_DIR:DecentralisedArtServerTests>/libevmone.so"
        VERBATIM
    )
endif()


gtest_discover_tests("${PROJECT_NAME}")
