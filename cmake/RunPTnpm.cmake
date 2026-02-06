cmake_minimum_required(VERSION 3.16)

if(NOT DEFINED PT_SOLIDITY_DIR)
    message(FATAL_ERROR "PT_SOLIDITY_DIR not set")
endif()
if(NOT DEFINED NPM_EXECUTABLE)
    message(FATAL_ERROR "NPM_EXECUTABLE not set")
endif()

# Build-time npm command
set(_npm_cmd "${NPM_EXECUTABLE}")
if(WIN32)
    set(_npm_cmd cmd /c call "${NPM_EXECUTABLE}")
endif()

if(EXISTS "${PT_SOLIDITY_DIR}/package-lock.json")
    set(_npm_args ci --no-audit --no-fund)
elseif(EXISTS "${PT_SOLIDITY_DIR}/package.json")
    set(_npm_args install --no-audit --no-fund)
else()
    message(STATUS "No package.json/package-lock.json in ${PT_SOLIDITY_DIR}, skipping npm install")
    return()
endif()

execute_process(
    COMMAND ${_npm_cmd} ${_npm_args}
    WORKING_DIRECTORY "${PT_SOLIDITY_DIR}"
    RESULT_VARIABLE _npm_result
)

if(NOT _npm_result EQUAL 0)
    message(FATAL_ERROR "npm failed with exit code ${_npm_result}")
endif()
