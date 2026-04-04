set(CMAKE_SYSTEM_NAME Linux)
set(CMAKE_SYSTEM_PROCESSOR aarch64)

find_program(AARCH64_GCC_COMPILER
    NAMES
        aarch64-linux-gnu-gcc
        aarch64-linux-gnu-gcc-13
        aarch64-linux-gnu-gcc-12
        aarch64-linux-gnu-gcc-11
    HINTS
        /usr/bin
        /usr/local/bin
        /opt/homebrew/bin
        /opt/local/bin
)

find_program(AARCH64_GXX_COMPILER
    NAMES
        aarch64-linux-gnu-g++
        aarch64-linux-gnu-g++-13
        aarch64-linux-gnu-g++-12
        aarch64-linux-gnu-g++-11
    HINTS
        /usr/bin
        /usr/local/bin
        /opt/homebrew/bin
        /opt/local/bin
)

if(NOT AARCH64_GCC_COMPILER OR NOT AARCH64_GXX_COMPILER)
    message(FATAL_ERROR
        "Could not locate aarch64 GNU cross-compilers (aarch64-linux-gnu-gcc/g++). "
        "Install them or provide full paths via CMAKE_C_COMPILER/CMAKE_CXX_COMPILER."
    )
endif()

set(CMAKE_C_COMPILER "${AARCH64_GCC_COMPILER}" CACHE FILEPATH "aarch64 C compiler" FORCE)
set(CMAKE_CXX_COMPILER "${AARCH64_GXX_COMPILER}" CACHE FILEPATH "aarch64 C++ compiler" FORCE)

message(STATUS "Using aarch64 C compiler: ${CMAKE_C_COMPILER}")
message(STATUS "Using aarch64 CXX compiler: ${CMAKE_CXX_COMPILER}")
