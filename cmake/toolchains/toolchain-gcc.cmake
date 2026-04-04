set(GCC_TARGET_BITS "64" CACHE STRING "GCC target bitness (32 or 64)")
set_property(CACHE GCC_TARGET_BITS PROPERTY STRINGS 32 64)

if(GCC_TARGET_BITS STREQUAL "64")
    set(CMAKE_SYSTEM_PROCESSOR x86_64)
    set(_gcc_candidates
        x86_64-linux-gnu-gcc-13
        x86_64-linux-gnu-gcc
        gcc-13
        gcc
        gcc-12
        gcc-11
    )
    set(_gxx_candidates
        x86_64-linux-gnu-g++-13
        x86_64-linux-gnu-g++
        g++-13
        g++
        g++-12
        g++-11
    )
    set(_arch_flag -m64)
elseif(GCC_TARGET_BITS STREQUAL "32")
    set(CMAKE_SYSTEM_PROCESSOR x86)
    set(_gcc_candidates
        i686-linux-gnu-gcc-13
        i686-linux-gnu-gcc
        gcc-13
        gcc
        gcc-12
        gcc-11
    )
    set(_gxx_candidates
        i686-linux-gnu-g++-13
        i686-linux-gnu-g++
        g++-13
        g++
        g++-12
        g++-11
    )
    set(_arch_flag -m32)
else()
    message(FATAL_ERROR
        "Invalid GCC_TARGET_BITS='${GCC_TARGET_BITS}'. "
        "Supported values are 32 or 64."
    )
endif()

find_program(GCC_COMPILER
    NAMES ${_gcc_candidates}
    HINTS
        /usr/bin
        /usr/local/bin
        /opt/homebrew/bin
        /opt/local/bin
)

find_program(GXX_COMPILER
    NAMES ${_gxx_candidates}
    HINTS
        /usr/bin
        /usr/local/bin
        /opt/homebrew/bin
        /opt/local/bin
)

if(NOT GCC_COMPILER OR NOT GXX_COMPILER)
    message(FATAL_ERROR
        "Could not locate GNU compilers for ${GCC_TARGET_BITS}-bit target. "
        "Install gcc/g++ toolchains or provide full paths via CMAKE_C_COMPILER/CMAKE_CXX_COMPILER."
    )
endif()

set(CMAKE_C_COMPILER "${GCC_COMPILER}" CACHE FILEPATH "GNU C compiler" FORCE)
set(CMAKE_CXX_COMPILER "${GXX_COMPILER}" CACHE FILEPATH "GNU CXX compiler" FORCE)

string(APPEND CMAKE_C_FLAGS_INIT " ${_arch_flag}")
string(APPEND CMAKE_CXX_FLAGS_INIT " ${_arch_flag}")
string(APPEND CMAKE_EXE_LINKER_FLAGS_INIT " ${_arch_flag}")
string(APPEND CMAKE_SHARED_LINKER_FLAGS_INIT " ${_arch_flag}")
string(APPEND CMAKE_MODULE_LINKER_FLAGS_INIT " ${_arch_flag}")

message(STATUS "GCC target bitness: ${GCC_TARGET_BITS}")
message(STATUS "Using GCC C compiler: ${CMAKE_C_COMPILER}")
message(STATUS "Using GCC CXX compiler: ${CMAKE_CXX_COMPILER}")
