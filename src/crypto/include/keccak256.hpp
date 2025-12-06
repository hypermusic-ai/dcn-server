#pragma once

#include <cstddef>
#include <cstdint>
#include <cassert>

namespace dcn::crypto
{
    class Keccak256 
    {
    	public:     
            static constexpr int HASH_LEN = 32;

    	    static void getHash(const std::uint8_t msg[], std::size_t len, std::uint8_t hashResult[HASH_LEN]);
    
    	private: 
        	Keccak256() = delete;

            static void absorb(std::uint64_t state[5][5]);
            static std::uint64_t rotl64(std::uint64_t x, int i);

    	    static const unsigned char ROTATION[5][5];
            static constexpr int BLOCK_SIZE = 200 - HASH_LEN * 2;
            static constexpr int NUM_ROUNDS = 24;
    };
}