#include "keccak256.hpp"


namespace dcn::crypto
{
    void Keccak256::getHash(const std::uint8_t msg[], std::size_t len, std::uint8_t hashResult[HASH_LEN]) 
    {
    	assert((msg != nullptr || len == 0) && hashResult != nullptr);
    	std::uint64_t state[5][5] = {};
    
    	// XOR each message byte into the state, and absorb full blocks
    	int blockOff = 0;
    	for (std::size_t i = 0; i < len; i++) 
        {
    		int j = blockOff >> 3;
    		state[j % 5][j / 5] ^= static_cast<std::uint64_t>(msg[i]) << ((blockOff & 7) << 3);
    		blockOff++;
    		if (blockOff == BLOCK_SIZE) 
            {
    			absorb(state);
    			blockOff = 0;
    		}
    	}
    
    	// Final block and padding
    	{
    		int i = blockOff >> 3;
    		state[i % 5][i / 5] ^= UINT64_C(0x01) << ((blockOff & 7) << 3);
    		blockOff = BLOCK_SIZE - 1;
    		int j = blockOff >> 3;
    		state[j % 5][j / 5] ^= UINT64_C(0x80) << ((blockOff & 7) << 3);
    		absorb(state);
    	}
    
    	// Uint64 array to bytes in little endian
    	for (int i = 0; i < HASH_LEN; i++) 
        {
    		int j = i >> 3;
    		hashResult[i] = static_cast<std::uint8_t>(state[j % 5][j / 5] >> ((i & 7) << 3));
    	}
    }

    void Keccak256::absorb(std::uint64_t state[5][5]) 
    {
        std::uint64_t (*a)[5] = state;
    	std::uint8_t r = 1;

    	for (int i = 0; i < NUM_ROUNDS; i++) 
        {
    		// Theta step
    		std::uint64_t c[5] = {};
    		for (int x = 0; x < 5; x++) 
            {
    			for (int y = 0; y < 5; y++)
    				c[x] ^= a[x][y];
    		}

    		for (int x = 0; x < 5; x++) 
            {
    			std::uint64_t d = c[(x + 4) % 5] ^ rotl64(c[(x + 1) % 5], 1);
    			for (int y = 0; y < 5; y++)
    				a[x][y] ^= d;
    		}
    
    		// Rho, Pi
    		std::uint64_t b[5][5];
    		for (int x = 0; x < 5; x++) {
    			for (int y = 0; y < 5; y++)
    				b[y][(x * 2 + y * 3) % 5] = rotl64(a[x][y], ROTATION[x][y]);
    		}
    
    		// Chi
    		for (int x = 0; x < 5; x++) {
    			for (int y = 0; y < 5; y++)
    				a[x][y] = b[x][y] ^ (~b[(x + 1) % 5][y] & b[(x + 2) % 5][y]);
    		}
    
    		// Iota
    		for (int j = 0; j < 7; j++) 
            {
    			a[0][0] ^= static_cast<std::uint64_t>(r & 1) << ((1 << j) - 1);
    			r = static_cast<std::uint8_t>((r << 1) ^ ((r >> 7) * 0x171));
    		}
    	}
    }

    std::uint64_t Keccak256::rotl64(std::uint64_t x, int i) 
    {
    	return ((0U + x) << i) | (x >> ((64 - i) & 63));
    }

    const unsigned char Keccak256::ROTATION[5][5] = {
    	{ 0, 36,  3, 41, 18},
    	{ 1, 44, 10, 45,  2},
    	{62,  6, 43, 15, 61},
    	{28, 55, 25, 21, 56},
    	{27, 20, 39,  8, 14},
    };
}