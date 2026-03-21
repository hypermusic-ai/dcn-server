#include "format_hash.hpp"

#include <array>
#include <cstring>

#include "crypto.hpp"

namespace
{
    constexpr std::uint8_t PATH_DIM_DOMAIN = 0x10;
    constexpr std::uint8_t PATH_CONCAT_DOMAIN = 0x11;
    constexpr std::uint8_t SCALAR_PATH_LABEL_DOMAIN = 0x12;
}

namespace dcn::chain
{
    bool equalBytes32(const evmc::bytes32 & lhs, const evmc::bytes32 & rhs)
    {
        return std::memcmp(lhs.bytes, rhs.bytes, sizeof(lhs.bytes)) == 0;
    }

    bool lessBytes32(const evmc::bytes32 & lhs, const evmc::bytes32 & rhs)
    {
        return std::memcmp(lhs.bytes, rhs.bytes, sizeof(lhs.bytes)) < 0;
    }

    evmc::bytes32 keccakBytes(const std::uint8_t * data, std::size_t size)
    {
        evmc::bytes32 out{};
        dcn::crypto::Keccak256::getHash(data, size, out.bytes);
        return out;
    }

    evmc::bytes32 keccakString(std::string_view value)
    {
        return keccakBytes(
            reinterpret_cast<const std::uint8_t *>(value.data()),
            value.size());
    }

    evmc::bytes32 composeFormatHash(const evmc::bytes32 & lhs, const evmc::bytes32 & rhs)
    {
        // lane0 occupies least-significant 64 bits (bytes[24..31]).
        const std::uint64_t lhs0 = dcn::crypto::readUint64BE(lhs.bytes + 24);
        const std::uint64_t lhs1 = dcn::crypto::readUint64BE(lhs.bytes + 16);
        const std::uint64_t lhs2 = dcn::crypto::readUint64BE(lhs.bytes + 8);
        const std::uint64_t lhs3 = dcn::crypto::readUint64BE(lhs.bytes + 0);

        const std::uint64_t rhs0 = dcn::crypto::readUint64BE(rhs.bytes + 24);
        const std::uint64_t rhs1 = dcn::crypto::readUint64BE(rhs.bytes + 16);
        const std::uint64_t rhs2 = dcn::crypto::readUint64BE(rhs.bytes + 8);
        const std::uint64_t rhs3 = dcn::crypto::readUint64BE(rhs.bytes + 0);

        evmc::bytes32 out{};
        dcn::crypto::writeUint64BE(out.bytes + 24, lhs0 + rhs0);
        dcn::crypto::writeUint64BE(out.bytes + 16, lhs1 + rhs1);
        dcn::crypto::writeUint64BE(out.bytes + 8, lhs2 + rhs2);
        dcn::crypto::writeUint64BE(out.bytes + 0, lhs3 + rhs3);
        return out;
    }

    evmc::bytes32 dimPathHash(std::uint32_t dim_id)
    {
        std::array<std::uint8_t, 5> input{};
        input[0] = PATH_DIM_DOMAIN;
        dcn::crypto::writeUint32BE(input.data() + 1, dim_id);
        return keccakBytes(input.data(), input.size());
    }

    evmc::bytes32 concatPathHash(const evmc::bytes32 & left, const evmc::bytes32 & right)
    {
        std::array<std::uint8_t, 65> input{};
        input[0] = PATH_CONCAT_DOMAIN;
        std::memcpy(input.data() + 1, left.bytes, sizeof(left.bytes));
        std::memcpy(input.data() + 33, right.bytes, sizeof(right.bytes));
        return keccakBytes(input.data(), input.size());
    }

    evmc::bytes32 scalarPathLabelHash(const evmc::bytes32 & scalar_hash, const evmc::bytes32 & path_hash)
    {
        std::array<std::uint8_t, 65> input{};
        input[0] = SCALAR_PATH_LABEL_DOMAIN;
        std::memcpy(input.data() + 1, scalar_hash.bytes, sizeof(scalar_hash.bytes));
        std::memcpy(input.data() + 33, path_hash.bytes, sizeof(path_hash.bytes));
        return keccakBytes(input.data(), input.size());
    }

    evmc::bytes32 labelHashToFormatHash(const evmc::bytes32 & label_hash)
    {
        std::array<std::uint8_t, 33> input{};
        std::memcpy(input.data() + 1, label_hash.bytes, sizeof(label_hash.bytes));

        std::uint64_t lanes[4]{};
        for(std::uint8_t domain = 0; domain < 4; ++domain)
        {
            input[0] = domain;
            const evmc::bytes32 lane_hash = keccakBytes(input.data(), input.size());
            lanes[domain] = dcn::crypto::readUint64BE(lane_hash.bytes + 24);
        }

        evmc::bytes32 out{};
        dcn::crypto::writeUint64BE(out.bytes + 24, lanes[0]);
        dcn::crypto::writeUint64BE(out.bytes + 16, lanes[1]);
        dcn::crypto::writeUint64BE(out.bytes + 8, lanes[2]);
        dcn::crypto::writeUint64BE(out.bytes + 0, lanes[3]);
        return out;
    }

    evmc::bytes32 computeFormatHashFromLabelHashes(const std::vector<evmc::bytes32> & label_hashes)
    {
        evmc::bytes32 format_hash{};
        for(const evmc::bytes32 & label_hash : label_hashes)
        {
            format_hash = composeFormatHash(format_hash, labelHashToFormatHash(label_hash));
        }

        return format_hash;
    }

    evmc::bytes32 computeFormatHash(const std::vector<ScalarHashEntry> & hash_entries)
    {
        std::vector<evmc::bytes32> label_hashes;
        label_hashes.reserve(hash_entries.size());

        for(const ScalarHashEntry & hash_entry : hash_entries)
        {
            label_hashes.push_back(scalarPathLabelHash(hash_entry.scalar_hash, hash_entry.path_hash));
        }

        return computeFormatHashFromLabelHashes(label_hashes);
    }
}
