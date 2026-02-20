#include "unit-tests.hpp"

using namespace dcn;
using namespace dcn::parse;
using namespace dcn::tests;

namespace
{
    Particle makeParticleSample()
    {
        Particle particle;
        particle.set_name("particle_beta");
        particle.set_feature_name("feature_alpha");
        particle.add_composite_names("comp_a");
        particle.add_composite_names("comp_b");
        particle.set_condition_name("condition_check");
        particle.add_condition_args(1);
        particle.add_condition_args(2);
        return particle;
    }

    ParticleRecord makeParticleRecordSample()
    {
        ParticleRecord record;
        *record.mutable_particle() = makeParticleSample();
        record.set_owner("0xabc123");
        return record;
    }

    void expectEqual(const Particle & lhs, const Particle & rhs)
    {
        ASSERT_EQ(lhs.name(), rhs.name());
        ASSERT_EQ(lhs.feature_name(), rhs.feature_name());
        ASSERT_EQ(lhs.composite_names_size(), rhs.composite_names_size());
        for (int i = 0; i < lhs.composite_names_size(); ++i)
        {
            EXPECT_EQ(lhs.composite_names(i), rhs.composite_names(i));
        }
        ASSERT_EQ(lhs.condition_name(), rhs.condition_name());
        ASSERT_EQ(lhs.condition_args_size(), rhs.condition_args_size());
        for (int i = 0; i < lhs.condition_args_size(); ++i)
        {
            EXPECT_EQ(lhs.condition_args(i), rhs.condition_args(i));
        }
    }

    void expectEqual(const ParticleRecord & lhs, const ParticleRecord & rhs)
    {
        expectEqual(lhs.particle(), rhs.particle());
        EXPECT_EQ(lhs.owner(), rhs.owner());
    }
}

TEST_F(UnitTest, Particle_ParseFromJson_JsonAndProtobufMatch)
{
    json json_input = {
        {"name", "particle_beta"},
        {"feature_name", "feature_alpha"},
        {"composite_names", json::array({"comp_a", "comp_b"})},
        {"condition_name", "condition_check"},
        {"condition_args", json::array({1, 2})}
    };

    auto json_particle = parseFromJson<Particle>(json_input, use_json);
    auto protobuf_particle = parseFromJson<Particle>(json_input.dump(), use_protobuf);

    ASSERT_TRUE(json_particle.has_value());
    ASSERT_TRUE(protobuf_particle.has_value());
    expectEqual(*json_particle, *protobuf_particle);
}

TEST_F(UnitTest, Particle_ParseToJson_RoundTripAcrossParsers)
{
    Particle particle = makeParticleSample();

    auto json_out = parseToJson(particle, use_json);
    auto protobuf_out = parseToJson(particle, use_protobuf);

    ASSERT_TRUE(json_out.has_value());
    ASSERT_TRUE(protobuf_out.has_value());

    auto from_json_via_protobuf = parseFromJson<Particle>(json_out->dump(), use_protobuf);
    auto from_protobuf_via_json = parseFromJson<Particle>(json::parse(*protobuf_out), use_json);

    ASSERT_TRUE(from_json_via_protobuf.has_value());
    ASSERT_TRUE(from_protobuf_via_json.has_value());
    expectEqual(particle, *from_json_via_protobuf);
    expectEqual(particle, *from_protobuf_via_json);
}

TEST_F(UnitTest, ParticleRecord_ParseFromJson_JsonAndProtobufMatch)
{
    json json_particle = {
        {"name", "particle_beta"},
        {"feature_name", "feature_alpha"},
        {"composite_names", json::array({"comp_a", "comp_b"})},
        {"condition_name", "condition_check"},
        {"condition_args", json::array({1, 2})}
    };
    json json_input = {
        {"particle", json_particle},
        {"owner", "0xabc123"}
    };

    auto json_record = parseFromJson<ParticleRecord>(json_input, use_json);
    auto protobuf_record = parseFromJson<ParticleRecord>(json_input.dump(), use_protobuf);

    ASSERT_TRUE(json_record.has_value());
    ASSERT_TRUE(protobuf_record.has_value());
    expectEqual(*json_record, *protobuf_record);
}

TEST_F(UnitTest, ParticleRecord_ParseToJson_RoundTripAcrossParsers)
{
    ParticleRecord record = makeParticleRecordSample();

    auto json_out = parseToJson(record, use_json);
    auto protobuf_out = parseToJson(record, use_protobuf);

    ASSERT_TRUE(json_out.has_value());
    ASSERT_TRUE(protobuf_out.has_value());

    auto from_json_via_protobuf = parseFromJson<ParticleRecord>(json_out->dump(), use_protobuf);
    auto from_protobuf_via_json = parseFromJson<ParticleRecord>(json::parse(*protobuf_out), use_json);

    ASSERT_TRUE(from_json_via_protobuf.has_value());
    ASSERT_TRUE(from_protobuf_via_json.has_value());
    expectEqual(record, *from_json_via_protobuf);
    expectEqual(record, *from_protobuf_via_json);
}

TEST_F(UnitTest, Particle_ConstructSolidityCode_UsesInitializerPattern)
{
    Particle particle = makeParticleSample();
    std::string solidity = constructParticleSolidityCode(particle);

    EXPECT_NE(solidity.find("function initialize(address registryAddr) external initializer"), std::string::npos);
    EXPECT_NE(solidity.find("__ParticleBase_init"), std::string::npos);
    EXPECT_EQ(solidity.find("constructor(address registryAddr)"), std::string::npos);
}
