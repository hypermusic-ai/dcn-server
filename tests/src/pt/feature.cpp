#include "unit-tests.hpp"

using namespace dcn;
using namespace dcn::parse;
using namespace dcn::tests;

namespace
{
    Feature makeFeatureSample()
    {
        Feature feature;
        feature.set_name("feature_alpha");
        feature.add_dimensions();
        feature.add_dimensions();
        return feature;
    }

    FeatureRecord makeFeatureRecordSample()
    {
        FeatureRecord record;
        *record.mutable_feature() = makeFeatureSample();
        record.set_owner("0xabc123");
        return record;
    }

    void expectEqual(const TransformationDef & lhs, const TransformationDef & rhs)
    {
        ASSERT_EQ(lhs.name(), rhs.name());
        ASSERT_EQ(lhs.args_size(), rhs.args_size());
        for(int i = 0; i < lhs.args_size(); ++i)
        {
            EXPECT_EQ(lhs.args(i), rhs.args(i));
        }
    }

    void expectEqual(const Dimension & lhs, const Dimension & rhs)
    {
        ASSERT_EQ(lhs.transformations_size(), rhs.transformations_size());
        for(int i = 0; i < lhs.transformations_size(); ++i)
        {
            expectEqual(lhs.transformations(i), rhs.transformations(i));
        }
    }

    void expectEqual(const Feature & lhs, const Feature & rhs)
    {
        ASSERT_EQ(lhs.name(), rhs.name());
        ASSERT_EQ(lhs.dimensions_size(), rhs.dimensions_size());
        for(int i = 0; i < lhs.dimensions_size(); ++i)
        {
            expectEqual(lhs.dimensions(i), rhs.dimensions(i));
        }
    }

    void expectEqual(const FeatureRecord & lhs, const FeatureRecord & rhs)
    {
        expectEqual(lhs.feature(), rhs.feature());
        EXPECT_EQ(lhs.owner(), rhs.owner());
    }
}

TEST_F(UnitTest, Feature_ParseFromJson_JsonAndProtobufMatch)
{
    json json_input = {
        {"name", "feature_alpha"},
        {"dimensions", json::array({
            json{{"transformations", json::array()}},
            json{{"transformations", json::array()}}
        })}
    };

    auto json_feature = parseFromJson<Feature>(json_input, use_json);
    auto protobuf_feature = parseFromJson<Feature>(json_input.dump(), use_protobuf);

    ASSERT_TRUE(json_feature.has_value());
    ASSERT_TRUE(protobuf_feature.has_value());
    expectEqual(*json_feature, *protobuf_feature);
}

TEST_F(UnitTest, Feature_ParseToJson_RoundTripAcrossParsers)
{
    Feature feature = makeFeatureSample();

    auto json_out = parseToJson(feature, use_json);
    auto protobuf_out = parseToJson(feature, use_protobuf);

    ASSERT_TRUE(json_out.has_value());
    ASSERT_TRUE(protobuf_out.has_value());

    auto from_json_via_protobuf = parseFromJson<Feature>(json_out->dump(), use_protobuf);
    auto from_protobuf_via_json = parseFromJson<Feature>(json::parse(*protobuf_out), use_json);

    ASSERT_TRUE(from_json_via_protobuf.has_value());
    ASSERT_TRUE(from_protobuf_via_json.has_value());
    expectEqual(feature, *from_json_via_protobuf);
    expectEqual(feature, *from_protobuf_via_json);
}

TEST_F(UnitTest, FeatureRecord_ParseFromJson_JsonAndProtobufMatch)
{
    json json_feature = {
        {"name", "feature_alpha"},
        {"dimensions", json::array({
            json{{"transformations", json::array()}},
            json{{"transformations", json::array()}}
        })}
    };
    json json_input = {
        {"feature", json_feature},
        {"owner", "0xabc123"}
    };

    auto json_record = parseFromJson<FeatureRecord>(json_input, use_json);
    auto protobuf_record = parseFromJson<FeatureRecord>(json_input.dump(), use_protobuf);

    ASSERT_TRUE(json_record.has_value());
    ASSERT_TRUE(protobuf_record.has_value());
    expectEqual(*json_record, *protobuf_record);
}

TEST_F(UnitTest, FeatureRecord_ParseToJson_RoundTripAcrossParsers)
{
    FeatureRecord record = makeFeatureRecordSample();

    auto json_out = parseToJson(record, use_json);
    auto protobuf_out = parseToJson(record, use_protobuf);

    ASSERT_TRUE(json_out.has_value());
    ASSERT_TRUE(protobuf_out.has_value());

    auto from_json_via_protobuf = parseFromJson<FeatureRecord>(json_out->dump(), use_protobuf);
    auto from_protobuf_via_json = parseFromJson<FeatureRecord>(json::parse(*protobuf_out), use_json);

    ASSERT_TRUE(from_json_via_protobuf.has_value());
    ASSERT_TRUE(from_protobuf_via_json.has_value());
    expectEqual(record, *from_json_via_protobuf);
    expectEqual(record, *from_protobuf_via_json);
}

TEST_F(UnitTest, Feature_ConstructSolidityCode_UsesInitializerPattern)
{
    Feature feature = makeFeatureSample();
    std::string solidity = constructFeatureSolidityCode(feature);

    EXPECT_NE(solidity.find("function initialize(address registryAddr) external initializer"), std::string::npos);
    EXPECT_NE(solidity.find("__FeatureBase_init"), std::string::npos);
    EXPECT_NE(solidity.find("__FeatureBase_finalizeInit"), std::string::npos);
    EXPECT_EQ(solidity.find("constructor(address registryAddr)"), std::string::npos);
}
