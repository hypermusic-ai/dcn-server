#include "unit-tests.hpp"

using namespace dcn;
using namespace dcn::parse;
using namespace dcn::tests;

namespace
{
    Transformation makeTransformationSample()
    {
        Transformation transformation;
        transformation.set_name("transform_add");
        transformation.set_sol_src("return x + args[0];");
        return transformation;
    }

    TransformationRecord makeTransformationRecordSample()
    {
        TransformationRecord record;
        *record.mutable_transformation() = makeTransformationSample();
        record.set_owner("0xabc123");
        return record;
    }

    void expectEqual(const Transformation & lhs, const Transformation & rhs)
    {
        ASSERT_EQ(lhs.name(), rhs.name());
        ASSERT_EQ(lhs.sol_src(), rhs.sol_src());
    }

    void expectEqual(const TransformationRecord & lhs, const TransformationRecord & rhs)
    {
        expectEqual(lhs.transformation(), rhs.transformation());
        EXPECT_EQ(lhs.owner(), rhs.owner());
    }
}

TEST_F(UnitTest, Transformation_ParseFromJson_JsonAndProtobufMatch)
{
    json json_input = {
        {"name", "transform_add"},
        {"sol_src", "return x + args[0];"}
    };

    auto json_transformation = parseFromJson<Transformation>(json_input, use_json);
    auto protobuf_transformation = parseFromJson<Transformation>(json_input.dump(), use_protobuf);

    ASSERT_TRUE(json_transformation.has_value());
    ASSERT_TRUE(protobuf_transformation.has_value());
    expectEqual(*json_transformation, *protobuf_transformation);
}

TEST_F(UnitTest, Transformation_ParseToJson_RoundTripAcrossParsers)
{
    Transformation transformation = makeTransformationSample();

    auto json_out = parseToJson(transformation, use_json);
    auto protobuf_out = parseToJson(transformation, use_protobuf);

    ASSERT_TRUE(json_out.has_value());
    ASSERT_TRUE(protobuf_out.has_value());

    auto from_json_via_protobuf = parseFromJson<Transformation>(json_out->dump(), use_protobuf);
    auto from_protobuf_via_json = parseFromJson<Transformation>(json::parse(*protobuf_out), use_json);

    ASSERT_TRUE(from_json_via_protobuf.has_value());
    ASSERT_TRUE(from_protobuf_via_json.has_value());
    expectEqual(transformation, *from_json_via_protobuf);
    expectEqual(transformation, *from_protobuf_via_json);
}

TEST_F(UnitTest, TransformationRecord_ParseFromJson_JsonAndProtobufMatch)
{
    json json_transformation = {
        {"name", "transform_add"},
        {"sol_src", "return x + args[0];"}
    };
    json json_input = {
        {"transformation", json_transformation},
        {"owner", "0xabc123"}
    };

    auto json_record = parseFromJson<TransformationRecord>(json_input, use_json);
    auto protobuf_record = parseFromJson<TransformationRecord>(json_input.dump(), use_protobuf);

    ASSERT_TRUE(json_record.has_value());
    ASSERT_TRUE(protobuf_record.has_value());
    expectEqual(*json_record, *protobuf_record);
}

TEST_F(UnitTest, TransformationRecord_ParseToJson_RoundTripAcrossParsers)
{
    TransformationRecord record = makeTransformationRecordSample();

    auto json_out = parseToJson(record, use_json);
    auto protobuf_out = parseToJson(record, use_protobuf);

    ASSERT_TRUE(json_out.has_value());
    ASSERT_TRUE(protobuf_out.has_value());

    auto from_json_via_protobuf = parseFromJson<TransformationRecord>(json_out->dump(), use_protobuf);
    auto from_protobuf_via_json = parseFromJson<TransformationRecord>(json::parse(*protobuf_out), use_json);

    ASSERT_TRUE(from_json_via_protobuf.has_value());
    ASSERT_TRUE(from_protobuf_via_json.has_value());
    expectEqual(record, *from_json_via_protobuf);
    expectEqual(record, *from_protobuf_via_json);
}
