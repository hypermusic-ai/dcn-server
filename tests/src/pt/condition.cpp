#include "unit-tests.hpp"

using namespace dcn;
using namespace dcn::parse;
using namespace dcn::tests;

namespace
{
    Condition makeConditionSample()
    {
        Condition condition;
        condition.set_name("condition_check");
        condition.set_sol_src("return true;");
        return condition;
    }

    ConditionRecord makeConditionRecordSample()
    {
        ConditionRecord record;
        *record.mutable_condition() = makeConditionSample();
        record.set_owner("0xabc123");
        return record;
    }

    void expectEqual(const Condition & lhs, const Condition & rhs)
    {
        ASSERT_EQ(lhs.name(), rhs.name());
        ASSERT_EQ(lhs.sol_src(), rhs.sol_src());
    }

    void expectEqual(const ConditionRecord & lhs, const ConditionRecord & rhs)
    {
        expectEqual(lhs.condition(), rhs.condition());
        EXPECT_EQ(lhs.owner(), rhs.owner());
    }
}

TEST_F(UnitTest, Condition_ParseFromJson_JsonAndProtobufMatch)
{
    json json_input = {
        {"name", "condition_check"},
        {"sol_src", "return true;"}
    };

    auto json_condition = parseFromJson<Condition>(json_input, use_json);
    auto protobuf_condition = parseFromJson<Condition>(json_input.dump(), use_protobuf);

    ASSERT_TRUE(json_condition.has_value());
    ASSERT_TRUE(protobuf_condition.has_value());
    expectEqual(*json_condition, *protobuf_condition);
}

TEST_F(UnitTest, Condition_ParseToJson_RoundTripAcrossParsers)
{
    Condition condition = makeConditionSample();

    auto json_out = parseToJson(condition, use_json);
    auto protobuf_out = parseToJson(condition, use_protobuf);

    ASSERT_TRUE(json_out.has_value());
    ASSERT_TRUE(protobuf_out.has_value());

    auto from_json_via_protobuf = parseFromJson<Condition>(json_out->dump(), use_protobuf);
    auto from_protobuf_via_json = parseFromJson<Condition>(json::parse(*protobuf_out), use_json);

    ASSERT_TRUE(from_json_via_protobuf.has_value());
    ASSERT_TRUE(from_protobuf_via_json.has_value());
    expectEqual(condition, *from_json_via_protobuf);
    expectEqual(condition, *from_protobuf_via_json);
}

TEST_F(UnitTest, ConditionRecord_ParseFromJson_JsonAndProtobufMatch)
{
    json json_condition = {
        {"name", "condition_check"},
        {"sol_src", "return true;"}
    };
    json json_input = {
        {"condition", json_condition},
        {"owner", "0xabc123"}
    };

    auto json_record = parseFromJson<ConditionRecord>(json_input, use_json);
    auto protobuf_record = parseFromJson<ConditionRecord>(json_input.dump(), use_protobuf);

    ASSERT_TRUE(json_record.has_value());
    ASSERT_TRUE(protobuf_record.has_value());
    expectEqual(*json_record, *protobuf_record);
}

TEST_F(UnitTest, ConditionRecord_ParseToJson_RoundTripAcrossParsers)
{
    ConditionRecord record = makeConditionRecordSample();

    auto json_out = parseToJson(record, use_json);
    auto protobuf_out = parseToJson(record, use_protobuf);

    ASSERT_TRUE(json_out.has_value());
    ASSERT_TRUE(protobuf_out.has_value());

    auto from_json_via_protobuf = parseFromJson<ConditionRecord>(json_out->dump(), use_protobuf);
    auto from_protobuf_via_json = parseFromJson<ConditionRecord>(json::parse(*protobuf_out), use_json);

    ASSERT_TRUE(from_json_via_protobuf.has_value());
    ASSERT_TRUE(from_protobuf_via_json.has_value());
    expectEqual(record, *from_json_via_protobuf);
    expectEqual(record, *from_protobuf_via_json);
}
