#include "unit-tests.hpp"

using namespace dcn;
using namespace dcn::parse;
using namespace dcn::tests;

namespace
{
    RunningInstance makeRunningInstanceSample(std::uint32_t start_point, std::uint32_t transformation_shift)
    {
        RunningInstance running_instance;
        running_instance.set_start_point(start_point);
        running_instance.set_transformation_shift(transformation_shift);
        return running_instance;
    }

    ExecuteRequest makeExecuteRequestSample()
    {
        ExecuteRequest execute_request;
        execute_request.set_connector_name("connector_beta");
        execute_request.set_particles_count(256);
        (*execute_request.mutable_dynamic_ri())[0] = makeRunningInstanceSample(0, 2);
        (*execute_request.mutable_dynamic_ri())[3] = makeRunningInstanceSample(12, 4);
        return execute_request;
    }

    void expectEqual(const RunningInstance & lhs, const RunningInstance & rhs)
    {
        ASSERT_EQ(lhs.start_point(), rhs.start_point());
        ASSERT_EQ(lhs.transformation_shift(), rhs.transformation_shift());
    }

    void expectEqual(const ExecuteRequest & lhs, const ExecuteRequest & rhs)
    {
        ASSERT_EQ(lhs.connector_name(), rhs.connector_name());
        ASSERT_EQ(lhs.particles_count(), rhs.particles_count());
        ASSERT_EQ(lhs.dynamic_ri_size(), rhs.dynamic_ri_size());
        for(const auto & [position, running_instance] : lhs.dynamic_ri())
        {
            const auto it = rhs.dynamic_ri().find(position);
            ASSERT_TRUE(it != rhs.dynamic_ri().end());
            expectEqual(running_instance, it->second);
        }
    }
}

TEST_F(UnitTest, ExecuteRequest_ParseFromJson_JsonAndProtobufMatch)
{
    json json_input = {
        {"connector_name", "connector_beta"},
        {"particles_count", 256},
        {"dynamic_ri", json::object({
            {"0", json{{"start_point", 0}, {"transformation_shift", 2}}},
            {"3", json{{"start_point", 12}, {"transformation_shift", 4}}}
        })}
    };

    auto json_request = parseFromJson<ExecuteRequest>(json_input, use_json);
    auto protobuf_request = parseFromJson<ExecuteRequest>(json_input.dump(), use_protobuf);

    ASSERT_TRUE(json_request.has_value());
    ASSERT_TRUE(protobuf_request.has_value());
    expectEqual(*json_request, *protobuf_request);
}

TEST_F(UnitTest, ExecuteRequest_ParseToJson_RoundTripAcrossParsers)
{
    ExecuteRequest execute_request = makeExecuteRequestSample();

    auto json_out = parseToJson(execute_request, use_json);
    auto protobuf_out = parseToJson(execute_request, use_protobuf);

    ASSERT_TRUE(json_out.has_value());
    ASSERT_TRUE(protobuf_out.has_value());

    auto from_json_via_protobuf = parseFromJson<ExecuteRequest>(json_out->dump(), use_protobuf);
    auto from_protobuf_via_json = parseFromJson<ExecuteRequest>(json::parse(*protobuf_out), use_json);

    ASSERT_TRUE(from_json_via_protobuf.has_value());
    ASSERT_TRUE(from_protobuf_via_json.has_value());
    expectEqual(execute_request, *from_json_via_protobuf);
    expectEqual(execute_request, *from_protobuf_via_json);
}

TEST_F(UnitTest, ExecuteRequest_ParseFromJson_AcceptsPartialDynamicRiMap)
{
    json json_input = {
        {"connector_name", "connector_beta"},
        {"particles_count", 64},
        {"dynamic_ri", json::object({
            {"2", json{{"start_point", 5}, {"transformation_shift", 6}}}
        })}
    };

    auto json_request = parseFromJson<ExecuteRequest>(json_input, use_json);
    auto protobuf_request = parseFromJson<ExecuteRequest>(json_input.dump(), use_protobuf);

    ASSERT_TRUE(json_request.has_value());
    ASSERT_TRUE(protobuf_request.has_value());
    ASSERT_EQ(json_request->dynamic_ri_size(), 1);
    ASSERT_EQ(protobuf_request->dynamic_ri_size(), 1);

    const auto json_entry = json_request->dynamic_ri().find(2);
    const auto protobuf_entry = protobuf_request->dynamic_ri().find(2);
    ASSERT_NE(json_entry, json_request->dynamic_ri().end());
    ASSERT_NE(protobuf_entry, protobuf_request->dynamic_ri().end());
    EXPECT_EQ(json_entry->second.start_point(), 5);
    EXPECT_EQ(json_entry->second.transformation_shift(), 6);
    EXPECT_EQ(protobuf_entry->second.start_point(), 5);
    EXPECT_EQ(protobuf_entry->second.transformation_shift(), 6);
}

TEST_F(UnitTest, ExecuteRequest_ParseFromJson_RejectsNonCanonicalDynamicRiKey)
{
    json json_input = {
        {"connector_name", "connector_beta"},
        {"particles_count", 64},
        {"dynamic_ri", json::object({
            {"01", json{{"start_point", 5}, {"transformation_shift", 6}}}
        })}
    };

    auto request = parseFromJson<ExecuteRequest>(json_input, use_json);
    ASSERT_FALSE(request.has_value());
    EXPECT_EQ(request.error().kind, ParseError::Kind::INVALID_VALUE);
}

TEST_F(UnitTest, ExecuteRequest_ParseFromJson_DuplicateDynamicRiKeyInRawJson_UsesSingleMapEntry)
{
    const std::string raw_json_input = R"JSON(
{
  "connector_name": "connector_beta",
  "particles_count": 64,
  "dynamic_ri": {
    "3": {"start_point": 1, "transformation_shift": 2},
    "3": {"start_point": 7, "transformation_shift": 8}
  }
}
)JSON";

    auto json_request = parseFromJson<ExecuteRequest>(json::parse(raw_json_input), use_json);
    auto protobuf_request = parseFromJson<ExecuteRequest>(raw_json_input, use_protobuf);

    ASSERT_TRUE(json_request.has_value());
    ASSERT_FALSE(protobuf_request.has_value());
    EXPECT_EQ(protobuf_request.error().kind, ParseError::Kind::INVALID_VALUE);
    ASSERT_EQ(json_request->dynamic_ri_size(), 1);

    const auto json_entry = json_request->dynamic_ri().find(3);
    ASSERT_NE(json_entry, json_request->dynamic_ri().end());
    EXPECT_EQ(json_entry->second.start_point(), 7);
    EXPECT_EQ(json_entry->second.transformation_shift(), 8);
}

TEST_F(UnitTest, ExecuteRequest_ParseFromJson_RejectsMissingDynamicRi)
{
    json json_input = {
        {"connector_name", "connector_beta"},
        {"particles_count", 64}
    };

    auto request = parseFromJson<ExecuteRequest>(json_input, use_json);
    ASSERT_FALSE(request.has_value());
    EXPECT_EQ(request.error().kind, ParseError::Kind::INVALID_VALUE);
}
