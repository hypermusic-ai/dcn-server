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
        execute_request.set_particle_name("particle_beta");
        execute_request.set_samples_count(256);
        *execute_request.add_running_instances() = makeRunningInstanceSample(0, 2);
        *execute_request.add_running_instances() = makeRunningInstanceSample(12, 4);
        return execute_request;
    }

    void expectEqual(const RunningInstance & lhs, const RunningInstance & rhs)
    {
        ASSERT_EQ(lhs.start_point(), rhs.start_point());
        ASSERT_EQ(lhs.transformation_shift(), rhs.transformation_shift());
    }

    void expectEqual(const ExecuteRequest & lhs, const ExecuteRequest & rhs)
    {
        ASSERT_EQ(lhs.particle_name(), rhs.particle_name());
        ASSERT_EQ(lhs.samples_count(), rhs.samples_count());
        ASSERT_EQ(lhs.running_instances_size(), rhs.running_instances_size());
        for(int i = 0; i < lhs.running_instances_size(); ++i)
        {
            expectEqual(lhs.running_instances(i), rhs.running_instances(i));
        }
    }
}

TEST_F(UnitTest, ExecuteRequest_ParseFromJson_JsonAndProtobufMatch)
{
    json json_input = {
        {"particle_name", "particle_beta"},
        {"samples_count", 256},
        {"running_instances", json::array({
            json{{"start_point", 0}, {"transformation_shift", 2}},
            json{{"start_point", 12}, {"transformation_shift", 4}}
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
