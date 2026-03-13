#include "unit-tests.hpp"

using namespace dcn;
using namespace dcn::parse;
using namespace dcn::tests;

namespace
{
    Connector makeConnectorSample()
    {
        Connector connector;
        connector.set_name("connector_beta");
        connector.set_feature_name("feature_alpha");
        (*connector.mutable_composites())[0] = "comp_a";
        (*connector.mutable_composites())[2] = "comp_b";
        connector.set_condition_name("condition_check");
        connector.add_condition_args(1);
        connector.add_condition_args(2);
        return connector;
    }

    ConnectorRecord makeConnectorRecordSample()
    {
        ConnectorRecord record;
        *record.mutable_connector() = makeConnectorSample();
        record.set_owner("0xabc123");
        return record;
    }

    void expectEqual(const Connector & lhs, const Connector & rhs)
    {
        ASSERT_EQ(lhs.name(), rhs.name());
        ASSERT_EQ(lhs.feature_name(), rhs.feature_name());
        ASSERT_EQ(lhs.composites().size(), rhs.composites().size());
        for (const auto & [dim_id, composite_name] : lhs.composites())
        {
            const auto it = rhs.composites().find(dim_id);
            ASSERT_NE(it, rhs.composites().end());
            EXPECT_EQ(composite_name, it->second);
        }
        ASSERT_EQ(lhs.condition_name(), rhs.condition_name());
        ASSERT_EQ(lhs.condition_args_size(), rhs.condition_args_size());
        for (int i = 0; i < lhs.condition_args_size(); ++i)
        {
            EXPECT_EQ(lhs.condition_args(i), rhs.condition_args(i));
        }
    }

    void expectEqual(const ConnectorRecord & lhs, const ConnectorRecord & rhs)
    {
        expectEqual(lhs.connector(), rhs.connector());
        EXPECT_EQ(lhs.owner(), rhs.owner());
    }
}

TEST_F(UnitTest, Connector_ParseFromJson_JsonAndProtobufMatch)
{
    json json_input = {
        {"name", "connector_beta"},
        {"feature_name", "feature_alpha"},
        {"composites", json::object({{"0", "comp_a"}, {"2", "comp_b"}})},
        {"condition_name", "condition_check"},
        {"condition_args", json::array({1, 2})}
    };

    auto json_connector = parseFromJson<Connector>(json_input, use_json);
    auto protobuf_connector = parseFromJson<Connector>(json_input.dump(), use_protobuf);

    ASSERT_TRUE(json_connector.has_value());
    ASSERT_TRUE(protobuf_connector.has_value());
    expectEqual(*json_connector, *protobuf_connector);
}

TEST_F(UnitTest, Connector_ParseToJson_RoundTripAcrossParsers)
{
    Connector connector = makeConnectorSample();

    auto json_out = parseToJson(connector, use_json);
    auto protobuf_out = parseToJson(connector, use_protobuf);

    ASSERT_TRUE(json_out.has_value());
    ASSERT_TRUE(protobuf_out.has_value());

    auto from_json_via_protobuf = parseFromJson<Connector>(json_out->dump(), use_protobuf);
    auto from_protobuf_via_json = parseFromJson<Connector>(json::parse(*protobuf_out), use_json);

    ASSERT_TRUE(from_json_via_protobuf.has_value());
    ASSERT_TRUE(from_protobuf_via_json.has_value());
    expectEqual(connector, *from_json_via_protobuf);
    expectEqual(connector, *from_protobuf_via_json);
}

TEST_F(UnitTest, ConnectorRecord_ParseFromJson_JsonAndProtobufMatch)
{
    json json_connector = {
        {"name", "connector_beta"},
        {"feature_name", "feature_alpha"},
        {"composites", json::object({{"0", "comp_a"}, {"2", "comp_b"}})},
        {"condition_name", "condition_check"},
        {"condition_args", json::array({1, 2})}
    };
    json json_input = {
        {"connector", json_connector},
        {"owner", "0xabc123"}
    };

    auto json_record = parseFromJson<ConnectorRecord>(json_input, use_json);
    auto protobuf_record = parseFromJson<ConnectorRecord>(json_input.dump(), use_protobuf);

    ASSERT_TRUE(json_record.has_value());
    ASSERT_TRUE(protobuf_record.has_value());
    expectEqual(*json_record, *protobuf_record);
}

TEST_F(UnitTest, ConnectorRecord_ParseToJson_RoundTripAcrossParsers)
{
    ConnectorRecord record = makeConnectorRecordSample();

    auto json_out = parseToJson(record, use_json);
    auto protobuf_out = parseToJson(record, use_protobuf);

    ASSERT_TRUE(json_out.has_value());
    ASSERT_TRUE(protobuf_out.has_value());

    auto from_json_via_protobuf = parseFromJson<ConnectorRecord>(json_out->dump(), use_protobuf);
    auto from_protobuf_via_json = parseFromJson<ConnectorRecord>(json::parse(*protobuf_out), use_json);

    ASSERT_TRUE(from_json_via_protobuf.has_value());
    ASSERT_TRUE(from_protobuf_via_json.has_value());
    expectEqual(record, *from_json_via_protobuf);
    expectEqual(record, *from_protobuf_via_json);
}

TEST_F(UnitTest, Connector_ConstructSolidityCode_UsesInitializerPattern)
{
    Connector connector = makeConnectorSample();
    std::string solidity = constructConnectorSolidityCode(connector);

    EXPECT_NE(solidity.find("function initialize(address registryAddr) external initializer"), std::string::npos);
    EXPECT_NE(solidity.find("__ConnectorBase_init"), std::string::npos);
    EXPECT_NE(solidity.find("function _compositeDimIds()"), std::string::npos);
    EXPECT_NE(solidity.find("function _compositeNames()"), std::string::npos);
    EXPECT_NE(solidity.find("_compositeDimIds(), _compositeNames()"), std::string::npos);
    EXPECT_EQ(solidity.find("constructor(address registryAddr)"), std::string::npos);
}