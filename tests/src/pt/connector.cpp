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

        auto * dim0 = connector.add_dimensions();
        dim0->set_composite("comp_a");
        (*dim0->mutable_bindings())["0"] = "comp_slot_a";
        auto * tx0 = dim0->add_transformations();
        tx0->set_name("transform_a");
        tx0->add_args(1);

        auto * dim1 = connector.add_dimensions();
        dim1->set_composite("comp_b");
        (*dim1->mutable_bindings())["1"] = "comp_slot_b";
        auto * tx1 = dim1->add_transformations();
        tx1->set_name("transform_b");
        tx1->add_args(2);
        tx1->add_args(3);
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
        EXPECT_EQ(lhs.composite(), rhs.composite());
        ASSERT_EQ(lhs.bindings_size(), rhs.bindings_size());
        for(const auto & [slot, composite] : lhs.bindings())
        {
            const auto it = rhs.bindings().find(slot);
            ASSERT_NE(it, rhs.bindings().end());
            EXPECT_EQ(it->second, composite);
        }
        ASSERT_EQ(lhs.transformations_size(), rhs.transformations_size());
        for(int i = 0; i < lhs.transformations_size(); ++i)
        {
            expectEqual(lhs.transformations(i), rhs.transformations(i));
        }
    }

    void expectEqual(const Connector & lhs, const Connector & rhs)
    {
        ASSERT_EQ(lhs.name(), rhs.name());
        ASSERT_EQ(lhs.dimensions_size(), rhs.dimensions_size());
        for(int i = 0; i < lhs.dimensions_size(); ++i)
        {
            expectEqual(lhs.dimensions(i), rhs.dimensions(i));
        }

        ASSERT_EQ(lhs.condition_name(), rhs.condition_name());
        ASSERT_EQ(lhs.condition_args_size(), rhs.condition_args_size());
        for(int i = 0; i < lhs.condition_args_size(); ++i)
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
        {"dimensions", json::array({
            json{
                {"composite", "comp_a"},
                {"bindings", json{{"0", "comp_slot_a"}}},
                {"transformations", json::array({
                    json{{"name", "transform_a"}, {"args", json::array({1})}}
                })}
            },
            json{
                {"composite", "comp_b"},
                {"bindings", json{{"1", "comp_slot_b"}}},
                {"transformations", json::array({
                    json{{"name", "transform_b"}, {"args", json::array({2, 3})}}
                })}
            }
        })},
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

TEST_F(UnitTest, Connector_ParseFromJson_RejectsLegacySlotBindings)
{
    json json_input = {
        {"name", "connector_beta"},
        {"dimensions", json::array({
            json{
                {"composite", "comp_a"},
                {"slot_bindings", json{{"0", "TIME"}}},
                {"transformations", json::array()}
            }
        })},
        {"condition_name", ""},
        {"condition_args", json::array()}
    };

    auto connector = parseFromJson<Connector>(json_input, use_json);
    ASSERT_FALSE(connector.has_value());
}

TEST_F(UnitTest, Connector_ParseFromJson_RejectsNonIntegerTransformationArgs)
{
    json json_input = {
        {"name", "connector_beta"},
        {"dimensions", json::array({
            json{
                {"composite", ""},
                {"transformations", json::array({
                    json{{"name", "transform_a"}, {"args", json::array({"bad"})}}
                })}
            }
        })},
        {"condition_name", ""},
        {"condition_args", json::array()}
    };

    auto connector = parseFromJson<Connector>(json_input, use_json);
    ASSERT_FALSE(connector.has_value());
}

TEST_F(UnitTest, Connector_ParseFromJson_RejectsOutOfRangeConditionArgs)
{
    json json_input = {
        {"name", "connector_beta"},
        {"dimensions", json::array({
            json{
                {"composite", ""},
                {"transformations", json::array()}
            }
        })},
        {"condition_name", "condition_check"},
        {"condition_args", json::array({2147483648})}
    };

    auto connector = parseFromJson<Connector>(json_input, use_json);
    ASSERT_FALSE(connector.has_value());
}

TEST_F(UnitTest, Connector_ParseFromJson_RejectsScalarDimensionBindings)
{
    json json_input = {
        {"name", "connector_beta"},
        {"dimensions", json::array({
            json{
                {"composite", ""},
                {"bindings", json{{"0", "comp_slot_a"}}},
                {"transformations", json::array()}
            }
        })},
        {"condition_name", ""},
        {"condition_args", json::array()}
    };

    auto connector = parseFromJson<Connector>(json_input, use_json);
    ASSERT_FALSE(connector.has_value());
    EXPECT_EQ(connector.error().kind, ParseError::Kind::INVALID_VALUE);
}

TEST_F(UnitTest, Connector_ParseFromJson_RejectsNonCanonicalBindingSlotKey)
{
    json json_input = {
        {"name", "connector_beta"},
        {"dimensions", json::array({
            json{
                {"composite", "comp_a"},
                {"bindings", json{{"01", "comp_slot_a"}}},
                {"transformations", json::array()}
            }
        })},
        {"condition_name", ""},
        {"condition_args", json::array()}
    };

    auto connector = parseFromJson<Connector>(json_input, use_json);
    ASSERT_FALSE(connector.has_value());
    EXPECT_EQ(connector.error().kind, ParseError::Kind::INVALID_VALUE);
}

TEST_F(UnitTest, Connector_ParseFromJson_RejectsConnectorWithoutDimensionsAtParseBoundary)
{
    json json_input = {
        {"name", "connector_without_dimensions"},
        {"dimensions", json::array()},
        {"condition_name", ""},
        {"condition_args", json::array()}
    };

    auto json_connector = parseFromJson<Connector>(json_input, use_json);
    ASSERT_FALSE(json_connector.has_value());
    EXPECT_EQ(json_connector.error().kind, ParseError::Kind::INVALID_VALUE);

    auto protobuf_connector = parseFromJson<Connector>(json_input.dump(), use_protobuf);
    ASSERT_FALSE(protobuf_connector.has_value());
    EXPECT_EQ(protobuf_connector.error().kind, ParseError::Kind::INVALID_VALUE);
}

TEST_F(UnitTest, ConnectorRecord_ParseFromJson_JsonAndProtobufMatch)
{
    json json_connector = {
        {"name", "connector_beta"},
        {"dimensions", json::array({
            json{
                {"composite", "comp_a"},
                {"bindings", json{{"0", "comp_slot_a"}}},
                {"transformations", json::array({
                    json{{"name", "transform_a"}, {"args", json::array({1})}}
                })}
            },
            json{
                {"composite", "comp_b"},
                {"bindings", json{{"1", "comp_slot_b"}}},
                {"transformations", json::array({
                    json{{"name", "transform_b"}, {"args", json::array({2, 3})}}
                })}
            }
        })},
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

TEST_F(UnitTest, Connector_ConstructSolidityCode_UsesConstructorPattern)
{
    Connector connector = makeConnectorSample();
    auto solidity_result = constructConnectorSolidityCode(connector);
    ASSERT_TRUE(solidity_result.has_value());
    const std::string & solidity = *solidity_result;

    EXPECT_NE(solidity.find("constructor(address registryAddr) ConnectorBase("), std::string::npos);
    EXPECT_NE(solidity.find("__ConnectorBase_finalizeInit"), std::string::npos);
    EXPECT_NE(solidity.find("function _compositeDimIds()"), std::string::npos);
    EXPECT_NE(solidity.find("function _compositeNames()"), std::string::npos);
    EXPECT_NE(solidity.find("function _bindingDimIds()"), std::string::npos);
    EXPECT_NE(solidity.find("function _bindingSlotIds()"), std::string::npos);
    EXPECT_NE(solidity.find("function _bindingNames()"), std::string::npos);
    EXPECT_EQ(solidity.find("function initialize(address registryAddr) external initializer"), std::string::npos);
}

TEST_F(UnitTest, Connector_ConstructSolidityCode_RejectsNonNumericBindingSlots)
{
    Connector connector = makeConnectorSample();
    connector.mutable_dimensions(0)->clear_bindings();
    (*connector.mutable_dimensions(0)->mutable_bindings())["dim:0"] = "comp_slot_a";

    auto solidity_result = constructConnectorSolidityCode(connector);
    EXPECT_FALSE(solidity_result.has_value());
}

TEST_F(UnitTest, Connector_ConstructSolidityCode_RejectsConnectorWithoutDimensions)
{
    Connector connector;
    connector.set_name("connector_without_dimensions");

    auto solidity_result = constructConnectorSolidityCode(connector);
    ASSERT_FALSE(solidity_result.has_value());
    EXPECT_EQ(solidity_result.error().kind, ParseError::Kind::INVALID_VALUE);
}

TEST_F(UnitTest, Connector_ConstructSolidityCode_RejectsInvalidContractIdentifier)
{
    Connector connector = makeConnectorSample();
    connector.set_name("bad.connector");

    auto solidity_result = constructConnectorSolidityCode(connector);
    ASSERT_FALSE(solidity_result.has_value());
    EXPECT_EQ(solidity_result.error().kind, ParseError::Kind::INVALID_VALUE);
}

TEST_F(UnitTest, Connector_ConstructSolidityCode_RejectsReservedKeywordContractIdentifier)
{
    Connector connector = makeConnectorSample();
    connector.set_name("mapping");

    auto solidity_result = constructConnectorSolidityCode(connector);
    ASSERT_FALSE(solidity_result.has_value());
    EXPECT_EQ(solidity_result.error().kind, ParseError::Kind::INVALID_VALUE);
}

TEST_F(UnitTest, Connector_ConstructSolidityCode_RejectsTransformationWithEmptyName)
{
    Connector connector = makeConnectorSample();
    connector.mutable_dimensions(0)->mutable_transformations(0)->set_name("");

    auto solidity_result = constructConnectorSolidityCode(connector);
    ASSERT_FALSE(solidity_result.has_value());
    EXPECT_EQ(solidity_result.error().kind, ParseError::Kind::INVALID_VALUE);
}

TEST_F(UnitTest, Connector_ConstructSolidityCode_RejectsCanonicalDuplicateBindingSlots)
{
    Connector connector = makeConnectorSample();
    connector.mutable_dimensions(0)->clear_bindings();
    (*connector.mutable_dimensions(0)->mutable_bindings())["1"] = "comp_slot_a";
    (*connector.mutable_dimensions(0)->mutable_bindings())["01"] = "comp_slot_b";

    auto solidity_result = constructConnectorSolidityCode(connector);
    EXPECT_FALSE(solidity_result.has_value());
}

TEST_F(UnitTest, Connector_ConstructSolidityCode_EscapesBindingTargetNamesInStringLiterals)
{
    Connector connector = makeConnectorSample();
    connector.mutable_dimensions(0)->clear_bindings();
    (*connector.mutable_dimensions(0)->mutable_bindings())["0"] = "bad\"target\\name\nline";

    auto solidity_result = constructConnectorSolidityCode(connector);
    ASSERT_TRUE(solidity_result.has_value());
    const std::string & solidity = *solidity_result;

    EXPECT_NE(solidity.find("bindingNames[0] = \"bad\\\"target\\\\name\\nline\";"), std::string::npos);
}

TEST_F(UnitTest, Connector_ConstructSolidityCode_EscapesNonAsciiBindingTargetBytes)
{
    Connector connector = makeConnectorSample();
    connector.mutable_dimensions(0)->clear_bindings();

    std::string non_ascii_name = "raw_";
    non_ascii_name.push_back(static_cast<char>(0xC3));
    non_ascii_name.push_back(static_cast<char>(0xA9));
    non_ascii_name.push_back(static_cast<char>(0xFF));
    (*connector.mutable_dimensions(0)->mutable_bindings())["0"] = non_ascii_name;

    auto solidity_result = constructConnectorSolidityCode(connector);
    ASSERT_TRUE(solidity_result.has_value());
    const std::string & solidity = *solidity_result;

    EXPECT_NE(solidity.find("bindingNames[0] = \"raw_\\xC3\\xA9\\xFF\";"), std::string::npos);
}
