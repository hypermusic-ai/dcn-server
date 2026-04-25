#include "unit-tests.hpp"

#include "events_test_harness.hpp"

using namespace dcn;
using namespace dcn::tests;
using namespace dcn::tests::events_harness;

TEST_F(UnitTest, Events_Decoder_TransformationEvent_DecodesCanonicalFields)
{
    events::PTEventDecoder decoder;

    const chain::Address caller = makeAddressFromByte(0x11);
    const chain::Address owner = makeAddressFromByte(0x22);
    const chain::Address entity = makeAddressFromByte(0x33);
    const std::string topic0 = topicForEvent("TransformationAdded(address,string,address,address,uint32)");
    const std::string data = encodeSimpleAddedEventDataV2(caller, "TransformAlpha", entity, owner, 3);

    events::RawChainLog log = makeRawLog(
        100,
        2,
        5,
        hexBytes(0xA1, 32),
        hexBytes(0xB1, 32),
        topic0,
        data,
        false,
        1'700'000'000,
        1'700'000'001'000);

    const auto decoded = decoder.decode(log);
    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(decoded->event_type, events::EventType::TRANSFORMATION_ADDED);
    EXPECT_EQ(decoded->state, events::EventState::OBSERVED);
    EXPECT_EQ(decoded->name, "TransformAlpha");
    EXPECT_EQ(decoded->caller, chain::normalizeHex(evmc::hex(caller)));
    EXPECT_EQ(decoded->owner, chain::normalizeHex(evmc::hex(owner)));
    EXPECT_EQ(decoded->entity_address, chain::normalizeHex(evmc::hex(entity)));
    ASSERT_TRUE(decoded->args_count.has_value());
    EXPECT_EQ(*decoded->args_count, 3u);

    const auto payload = json::parse(decoded->decoded_json, nullptr, false);
    ASSERT_FALSE(payload.is_discarded());
    EXPECT_EQ(payload.value("name", ""), "TransformAlpha");
    EXPECT_EQ(payload.value("caller", ""), chain::normalizeHex(evmc::hex(caller)));
    EXPECT_EQ(payload.value("owner", ""), chain::normalizeHex(evmc::hex(owner)));
    EXPECT_EQ(payload.value("transformation_address", ""), chain::normalizeHex(evmc::hex(entity)));
    EXPECT_EQ(payload.value("args_count", 0), 3);
}

TEST_F(UnitTest, Events_Decoder_UnsupportedTopic_ReturnsNullopt)
{
    events::PTEventDecoder decoder;

    events::RawChainLog log = makeRawLog(
        100,
        0,
        0,
        hexBytes(0xA2, 32),
        hexBytes(0xB2, 32),
        topicForEvent("CompletelyUnknown(address,uint256)"),
        "0x00",
        false,
        1'700'000'010,
        1'700'000'011'000);

    const auto decoded = decoder.decode(log);
    EXPECT_FALSE(decoded.has_value());
}

TEST_F(UnitTest, Events_Decoder_MalformedPayload_ReturnsNullopt)
{
    events::PTEventDecoder decoder;

    events::RawChainLog log = makeRawLog(
        100,
        0,
        0,
        hexBytes(0xA3, 32),
        hexBytes(0xB3, 32),
        topicForEvent("ConditionAdded(address,string,address,address,uint32)"),
        "0x1234",
        false,
        1'700'000'020,
        1'700'000'021'000);

    const auto decoded = decoder.decode(log);
    EXPECT_FALSE(decoded.has_value());
}

TEST_F(UnitTest, Events_Decoder_RemovedFlag_MapsToRemovedState)
{
    events::PTEventDecoder decoder;

    const chain::Address caller = makeAddressFromByte(0x15);
    const chain::Address owner = makeAddressFromByte(0x16);
    const chain::Address entity = makeAddressFromByte(0x17);

    events::RawChainLog log = makeRawLog(
        101,
        1,
        2,
        hexBytes(0xA4, 32),
        hexBytes(0xB4, 32),
        topicForEvent("ConditionAdded(address,string,address,address,uint32)"),
        encodeSimpleAddedEventDataV2(caller, "ConditionAlpha", entity, owner, 1),
        true,
        1'700'000'030,
        1'700'000'031'000);

    const auto decoded = decoder.decode(log);
    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(decoded->event_type, events::EventType::CONDITION_ADDED);
    EXPECT_EQ(decoded->state, events::EventState::REMOVED);
}

TEST_F(UnitTest, Events_Decoder_MissingTopic0_ReturnsNullopt)
{
    events::PTEventDecoder decoder;

    events::RawChainLog log{};
    log.chain_id = CHAIN_ID;
    log.block_number = 10;
    log.block_hash = hexBytes(0xAA, 32);
    log.tx_hash = hexBytes(0xBB, 32);
    log.log_index = 1;
    log.tx_index = 1;
    log.address = hexAddress(0x11);
    log.data_hex = "0x";
    log.seen_at_ms = 1'700'000'032'000;

    const auto decoded = decoder.decode(log);
    EXPECT_FALSE(decoded.has_value());
}
