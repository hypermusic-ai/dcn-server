#pragma once

#include <chrono>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <format>
#include <fstream>
#include <optional>
#include <span>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

#include <sqlite3.h>
#include <nlohmann/json.hpp>

#include "decentralised_art.hpp"

#ifndef DECENTRALISED_ART_TEST_BINARY_DIR
    #error "DECENTRALISED_ART_TEST_BINARY_DIR is not defined"
#endif

namespace dcn::tests::events_harness
{
    using json = nlohmann::json;

    constexpr int CHAIN_ID = 1;

    template<class AwaitableT>
    auto runAwaitable(asio::io_context & io_context, AwaitableT awaitable)
    {
        auto future = asio::co_spawn(io_context, std::move(awaitable), asio::use_future);
        io_context.restart();
        io_context.run();
        return future.get();
    }

    inline std::filesystem::path buildPath()
    {
        return std::filesystem::path(DECENTRALISED_ART_TEST_BINARY_DIR);
    }

    struct TempEventsPaths
    {
        std::filesystem::path root;
        std::filesystem::path hot_db;
        std::filesystem::path archive_root;

        ~TempEventsPaths()
        {
            std::error_code ec;
            std::filesystem::remove_all(root, ec);
        }
    };

    inline TempEventsPaths makeTempEventsPaths(const std::string & test_name)
    {
        const std::string suffix = std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
        TempEventsPaths paths{};
        paths.root = buildPath() / "tests" / "events" / (test_name + "_" + suffix);
        paths.hot_db = paths.root / "events_hot.sqlite";
        paths.archive_root = paths.root / "archive";

        std::error_code ec;
        std::filesystem::remove_all(paths.root, ec);
        ec.clear();
        std::filesystem::create_directories(paths.archive_root, ec);
        if(ec)
        {
            throw std::runtime_error(std::format("Failed to create temp events test dir '{}': {}", paths.root.string(), ec.message()));
        }

        return paths;
    }

    class SqliteReadonly final
    {
        public:
            explicit SqliteReadonly(const std::filesystem::path & path)
            {
                const int open_rc = sqlite3_open_v2(
                    path.string().c_str(),
                    &_db,
                    SQLITE_OPEN_READONLY | SQLITE_OPEN_FULLMUTEX,
                    nullptr);
                if(open_rc != SQLITE_OK)
                {
                    const std::string err = (_db == nullptr) ? "sqlite open failed" : sqlite3_errmsg(_db);
                    if(_db != nullptr)
                    {
                        sqlite3_close(_db);
                        _db = nullptr;
                    }
                    throw std::runtime_error(std::format("Failed to open sqlite db '{}': {}", path.string(), err));
                }

                sqlite3_busy_timeout(_db, 10'000);
            }

            ~SqliteReadonly()
            {
                if(_db != nullptr)
                {
                    sqlite3_close(_db);
                    _db = nullptr;
                }
            }

            SqliteReadonly(const SqliteReadonly &) = delete;
            SqliteReadonly & operator=(const SqliteReadonly &) = delete;

            std::int64_t scalarInt64(const std::string & sql) const
            {
                sqlite3_stmt * stmt = nullptr;
                if(sqlite3_prepare_v2(_db, sql.c_str(), -1, &stmt, nullptr) != SQLITE_OK)
                {
                    throw std::runtime_error(sqlite3_errmsg(_db));
                }

                std::int64_t value = 0;
                if(sqlite3_step(stmt) == SQLITE_ROW)
                {
                    value = static_cast<std::int64_t>(sqlite3_column_int64(stmt, 0));
                }
                sqlite3_finalize(stmt);
                return value;
            }

            std::string scalarText(const std::string & sql) const
            {
                sqlite3_stmt * stmt = nullptr;
                if(sqlite3_prepare_v2(_db, sql.c_str(), -1, &stmt, nullptr) != SQLITE_OK)
                {
                    throw std::runtime_error(sqlite3_errmsg(_db));
                }

                std::string value;
                if(sqlite3_step(stmt) == SQLITE_ROW)
                {
                    const unsigned char * txt = sqlite3_column_text(stmt, 0);
                    if(txt != nullptr)
                    {
                        value = reinterpret_cast<const char *>(txt);
                    }
                }
                sqlite3_finalize(stmt);
                return value;
            }

            json scalarJson(const std::string & sql) const
            {
                const std::string txt = scalarText(sql);
                const json parsed = json::parse(txt, nullptr, false);
                if(parsed.is_discarded())
                {
                    return json::object();
                }
                return parsed;
            }

        private:
            sqlite3 * _db = nullptr;
    };

    inline std::int64_t rowCount(const std::filesystem::path & db_path, const std::string & table_name)
    {
        SqliteReadonly db(db_path);
        return db.scalarInt64(std::format("SELECT COUNT(1) FROM {};", table_name));
    }

    inline std::string hexBytes(const std::uint8_t value, const std::size_t bytes)
    {
        std::string out = "0x";
        out.reserve(2 + bytes * 2);
        constexpr char LUT[] = "0123456789abcdef";
        for(std::size_t i = 0; i < bytes; ++i)
        {
            out.push_back(LUT[(value >> 4) & 0x0F]);
            out.push_back(LUT[value & 0x0F]);
        }
        return out;
    }

    inline chain::Address makeAddressFromByte(const std::uint8_t value)
    {
        chain::Address address{};
        address.bytes[19] = value;
        return address;
    }

    inline std::string hexAddress(const std::uint8_t value)
    {
        return chain::normalizeHex(evmc::hex(makeAddressFromByte(value)));
    }

    inline std::string topicForEvent(const std::string & signature)
    {
        return chain::normalizeHex(evmc::hex(chain::constructEventTopic(signature)));
    }

    inline std::vector<std::uint8_t> encodeUint256Word(const std::uint64_t value)
    {
        std::vector<std::uint8_t> out(32, 0);
        for(int i = 0; i < 8; ++i)
        {
            out[31 - i] = static_cast<std::uint8_t>((value >> (8 * i)) & 0xFFu);
        }
        return out;
    }

    inline std::vector<std::uint8_t> encodeAddressWord(const chain::Address & value)
    {
        std::vector<std::uint8_t> out(32, 0);
        std::memcpy(out.data() + 12, value.bytes, 20);
        return out;
    }

    inline std::vector<std::uint8_t> encodeStringTail(const std::string & value)
    {
        std::vector<std::uint8_t> out = encodeUint256Word(value.size());
        out.insert(out.end(), value.begin(), value.end());
        const std::size_t pad = (32 - (value.size() % 32)) % 32;
        out.insert(out.end(), pad, 0);
        return out;
    }

    inline std::string toHexPrefixed(std::span<const std::uint8_t> bytes)
    {
        return std::string("0x") + evmc::hex(evmc::bytes_view{bytes.data(), bytes.size()});
    }

    inline std::string encodeSimpleAddedEventDataV2(
        const chain::Address & caller,
        const std::string & name,
        const chain::Address & entity_address,
        const chain::Address & owner,
        const std::uint32_t count)
    {
        std::vector<std::uint8_t> out;
        const auto name_tail = encodeStringTail(name);

        const auto caller_word = encodeAddressWord(caller);
        const auto name_offset_word = encodeUint256Word(160);
        const auto entity_word = encodeAddressWord(entity_address);
        const auto owner_word = encodeAddressWord(owner);
        const auto count_word = encodeUint256Word(count);

        out.insert(out.end(), caller_word.begin(), caller_word.end());
        out.insert(out.end(), name_offset_word.begin(), name_offset_word.end());
        out.insert(out.end(), entity_word.begin(), entity_word.end());
        out.insert(out.end(), owner_word.begin(), owner_word.end());
        out.insert(out.end(), count_word.begin(), count_word.end());
        out.insert(out.end(), name_tail.begin(), name_tail.end());

        return toHexPrefixed(std::span<const std::uint8_t>(out.data(), out.size()));
    }

    inline events::RawChainLog makeRawLog(
        const std::int64_t block_number,
        const std::int64_t tx_index,
        const std::int64_t log_index,
        std::string block_hash,
        std::string tx_hash,
        std::string topic0,
        std::string data_hex,
        const bool removed,
        const std::optional<std::int64_t> block_time,
        const std::int64_t seen_at_ms)
    {
        events::RawChainLog log{};
        log.chain_id = CHAIN_ID;
        log.block_number = block_number;
        log.block_hash = std::move(block_hash);
        log.parent_hash = hexBytes(0x7F, 32);
        log.tx_index = tx_index;
        log.tx_hash = std::move(tx_hash);
        log.log_index = log_index;
        log.address = hexAddress(0xAB);
        log.topics[0] = std::move(topic0);
        log.data_hex = std::move(data_hex);
        log.removed = removed;
        log.block_time = block_time;
        log.seen_at_ms = seen_at_ms;
        return log;
    }

    inline events::DecodedEvent makeDecodedEvent(
        const std::int64_t block_number,
        const std::int64_t tx_index,
        const std::int64_t log_index,
        const std::uint8_t block_hash_byte,
        const std::uint8_t tx_hash_byte,
        const events::EventType event_type,
        const events::EventState state = events::EventState::OBSERVED,
        const std::optional<std::int64_t> block_time = std::nullopt,
        const std::int64_t seen_at_ms = 1'000)
    {
        events::DecodedEvent event{};
        event.raw = makeRawLog(
            block_number,
            tx_index,
            log_index,
            hexBytes(block_hash_byte, 32),
            hexBytes(tx_hash_byte, 32),
            topicForEvent("TransformationAdded(address,string,address,address,uint32)"),
            "0x00",
            state == events::EventState::REMOVED,
            block_time,
            seen_at_ms);
        event.event_type = event_type;
        event.state = state;
        event.name = std::format("entity_{}_{}", block_number, log_index);
        event.caller = hexAddress(0x41);
        event.owner = hexAddress(0x42);
        event.entity_address = hexAddress(0x43);
        event.args_count = 2;
        event.format_hash = hexBytes(0x11, 32);
        event.decoded_json = json{
            {"name", event.name},
            {"caller", event.caller},
            {"owner", event.owner},
            {"entity_address", event.entity_address}
        }.dump();
        return event;
    }

    inline events::ChainBlockInfo makeBlockInfo(
        const std::int64_t block_number,
        std::string block_hash,
        std::string parent_hash,
        const std::int64_t block_time,
        const std::int64_t seen_at_ms)
    {
        events::ChainBlockInfo info{};
        info.chain_id = CHAIN_ID;
        info.block_number = block_number;
        info.block_hash = std::move(block_hash);
        info.parent_hash = std::move(parent_hash);
        info.block_time = block_time;
        info.seen_at_ms = seen_at_ms;
        return info;
    }

    inline std::size_t projectAll(events::SQLiteHotStore & store, const std::int64_t now_ms)
    {
        std::size_t total = 0;
        for(std::size_t i = 0; i < 1024; ++i)
        {
            const std::size_t projected = store.projectBatch(512, now_ms);
            total += projected;
            if(projected == 0)
            {
                break;
            }
        }
        return total;
    }

    inline server::RouteArg makeUintQueryArg(const std::size_t value)
    {
        return server::RouteArg(
            server::RouteArgDef(server::RouteArgType::unsigned_integer, server::RouteArgRequirement::required),
            std::to_string(value));
    }

    inline server::RouteArg makeStringQueryArg(const std::string & value)
    {
        return server::RouteArg(
            server::RouteArgDef(server::RouteArgType::string, server::RouteArgRequirement::required),
            value);
    }

    struct SseFrame
    {
        bool is_comment = false;
        std::string comment;
        std::optional<std::int64_t> id = std::nullopt;
        std::string event;
        std::string data;
    };

    inline std::vector<SseFrame> parseSseFrames(const std::string & body)
    {
        std::vector<SseFrame> frames;

        std::size_t offset = 0;
        while(offset < body.size())
        {
            std::size_t block_end = body.find("\n\n", offset);
            if(block_end == std::string::npos)
            {
                block_end = body.size();
            }

            const std::string block = body.substr(offset, block_end - offset);
            offset = (block_end == body.size()) ? body.size() : block_end + 2;

            if(block.empty())
            {
                continue;
            }

            SseFrame frame{};
            std::istringstream lines(block);
            std::string line;
            while(std::getline(lines, line))
            {
                if(!line.empty() && line.back() == '\r')
                {
                    line.pop_back();
                }

                if(line.empty())
                {
                    continue;
                }

                if(line[0] == ':')
                {
                    frame.is_comment = true;
                    frame.comment = line.substr(1);
                    if(!frame.comment.empty() && frame.comment[0] == ' ')
                    {
                        frame.comment.erase(frame.comment.begin());
                    }
                    continue;
                }

                if(line.rfind("id:", 0) == 0)
                {
                    const std::string value = line.substr(3);
                    const auto trimmed = utils::trimAsciiWhitespace(value);
                    if(trimmed.has_value())
                    {
                        try
                        {
                            frame.id = std::stoll(*trimmed);
                        }
                        catch(...)
                        {
                            frame.id = std::nullopt;
                        }
                    }
                    continue;
                }

                if(line.rfind("event:", 0) == 0)
                {
                    frame.event = line.substr(6);
                    if(!frame.event.empty() && frame.event[0] == ' ')
                    {
                        frame.event.erase(frame.event.begin());
                    }
                    continue;
                }

                if(line.rfind("data:", 0) == 0)
                {
                    frame.data = line.substr(5);
                    if(!frame.data.empty() && frame.data[0] == ' ')
                    {
                        frame.data.erase(frame.data.begin());
                    }
                    continue;
                }
            }

            frames.push_back(std::move(frame));
        }

        return frames;
    }
}
