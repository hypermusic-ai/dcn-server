#include <algorithm>
#include <chrono>
#include <ranges>
#include <string_view>
#include <unordered_map>
#include <format>

#include <spdlog/spdlog.h>

#include "utils.hpp"
#include "sqlite/statement.hpp"
#include "sqlite/exec.hpp"

#include "sqlite_hot_store.hpp"

namespace dcn::events
{
    constexpr int CURRENT_PROJECTOR_VERSION = 1;

    static bool _usesLogicalFeedIdentity(const std::string_view event_type)
    {
        return event_type == CONNECTOR_ADDED_TYPE
            || event_type == TRANSFORMATION_ADDED_TYPE
            || event_type == CONDITION_ADDED_TYPE;
    }

    static std::string _logicalFeedId(
        const std::string & chain_namespace,
        const int chain_id,
        const std::string & event_type,
        const std::string & name)
    {
        return std::format("{}:{}:{}:{}", chain_namespace, chain_id, event_type, name);
    }

    static std::string _projectedFeedId(
        const std::string & chain_namespace,
        const int chain_id,
        const std::string & tx_hash,
        const std::int64_t log_index,
        const std::string & event_type,
        const std::string & name)
    {
        if(_usesLogicalFeedIdentity(event_type))
        {
            return _logicalFeedId(chain_namespace, chain_id, event_type, name);
        }

        return std::format("{}:{}:{}:{}", chain_namespace, chain_id, tx_hash, log_index);
    }

    static std::string _compactFeedPayloadJson(
        const std::string& event_type,
        const std::string& name,
        const std::string& owner)
    {
        std::string type;

        if (event_type == toString(EventType::CONNECTOR_ADDED))
        {
            type = "connector";
        }
        else if (event_type == toString(EventType::TRANSFORMATION_ADDED))
        {
            type = "transformation";
        }
        else if (event_type == toString(EventType::CONDITION_ADDED))
        {
            type = "condition";
        }
        else
        {
            type = event_type;
        }

        json payload{
            {"type", type},
            {"name", name},
            {"owner", owner}
        };

        return payload.dump(-1, ' ', false, json::error_handler_t::replace);
    }

    static std::optional<std::int64_t> _columnInt64Optional(sqlite3_stmt * stmt, const int index)
    {
        if(sqlite3_column_type(stmt, index) == SQLITE_NULL)
        {
            return std::nullopt;
        }
        return static_cast<std::int64_t>(sqlite3_column_int64(stmt, index));
    }

    static std::optional<std::string> _columnTextOptional(sqlite3_stmt * stmt, const int index)
    {
        const unsigned char * txt = sqlite3_column_text(stmt, index);
        if(txt == nullptr)
        {
            return std::nullopt;
        }
        return std::string(reinterpret_cast<const char *>(txt));
    }

    static int _bindOptionalInt64(sqlite3_stmt * stmt, int index, const std::optional<std::int64_t> & value)
    {
        if(!value.has_value())
        {
            return sqlite3_bind_null(stmt, index);
        }
        return sqlite3_bind_int64(stmt, index, static_cast<sqlite3_int64>(*value));
    }

    static int _bindOptionalText(sqlite3_stmt * stmt, int index, const std::optional<std::string> & value)
    {
        if(!value.has_value())
        {
            return sqlite3_bind_null(stmt, index);
        }
        return sqlite3_bind_text(stmt, index, value->c_str(), static_cast<int>(value->size()), SQLITE_TRANSIENT);
    }

    static bool _feedDescComparator(const FeedItem & lhs, const FeedItem & rhs)
    {
        if(lhs.created_at_ms != rhs.created_at_ms)
        {
            return lhs.created_at_ms > rhs.created_at_ms;
        }
        if(lhs.block_number != rhs.block_number)
        {
            return lhs.block_number > rhs.block_number;
        }
        if(lhs.tx_index != rhs.tx_index)
        {
            return lhs.tx_index > rhs.tx_index;
        }
        return lhs.feed_id > rhs.feed_id;
    }

    static bool _cursorLessInDescOrder(const FeedItem & lhs, const CursorKey & rhs)
    {
        if(lhs.created_at_ms != rhs.created_at_ms)
        {
            return lhs.created_at_ms < rhs.created_at_ms;
        }
        if(lhs.block_number != rhs.block_number)
        {
            return lhs.block_number < rhs.block_number;
        }
        if(lhs.tx_index != rhs.tx_index)
        {
            return lhs.tx_index < rhs.tx_index;
        }
        return lhs.feed_id < rhs.feed_id;
    }

    struct EventKey
    {
        std::string block_hash;
        std::int64_t log_index = 0;

        bool operator==(const EventKey & other) const
        {
            return block_hash == other.block_hash && log_index == other.log_index;
        }
    };

    struct EventKeyHash
    {
        std::size_t operator()(const EventKey & key) const
        {
            const std::size_t h1 = std::hash<std::string>{}(key.block_hash);
            const std::size_t h2 = std::hash<std::int64_t>{}(key.log_index);
            return h1 ^ (h2 + 0x9e3779b97f4a7c15ULL + (h1 << 6) + (h1 >> 2));
        }
    };
    struct NormalizedArchiveRow
    {
        int chain_id = 1;
        std::string block_hash;
        std::int64_t log_index = 0;
        std::string tx_hash;
        std::int64_t block_number = 0;
        std::int64_t tx_index = 0;
        std::optional<std::int64_t> block_time = std::nullopt;
        std::string event_type;
        std::string name;
        std::string caller;
        std::string owner;
        std::string entity_address;
        std::optional<std::int64_t> args_count = std::nullopt;
        std::optional<std::string> format_hash = std::nullopt;
        std::string state;
        std::int64_t seen_at_ms = 0;
        std::int64_t updated_at_ms = 0;
    };

    struct FeedArchiveRow
    {
        std::string feed_id;
        int chain_id = 1;
        std::string tx_hash;
        std::int64_t log_index = 0;
        std::int64_t block_number = 0;
        std::int64_t tx_index = 0;
        std::optional<std::int64_t> block_time = std::nullopt;
        std::string event_type;
        std::string status;
        bool visible = true;
        std::string history_cursor;
        std::string payload_json;
        std::int64_t created_at_ms = 0;
        std::int64_t updated_at_ms = 0;
        int projector_version = 1;
    };

    struct NormalizedHotKey
    {
        int chain_id = 1;
        std::string block_hash;
        std::int64_t log_index = 0;
        int projected_version = 0;
        std::int64_t updated_at_ms = 0;
    };

    struct FeedHotKey
    {
        int chain_id = 1;
        std::string feed_id;
        int projector_version = 0;
        std::int64_t updated_at_ms = 0;
        std::string history_cursor;
        std::string payload_json;
    };

    struct MonthBounds
    {
        int year = 0;
        int month = 0;
        std::int64_t start_block_time = 0;
        std::int64_t end_block_time = 0;
    };

    static std::optional<MonthBounds> parseMonthBounds(const std::string& month_token)
    {
        if (month_token.size() != 7 || month_token[4] != '-')
        {
            return std::nullopt;
        }

        int year = 0;
        int month = 0;
        try
        {
            year = std::stoi(month_token.substr(0, 4));
            month = std::stoi(month_token.substr(5, 2));
        }
        catch (...)
        {
            return std::nullopt;
        }

        const std::chrono::year chrono_year{year};
        const std::chrono::month chrono_month{static_cast<unsigned>(month)};
        const std::chrono::year_month_day start_ymd{chrono_year, chrono_month, std::chrono::day{1}};
        if (!start_ymd.ok())
        {
            return std::nullopt;
        }

        const std::chrono::sys_days start_day{start_ymd};
        const std::chrono::sys_days end_day{start_ymd + std::chrono::months{1}};

        MonthBounds bounds{};
        bounds.year = year;
        bounds.month = month;
        bounds.start_block_time = static_cast<std::int64_t>(
            std::chrono::duration_cast<std::chrono::seconds>(start_day.time_since_epoch()).count());
        bounds.end_block_time = static_cast<std::int64_t>(
            std::chrono::duration_cast<std::chrono::seconds>(end_day.time_since_epoch()).count());
        return bounds;
    }
    
    SQLiteHotStore::SQLiteHotStore(const std::filesystem::path& hot_db_path,
                                   const std::filesystem::path& archive_root,
                                   const std::int64_t outbox_retention_ms,
                                   const int default_chain_id,
                                   std::string default_chain_namespace)
        : _hot_db_path(hot_db_path)
        , _archive_root(archive_root)
        , _outbox_retention_ms(outbox_retention_ms)
        , _default_chain_id(default_chain_id)
        , _default_chain_namespace(default_chain_namespace.empty() ? "eth" : default_chain_namespace)
        , _shard_router(std::make_unique<MonthlyEventShardRouter>(_archive_root))
    {
        if (!_hot_db_path.parent_path().empty())
        {
            std::error_code ec;
            std::filesystem::create_directories(_hot_db_path.parent_path(), ec);
            if (ec)
            {
                throw std::runtime_error(std::format(
                    "Failed to create events DB directory '{}': {}", _hot_db_path.parent_path().string(), ec.message()));
            }
        }

        if (!_archive_root.empty())
        {
            std::error_code ec;
            std::filesystem::create_directories(_archive_root, ec);
            if (ec)
            {
                throw std::runtime_error(std::format(
                    "Failed to create events archive directory '{}': {}", _archive_root.string(), ec.message()));
            }
        }

        if (utils::likelyNetworkPath(_hot_db_path))
        {
            throw std::runtime_error(std::format("Refusing to open events hot DB on likely network filesystem path '{}'",
                                                 _hot_db_path.string()));
        }

        const int write_rc = sqlite3_open_v2(_hot_db_path.string().c_str(),
                                             &_write_db,
                                             SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_FULLMUTEX,
                                             nullptr);
        if (write_rc != SQLITE_OK)
        {
            const std::string err = (_write_db == nullptr) ? "sqlite open failed" : sqlite3_errmsg(_write_db);
            if (_write_db != nullptr)
            {
                sqlite3_close(_write_db);
                _write_db = nullptr;
            }
            throw std::runtime_error(err);
        }

        const int read_rc = sqlite3_open_v2(_hot_db_path.string().c_str(),
                                            &_read_db,
                                            SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_FULLMUTEX,
                                            nullptr);
        if (read_rc != SQLITE_OK)
        {
            const std::string err = (_read_db == nullptr) ? "sqlite open failed" : sqlite3_errmsg(_read_db);
            if (_read_db != nullptr)
            {
                sqlite3_close(_read_db);
                _read_db = nullptr;
            }
            sqlite3_close(_write_db);
            _write_db = nullptr;
            throw std::runtime_error(err);
        }

        sqlite3_busy_timeout(_write_db, 10'000);
        sqlite3_busy_timeout(_read_db, 10'000);

        if (!storage::sqlite::exec(_write_db, "PRAGMA journal_mode=WAL;") 
            || !storage::sqlite::exec(_write_db, "PRAGMA synchronous=NORMAL;") 
            || !storage::sqlite::exec(_write_db, "PRAGMA temp_store=MEMORY;") 
            || !storage::sqlite::exec(_write_db, "PRAGMA foreign_keys=OFF;") 
            || !storage::sqlite::exec(_write_db, "PRAGMA wal_autocheckpoint=0;") 
            || !storage::sqlite::exec(_write_db, "PRAGMA busy_timeout=10000;"))
        {
            throw std::runtime_error("Failed to configure events hot write DB pragmas");
        }

        if (!storage::sqlite::exec(_read_db, "PRAGMA journal_mode=WAL;") 
            || !storage::sqlite::exec(_read_db, "PRAGMA synchronous=NORMAL;") 
            || !storage::sqlite::exec(_read_db, "PRAGMA temp_store=MEMORY;") 
            || !storage::sqlite::exec(_read_db, "PRAGMA foreign_keys=OFF;") 
            || !storage::sqlite::exec(_read_db, "PRAGMA busy_timeout=10000;") 
            || !storage::sqlite::exec(_read_db, "PRAGMA query_only=ON;"))
        {
            throw std::runtime_error("Failed to configure events hot read DB pragmas");
        }

        if (!_initializeHotSchema())
        {
            throw std::runtime_error("Failed to initialize events hot DB schema");
        }
    }

    SQLiteHotStore::~SQLiteHotStore()
    {
        if (_read_db != nullptr)
        {
            sqlite3_close(_read_db);
            _read_db = nullptr;
        }
        if (_write_db != nullptr)
        {
            sqlite3_close(_write_db);
            _write_db = nullptr;
        }
    }

    storage::sqlite::WalCheckpointStats SQLiteHotStore::checkpointWal(storage::sqlite::WalCheckpointMode mode)
    {
        storage::sqlite::WalCheckpointStats stats{};

        const auto mode_str = storage::sqlite::checkpointModeToString(mode);

        try
        {
            storage::sqlite::Statement stmt(_write_db, std::format("PRAGMA wal_checkpoint({});", mode_str).c_str());

            if (stmt.step() == SQLITE_ROW)
            {
                stats.ok = true;
                stats.busy = sqlite3_column_int(stmt.get(), 0);
                stats.log_frames = sqlite3_column_int(stmt.get(), 1);
                stats.checkpointed_frames = sqlite3_column_int(stmt.get(), 2);
            }
        }
        catch (const std::exception& e)
        {
            spdlog::warn("Events WAL checkpoint mode={} failed during prepare/step: {}", mode_str, e.what());
            return stats;
        }

        std::error_code ec;
        const std::filesystem::path wal_path = std::filesystem::path(_hot_db_path.string() + "-wal");
        if (std::filesystem::exists(wal_path, ec) && !ec)
        {
            stats.wal_bytes = std::filesystem::file_size(wal_path, ec);
            if (ec)
            {
                stats.wal_bytes = 0;
            }
        }

        if (stats.ok)
        {
            spdlog::debug(
                "Events WAL checkpoint mode={} busy={} log_frames={} checkpointed_frames={} wal_bytes={}",
                mode_str,
                stats.busy,
                stats.log_frames,
                stats.checkpointed_frames,
                stats.wal_bytes);
        }
        else
        {
            spdlog::warn("Events WAL checkpoint mode={} failed", mode_str);
        }

        return stats;
    }

    std::optional<std::int64_t> SQLiteHotStore::loadNextFromBlock(const int chain_id)
    {
        storage::sqlite::Statement stmt(_write_db, "SELECT next_from_block FROM ingest_resume_state WHERE chain_id=?1;");
        sqlite3_bind_int(stmt.get(), 1, chain_id);

        if (stmt.step() != SQLITE_ROW)
        {
            return std::nullopt;
        }

        return static_cast<std::int64_t>(sqlite3_column_int64(stmt.get(), 0));
    }

    std::optional<std::uint64_t> SQLiteHotStore::loadNextLocalSeq(const int chain_id)
    {
        storage::sqlite::Statement stmt(_write_db, "SELECT next_seq FROM local_ingest_resume_state WHERE chain_id=?1;");
        sqlite3_bind_int(stmt.get(), 1, chain_id);

        if (stmt.step() != SQLITE_ROW)
        {
            return std::nullopt;
        }

        return static_cast<std::uint64_t>(sqlite3_column_int64(stmt.get(), 0));
    }

    bool SQLiteHotStore::saveNextLocalSeq(
        const int chain_id,
        const std::uint64_t next_seq,
        const std::int64_t now_ms)
    {
        storage::sqlite::Statement stmt(
            _write_db,
            "INSERT INTO local_ingest_resume_state(chain_id, next_seq, updated_at_ms) "
            "VALUES(?1, ?2, ?3) "
            "ON CONFLICT(chain_id) DO UPDATE SET "
            "next_seq=excluded.next_seq, updated_at_ms=excluded.updated_at_ms;");

        sqlite3_bind_int(stmt.get(), 1, chain_id);
        sqlite3_bind_int64(stmt.get(), 2, static_cast<sqlite3_int64>(next_seq));
        sqlite3_bind_int64(stmt.get(), 3, static_cast<sqlite3_int64>(now_ms));

        return stmt.step() == SQLITE_DONE;
    }

    std::vector<std::int64_t> SQLiteHotStore::loadReorgWindowBlocks(
        const int chain_id,
        const std::int64_t from_block,
        const std::int64_t to_block) const
    {
        std::vector<std::int64_t> block_numbers;

        storage::sqlite::Statement stmt(
            _read_db,
            "SELECT block_number "
            "FROM reorg_window "
            "WHERE chain_id=?1 AND block_number>=?2 AND block_number<=?3 "
            "ORDER BY block_number ASC;");

        sqlite3_bind_int(stmt.get(), 1, chain_id);
        sqlite3_bind_int64(stmt.get(), 2, static_cast<sqlite3_int64>(from_block));
        sqlite3_bind_int64(stmt.get(), 3, static_cast<sqlite3_int64>(to_block));

        int rc = SQLITE_OK;
        while ((rc = stmt.step()) == SQLITE_ROW)
        {
            block_numbers.push_back(static_cast<std::int64_t>(sqlite3_column_int64(stmt.get(), 0)));
        }
        if (rc != SQLITE_DONE)
        {
            throw std::runtime_error(sqlite3_errmsg(_read_db));
        }

        return block_numbers;
    }

    bool SQLiteHotStore::ingestBatch(
        const int chain_id,
        const std::vector<DecodedEvent>& events,
        const std::vector<ChainBlockInfo>& block_infos,
        const std::int64_t next_from_block,
        const std::int64_t now_ms)
    {
        std::vector<RawChainLog> raw_events;
        raw_events.reserve(events.size());

        for (const DecodedEvent& ev : events)
        {
            raw_events.push_back(ev.raw);
        }

        return ingestBatch(
            chain_id,
            raw_events,
            events,
            block_infos,
            next_from_block,
            now_ms,
            std::nullopt);
    }

    bool SQLiteHotStore::ingestBatch(
        const int chain_id,
        const std::vector<RawChainLog>& raw_events,
        const std::vector<DecodedEvent>& decoded_events,
        const std::vector<ChainBlockInfo>& block_infos,
        const std::int64_t next_from_block,
        const std::int64_t now_ms,
        const std::optional<std::uint64_t> next_local_seq)
    {
        if (!storage::sqlite::exec(_write_db, "BEGIN IMMEDIATE TRANSACTION;"))
        {
            return false;
        }

        try
        {
            std::unordered_map<EventKey, const DecodedEvent*, EventKeyHash> decoded_by_key;
            decoded_by_key.reserve(decoded_events.size());
            for (const DecodedEvent& event : decoded_events)
            {
                decoded_by_key.insert_or_assign(EventKey{event.raw.block_hash, event.raw.log_index}, &event);
            }

            storage::sqlite::Statement block_window_stmt(
                _write_db,
                "INSERT INTO reorg_window(chain_id, block_number, block_hash, parent_hash, seen_at_ms) "
                "VALUES(?1, ?2, ?3, ?4, ?5) "
                "ON CONFLICT(chain_id, block_number) DO UPDATE SET "
                "block_hash=excluded.block_hash, parent_hash=excluded.parent_hash, seen_at_ms=excluded.seen_at_ms;");

            storage::sqlite::Statement queue_reorg_removed_jobs_stmt(
                _write_db,
                "INSERT OR IGNORE INTO projection_jobs(chain_id, block_hash, log_index, created_at_ms) "
                "SELECT chain_id, block_hash, log_index, ?1 "
                "FROM normalized_events_hot "
                "WHERE chain_id=?2 AND block_number=?3 AND block_hash<>?4 AND state!='removed';");

            storage::sqlite::Statement mark_reorg_removed_norm_stmt(
                _write_db,
                "UPDATE normalized_events_hot "
                "SET state='removed', updated_at_ms=?1, exported=0, projected_version=0, projected_at_ms=NULL "
                "WHERE chain_id=?2 AND block_number=?3 AND block_hash<>?4 AND state!='removed';");

            storage::sqlite::Statement mark_reorg_removed_raw_stmt(
                _write_db,
                "UPDATE raw_events_hot "
                "SET removed=1, state='removed', removed_at_ms=COALESCE(removed_at_ms, ?1), updated_at_ms=?2 "
                "WHERE chain_id=?3 AND block_number=?4 AND block_hash<>?5 AND state!='removed';");

            for (const ChainBlockInfo& block_info : block_infos)
            {
                sqlite3_bind_int(block_window_stmt.get(), 1, block_info.chain_id);
                sqlite3_bind_int64(block_window_stmt.get(), 2, static_cast<sqlite3_int64>(block_info.block_number));
                sqlite3_bind_text(
                    block_window_stmt.get(),
                    3,
                    block_info.block_hash.c_str(),
                    static_cast<int>(block_info.block_hash.size()),
                    SQLITE_TRANSIENT);
                sqlite3_bind_text(
                    block_window_stmt.get(),
                    4,
                    block_info.parent_hash.c_str(),
                    static_cast<int>(block_info.parent_hash.size()),
                    SQLITE_TRANSIENT);
                sqlite3_bind_int64(block_window_stmt.get(), 5, static_cast<sqlite3_int64>(block_info.seen_at_ms));

                if (block_window_stmt.step() != SQLITE_DONE)
                {
                    throw std::runtime_error(sqlite3_errmsg(_write_db));
                }
                sqlite3_reset(block_window_stmt.get());
                sqlite3_clear_bindings(block_window_stmt.get());

                sqlite3_bind_int64(queue_reorg_removed_jobs_stmt.get(), 1, static_cast<sqlite3_int64>(now_ms));
                sqlite3_bind_int(queue_reorg_removed_jobs_stmt.get(), 2, block_info.chain_id);
                sqlite3_bind_int64(
                    queue_reorg_removed_jobs_stmt.get(),
                    3,
                    static_cast<sqlite3_int64>(block_info.block_number));
                sqlite3_bind_text(
                    queue_reorg_removed_jobs_stmt.get(),
                    4,
                    block_info.block_hash.c_str(),
                    static_cast<int>(block_info.block_hash.size()),
                    SQLITE_TRANSIENT);

                if (queue_reorg_removed_jobs_stmt.step() != SQLITE_DONE)
                {
                    throw std::runtime_error(sqlite3_errmsg(_write_db));
                }
                sqlite3_reset(queue_reorg_removed_jobs_stmt.get());
                sqlite3_clear_bindings(queue_reorg_removed_jobs_stmt.get());

                sqlite3_bind_int64(mark_reorg_removed_norm_stmt.get(), 1, static_cast<sqlite3_int64>(now_ms));
                sqlite3_bind_int(mark_reorg_removed_norm_stmt.get(), 2, block_info.chain_id);
                sqlite3_bind_int64(
                    mark_reorg_removed_norm_stmt.get(),
                    3,
                    static_cast<sqlite3_int64>(block_info.block_number));
                sqlite3_bind_text(
                    mark_reorg_removed_norm_stmt.get(),
                    4,
                    block_info.block_hash.c_str(),
                    static_cast<int>(block_info.block_hash.size()),
                    SQLITE_TRANSIENT);

                if (mark_reorg_removed_norm_stmt.step() != SQLITE_DONE)
                {
                    throw std::runtime_error(sqlite3_errmsg(_write_db));
                }
                sqlite3_reset(mark_reorg_removed_norm_stmt.get());
                sqlite3_clear_bindings(mark_reorg_removed_norm_stmt.get());

                sqlite3_bind_int64(mark_reorg_removed_raw_stmt.get(), 1, static_cast<sqlite3_int64>(now_ms));
                sqlite3_bind_int64(mark_reorg_removed_raw_stmt.get(), 2, static_cast<sqlite3_int64>(now_ms));
                sqlite3_bind_int(mark_reorg_removed_raw_stmt.get(), 3, block_info.chain_id);
                sqlite3_bind_int64(
                    mark_reorg_removed_raw_stmt.get(),
                    4,
                    static_cast<sqlite3_int64>(block_info.block_number));
                sqlite3_bind_text(
                    mark_reorg_removed_raw_stmt.get(),
                    5,
                    block_info.block_hash.c_str(),
                    static_cast<int>(block_info.block_hash.size()),
                    SQLITE_TRANSIENT);

                if (mark_reorg_removed_raw_stmt.step() != SQLITE_DONE)
                {
                    throw std::runtime_error(sqlite3_errmsg(_write_db));
                }
                sqlite3_reset(mark_reorg_removed_raw_stmt.get());
                sqlite3_clear_bindings(mark_reorg_removed_raw_stmt.get());
            }

            storage::sqlite::Statement raw_stmt(
                _write_db,
                "INSERT INTO raw_events_hot("
                "chain_id, block_hash, log_index, tx_hash, block_number, tx_index, block_time, "
                "address, topic0, topic1, topic2, topic3, data_hex, removed, state, seen_at_ms, "
                "updated_at_ms, removed_at_ms"
                ") VALUES("
                "?1, ?2, ?3, ?4, ?5, ?6, ?7, "
                "?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18"
                ") "
                "ON CONFLICT(chain_id, block_hash, log_index) DO UPDATE SET "
                "tx_hash=excluded.tx_hash, block_number=excluded.block_number, tx_index=excluded.tx_index, "
                "block_time=excluded.block_time, "
                "address=excluded.address, topic0=excluded.topic0, topic1=excluded.topic1, "
                "topic2=excluded.topic2, topic3=excluded.topic3, "
                "data_hex=excluded.data_hex, removed=MAX(raw_events_hot.removed, excluded.removed), "
                "state=CASE "
                "WHEN excluded.state='removed' THEN 'removed' "
                "WHEN raw_events_hot.state='removed' THEN 'removed' "
                "WHEN raw_events_hot.state='finalized' THEN 'finalized' "
                "WHEN raw_events_hot.state='safe' AND excluded.state='observed' THEN 'safe' "
                "ELSE excluded.state END, "
                "seen_at_ms=MIN(raw_events_hot.seen_at_ms, excluded.seen_at_ms), "
                "updated_at_ms=excluded.updated_at_ms, "
                "removed_at_ms=CASE "
                "WHEN excluded.state='removed' OR excluded.removed=1 THEN "
                "COALESCE(raw_events_hot.removed_at_ms, excluded.removed_at_ms) "
                "ELSE raw_events_hot.removed_at_ms END "
                "WHERE raw_events_hot.tx_hash<>excluded.tx_hash "
                "OR raw_events_hot.block_number<>excluded.block_number "
                "OR raw_events_hot.tx_index<>excluded.tx_index "
                "OR raw_events_hot.block_time IS NOT excluded.block_time "
                "OR raw_events_hot.address<>excluded.address "
                "OR raw_events_hot.topic0<>excluded.topic0 "
                "OR raw_events_hot.topic1 IS NOT excluded.topic1 "
                "OR raw_events_hot.topic2 IS NOT excluded.topic2 "
                "OR raw_events_hot.topic3 IS NOT excluded.topic3 "
                "OR raw_events_hot.data_hex<>excluded.data_hex "
                "OR raw_events_hot.removed<>MAX(raw_events_hot.removed, excluded.removed) "
                "OR raw_events_hot.state<>CASE "
                "WHEN excluded.state='removed' THEN 'removed' "
                "WHEN raw_events_hot.state='removed' THEN 'removed' "
                "WHEN raw_events_hot.state='finalized' THEN 'finalized' "
                "WHEN raw_events_hot.state='safe' AND excluded.state='observed' THEN 'safe' "
                "ELSE excluded.state END "
                "OR (excluded.state='removed' AND raw_events_hot.removed_at_ms IS NULL);");

            storage::sqlite::Statement normalized_stmt(
                _write_db,
                "INSERT INTO normalized_events_hot("
                "chain_id, block_hash, log_index, tx_hash, block_number, tx_index, block_time, "
                "event_type, name, caller, owner, entity_address, args_count, format_hash, state, "
                "seen_at_ms, updated_at_ms, exported"
                ") VALUES("
                "?1, ?2, ?3, ?4, ?5, ?6, ?7, "
                "?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, 0"
                ") "
                "ON CONFLICT(chain_id, block_hash, log_index) DO UPDATE SET "
                "tx_hash=excluded.tx_hash, block_number=excluded.block_number, tx_index=excluded.tx_index, "
                "block_time=excluded.block_time, "
                "event_type=excluded.event_type, name=excluded.name, caller=excluded.caller, owner=excluded.owner, "
                "entity_address=excluded.entity_address, "
                "args_count=excluded.args_count, format_hash=excluded.format_hash, "
                "state=CASE "
                "WHEN excluded.state='removed' THEN 'removed' "
                "WHEN normalized_events_hot.state='removed' THEN 'removed' "
                "WHEN normalized_events_hot.state='finalized' THEN 'finalized' "
                "WHEN normalized_events_hot.state='safe' AND excluded.state='observed' THEN 'safe' "
                "ELSE excluded.state END, "
                "seen_at_ms=MIN(normalized_events_hot.seen_at_ms, excluded.seen_at_ms), "
                "updated_at_ms=excluded.updated_at_ms, exported=0, projected_version=0, projected_at_ms=NULL "
                "WHERE normalized_events_hot.tx_hash<>excluded.tx_hash "
                "OR normalized_events_hot.block_number<>excluded.block_number "
                "OR normalized_events_hot.tx_index<>excluded.tx_index "
                "OR normalized_events_hot.block_time IS NOT excluded.block_time "
                "OR normalized_events_hot.event_type<>excluded.event_type "
                "OR normalized_events_hot.name<>excluded.name "
                "OR normalized_events_hot.caller<>excluded.caller "
                "OR normalized_events_hot.owner<>excluded.owner "
                "OR normalized_events_hot.entity_address<>excluded.entity_address "
                "OR normalized_events_hot.args_count IS NOT excluded.args_count "
                "OR normalized_events_hot.format_hash IS NOT excluded.format_hash "
                "OR normalized_events_hot.state<>CASE "
                "WHEN excluded.state='removed' THEN 'removed' "
                "WHEN normalized_events_hot.state='removed' THEN 'removed' "
                "WHEN normalized_events_hot.state='finalized' THEN 'finalized' "
                "WHEN normalized_events_hot.state='safe' AND excluded.state='observed' THEN 'safe' "
                "ELSE excluded.state END;");

            storage::sqlite::Statement job_stmt(
                _write_db,
                "INSERT INTO projection_jobs(chain_id, block_hash, log_index, created_at_ms) "
                "VALUES(?1, ?2, ?3, ?4) "
                "ON CONFLICT(chain_id, block_hash, log_index) DO UPDATE SET created_at_ms=excluded.created_at_ms;");

            storage::sqlite::Statement decode_failure_stmt(
                _write_db,
                "INSERT INTO decode_failures_hot("
                "chain_id, block_hash, log_index, last_error, attempts, first_seen_at_ms, last_seen_at_ms, retryable, dead_letter"
                ") VALUES("
                "?1, ?2, ?3, ?4, 1, ?5, ?6, 1, 0"
                ") ON CONFLICT(chain_id, block_hash, log_index) DO UPDATE SET "
                "last_error=excluded.last_error, "
                "attempts=decode_failures_hot.attempts + 1, "
                "last_seen_at_ms=excluded.last_seen_at_ms, "
                "retryable=CASE WHEN decode_failures_hot.attempts + 1 >= 16 THEN 0 ELSE 1 END, "
                "dead_letter=CASE WHEN decode_failures_hot.attempts + 1 >= 16 THEN 1 ELSE 0 END;");

            storage::sqlite::Statement clear_decode_failure_stmt(
                _write_db,
                "DELETE FROM decode_failures_hot WHERE chain_id=?1 AND block_hash=?2 AND log_index=?3;");

            storage::sqlite::Statement force_removed_norm_stmt(
                _write_db,
                "UPDATE normalized_events_hot "
                "SET state='removed', updated_at_ms=?1, exported=0, projected_version=0, projected_at_ms=NULL "
                "WHERE chain_id=?2 AND block_hash=?3 AND log_index=?4 AND state!='removed';");

            for (const RawChainLog& raw_event : raw_events)
            {
                const auto decoded_it = decoded_by_key.find(EventKey{raw_event.block_hash, raw_event.log_index});
                const DecodedEvent* decoded = (decoded_it == decoded_by_key.end()) ? nullptr : decoded_it->second;

                const std::string decoded_state =
                    (decoded != nullptr)
                        ? toString(decoded->state)
                        : std::string(OBSERVED_STATE);

                const bool incoming_removed = raw_event.removed || decoded_state == REMOVED_STATE;
                const std::string effective_state =
                    incoming_removed ? std::string(REMOVED_STATE) : decoded_state;
                const std::optional<std::int64_t> removed_at =
                    incoming_removed ? std::optional<std::int64_t>(now_ms) : std::nullopt;

                // topic0 is persisted in a NOT NULL column; missing topic0 is normalized to empty string.
                const std::string topic0 = raw_event.topics[0].value_or(std::string());
                const std::optional<std::string> topic1 = raw_event.topics[1];
                const std::optional<std::string> topic2 = raw_event.topics[2];
                const std::optional<std::string> topic3 = raw_event.topics[3];

                sqlite3_bind_int(raw_stmt.get(), 1, chain_id);
                sqlite3_bind_text(
                    raw_stmt.get(),
                    2,
                    raw_event.block_hash.c_str(),
                    static_cast<int>(raw_event.block_hash.size()),
                    SQLITE_TRANSIENT);
                sqlite3_bind_int64(raw_stmt.get(), 3, static_cast<sqlite3_int64>(raw_event.log_index));
                sqlite3_bind_text(
                    raw_stmt.get(),
                    4,
                    raw_event.tx_hash.c_str(),
                    static_cast<int>(raw_event.tx_hash.size()),
                    SQLITE_TRANSIENT);
                sqlite3_bind_int64(raw_stmt.get(), 5, static_cast<sqlite3_int64>(raw_event.block_number));
                sqlite3_bind_int64(raw_stmt.get(), 6, static_cast<sqlite3_int64>(raw_event.tx_index));
                _bindOptionalInt64(raw_stmt.get(), 7, raw_event.block_time);
                sqlite3_bind_text(
                    raw_stmt.get(),
                    8,
                    raw_event.address.c_str(),
                    static_cast<int>(raw_event.address.size()),
                    SQLITE_TRANSIENT);
                sqlite3_bind_text(
                    raw_stmt.get(),
                    9,
                    topic0.c_str(),
                    static_cast<int>(topic0.size()),
                    SQLITE_TRANSIENT);
                _bindOptionalText(raw_stmt.get(), 10, topic1);
                _bindOptionalText(raw_stmt.get(), 11, topic2);
                _bindOptionalText(raw_stmt.get(), 12, topic3);
                sqlite3_bind_text(
                    raw_stmt.get(),
                    13,
                    raw_event.data_hex.c_str(),
                    static_cast<int>(raw_event.data_hex.size()),
                    SQLITE_TRANSIENT);
                sqlite3_bind_int(raw_stmt.get(), 14, incoming_removed ? 1 : 0);
                sqlite3_bind_text(
                    raw_stmt.get(),
                    15,
                    effective_state.c_str(),
                    static_cast<int>(effective_state.size()),
                    SQLITE_TRANSIENT);
                sqlite3_bind_int64(raw_stmt.get(), 16, static_cast<sqlite3_int64>(raw_event.seen_at_ms));
                sqlite3_bind_int64(raw_stmt.get(), 17, static_cast<sqlite3_int64>(now_ms));
                _bindOptionalInt64(raw_stmt.get(), 18, removed_at);

                if (raw_stmt.step() != SQLITE_DONE)
                {
                    throw std::runtime_error(sqlite3_errmsg(_write_db));
                }

                sqlite3_reset(raw_stmt.get());
                sqlite3_clear_bindings(raw_stmt.get());

                if (decoded == nullptr)
                {
                    if (incoming_removed)
                    {
                        sqlite3_bind_int64(force_removed_norm_stmt.get(), 1, static_cast<sqlite3_int64>(now_ms));
                        sqlite3_bind_int(force_removed_norm_stmt.get(), 2, chain_id);
                        sqlite3_bind_text(
                            force_removed_norm_stmt.get(),
                            3,
                            raw_event.block_hash.c_str(),
                            static_cast<int>(raw_event.block_hash.size()),
                            SQLITE_TRANSIENT);
                        sqlite3_bind_int64(
                            force_removed_norm_stmt.get(),
                            4,
                            static_cast<sqlite3_int64>(raw_event.log_index));

                        if (force_removed_norm_stmt.step() != SQLITE_DONE)
                        {
                            throw std::runtime_error(sqlite3_errmsg(_write_db));
                        }
                        sqlite3_reset(force_removed_norm_stmt.get());
                        sqlite3_clear_bindings(force_removed_norm_stmt.get());

                        if (sqlite3_changes(_write_db) > 0)
                        {
                            sqlite3_bind_int(job_stmt.get(), 1, chain_id);
                            sqlite3_bind_text(
                                job_stmt.get(),
                                2,
                                raw_event.block_hash.c_str(),
                                static_cast<int>(raw_event.block_hash.size()),
                                SQLITE_TRANSIENT);
                            sqlite3_bind_int64(job_stmt.get(), 3, static_cast<sqlite3_int64>(raw_event.log_index));
                            sqlite3_bind_int64(job_stmt.get(), 4, static_cast<sqlite3_int64>(now_ms));

                            if (job_stmt.step() != SQLITE_DONE)
                            {
                                throw std::runtime_error(sqlite3_errmsg(_write_db));
                            }
                            sqlite3_reset(job_stmt.get());
                            sqlite3_clear_bindings(job_stmt.get());
                        }

                        sqlite3_bind_int(clear_decode_failure_stmt.get(), 1, chain_id);
                        sqlite3_bind_text(
                            clear_decode_failure_stmt.get(),
                            2,
                            raw_event.block_hash.c_str(),
                            static_cast<int>(raw_event.block_hash.size()),
                            SQLITE_TRANSIENT);
                        sqlite3_bind_int64(
                            clear_decode_failure_stmt.get(),
                            3,
                            static_cast<sqlite3_int64>(raw_event.log_index));

                        if (clear_decode_failure_stmt.step() != SQLITE_DONE)
                        {
                            throw std::runtime_error(sqlite3_errmsg(_write_db));
                        }
                        sqlite3_reset(clear_decode_failure_stmt.get());
                        sqlite3_clear_bindings(clear_decode_failure_stmt.get());
                        continue;
                    }

                    sqlite3_bind_int(decode_failure_stmt.get(), 1, chain_id);
                    sqlite3_bind_text(
                        decode_failure_stmt.get(),
                        2,
                        raw_event.block_hash.c_str(),
                        static_cast<int>(raw_event.block_hash.size()),
                        SQLITE_TRANSIENT);
                    sqlite3_bind_int64(
                        decode_failure_stmt.get(),
                        3,
                        static_cast<sqlite3_int64>(raw_event.log_index));
                    static constexpr std::string_view DECODE_ERROR = "decode_failed";
                    sqlite3_bind_text(
                        decode_failure_stmt.get(),
                        4,
                        DECODE_ERROR.data(),
                        static_cast<int>(DECODE_ERROR.size()),
                        SQLITE_TRANSIENT);
                    sqlite3_bind_int64(
                        decode_failure_stmt.get(),
                        5,
                        static_cast<sqlite3_int64>(raw_event.seen_at_ms));
                    sqlite3_bind_int64(
                        decode_failure_stmt.get(),
                        6,
                        static_cast<sqlite3_int64>(now_ms));

                    if (decode_failure_stmt.step() != SQLITE_DONE)
                    {
                        throw std::runtime_error(sqlite3_errmsg(_write_db));
                    }
                    sqlite3_reset(decode_failure_stmt.get());
                    sqlite3_clear_bindings(decode_failure_stmt.get());
                    continue;
                }

                sqlite3_bind_int(clear_decode_failure_stmt.get(), 1, chain_id);
                sqlite3_bind_text(
                    clear_decode_failure_stmt.get(),
                    2,
                    raw_event.block_hash.c_str(),
                    static_cast<int>(raw_event.block_hash.size()),
                    SQLITE_TRANSIENT);
                sqlite3_bind_int64(
                    clear_decode_failure_stmt.get(),
                    3,
                    static_cast<sqlite3_int64>(raw_event.log_index));

                if (clear_decode_failure_stmt.step() != SQLITE_DONE)
                {
                    throw std::runtime_error(sqlite3_errmsg(_write_db));
                }
                sqlite3_reset(clear_decode_failure_stmt.get());
                sqlite3_clear_bindings(clear_decode_failure_stmt.get());

                const std::string event_type_str = toString(decoded->event_type);
                const std::string state_str = effective_state;

                sqlite3_bind_int(normalized_stmt.get(), 1, chain_id);
                sqlite3_bind_text(
                    normalized_stmt.get(),
                    2,
                    decoded->raw.block_hash.c_str(),
                    static_cast<int>(decoded->raw.block_hash.size()),
                    SQLITE_TRANSIENT);
                sqlite3_bind_int64(normalized_stmt.get(), 3, static_cast<sqlite3_int64>(decoded->raw.log_index));
                sqlite3_bind_text(
                    normalized_stmt.get(),
                    4,
                    decoded->raw.tx_hash.c_str(),
                    static_cast<int>(decoded->raw.tx_hash.size()),
                    SQLITE_TRANSIENT);
                sqlite3_bind_int64(normalized_stmt.get(), 5, static_cast<sqlite3_int64>(decoded->raw.block_number));
                sqlite3_bind_int64(normalized_stmt.get(), 6, static_cast<sqlite3_int64>(decoded->raw.tx_index));
                _bindOptionalInt64(normalized_stmt.get(), 7, decoded->raw.block_time);
                sqlite3_bind_text(
                    normalized_stmt.get(),
                    8,
                    event_type_str.c_str(),
                    static_cast<int>(event_type_str.size()),
                    SQLITE_TRANSIENT);
                sqlite3_bind_text(
                    normalized_stmt.get(),
                    9,
                    decoded->name.c_str(),
                    static_cast<int>(decoded->name.size()),
                    SQLITE_TRANSIENT);
                sqlite3_bind_text(
                    normalized_stmt.get(),
                    10,
                    decoded->caller.c_str(),
                    static_cast<int>(decoded->caller.size()),
                    SQLITE_TRANSIENT);
                sqlite3_bind_text(
                    normalized_stmt.get(),
                    11,
                    decoded->owner.c_str(),
                    static_cast<int>(decoded->owner.size()),
                    SQLITE_TRANSIENT);
                sqlite3_bind_text(
                    normalized_stmt.get(),
                    12,
                    decoded->entity_address.c_str(),
                    static_cast<int>(decoded->entity_address.size()),
                    SQLITE_TRANSIENT);

                if (decoded->args_count.has_value())
                {
                    sqlite3_bind_int64(
                        normalized_stmt.get(),
                        13,
                        static_cast<sqlite3_int64>(*decoded->args_count));
                }
                else
                {
                    sqlite3_bind_null(normalized_stmt.get(), 13);
                }

                _bindOptionalText(normalized_stmt.get(), 14, decoded->format_hash);
                sqlite3_bind_text(
                    normalized_stmt.get(),
                    15,
                    state_str.c_str(),
                    static_cast<int>(state_str.size()),
                    SQLITE_TRANSIENT);
                sqlite3_bind_int64(
                    normalized_stmt.get(),
                    16,
                    static_cast<sqlite3_int64>(decoded->raw.seen_at_ms));
                sqlite3_bind_int64(normalized_stmt.get(), 17, static_cast<sqlite3_int64>(now_ms));

                if (normalized_stmt.step() != SQLITE_DONE)
                {
                    throw std::runtime_error(sqlite3_errmsg(_write_db));
                }

                sqlite3_reset(normalized_stmt.get());
                sqlite3_clear_bindings(normalized_stmt.get());

                const int normalized_changed = sqlite3_changes(_write_db);
                if (normalized_changed == 0)
                {
                    continue;
                }

                sqlite3_bind_int(job_stmt.get(), 1, chain_id);
                sqlite3_bind_text(
                    job_stmt.get(),
                    2,
                    decoded->raw.block_hash.c_str(),
                    static_cast<int>(decoded->raw.block_hash.size()),
                    SQLITE_TRANSIENT);
                sqlite3_bind_int64(job_stmt.get(), 3, static_cast<sqlite3_int64>(decoded->raw.log_index));
                sqlite3_bind_int64(job_stmt.get(), 4, static_cast<sqlite3_int64>(now_ms));

                if (job_stmt.step() != SQLITE_DONE)
                {
                    throw std::runtime_error(sqlite3_errmsg(_write_db));
                }

                sqlite3_reset(job_stmt.get());
                sqlite3_clear_bindings(job_stmt.get());
            }

            storage::sqlite::Statement resume_stmt(
                _write_db,
                "INSERT INTO ingest_resume_state(chain_id, next_from_block, updated_at_ms) "
                "VALUES(?1, ?2, ?3) "
                "ON CONFLICT(chain_id) DO UPDATE SET "
                "next_from_block=excluded.next_from_block, updated_at_ms=excluded.updated_at_ms;");

            sqlite3_bind_int(resume_stmt.get(), 1, chain_id);
            sqlite3_bind_int64(resume_stmt.get(), 2, static_cast<sqlite3_int64>(next_from_block));
            sqlite3_bind_int64(resume_stmt.get(), 3, static_cast<sqlite3_int64>(now_ms));

            if (resume_stmt.step() != SQLITE_DONE)
            {
                throw std::runtime_error(sqlite3_errmsg(_write_db));
            }

            if (next_local_seq.has_value())
            {
                storage::sqlite::Statement local_resume_stmt(
                    _write_db,
                    "INSERT INTO local_ingest_resume_state(chain_id, next_seq, updated_at_ms) "
                    "VALUES(?1, ?2, ?3) "
                    "ON CONFLICT(chain_id) DO UPDATE SET "
                    "next_seq=excluded.next_seq, updated_at_ms=excluded.updated_at_ms;");

                sqlite3_bind_int(local_resume_stmt.get(), 1, chain_id);
                sqlite3_bind_int64(local_resume_stmt.get(), 2, static_cast<sqlite3_int64>(*next_local_seq));
                sqlite3_bind_int64(local_resume_stmt.get(), 3, static_cast<sqlite3_int64>(now_ms));

                if (local_resume_stmt.step() != SQLITE_DONE)
                {
                    throw std::runtime_error(sqlite3_errmsg(_write_db));
                }
            }

            if (!storage::sqlite::exec(_write_db, "COMMIT;"))
            {
                throw std::runtime_error("commit failed");
            }

            return true;
        }
        catch (const std::exception& e)
        {
            spdlog::error("Events ingestBatch failed: {}", e.what());
            (void)storage::sqlite::exec(_write_db, "ROLLBACK;");
            return false;
        }
    }

    bool SQLiteHotStore::applyFinality(
        const int chain_id,
        const FinalityHeights& heights,
        const std::int64_t now_ms,
        const std::size_t reorg_window_blocks)
    {
        const std::int64_t clamped_head = std::max<std::int64_t>(0, heights.head);
        const std::int64_t clamped_safe = std::clamp<std::int64_t>(heights.safe, 0, clamped_head);
        const std::int64_t clamped_finalized = std::clamp<std::int64_t>(heights.finalized, 0, clamped_safe);

        if (!storage::sqlite::exec(_write_db, "BEGIN IMMEDIATE TRANSACTION;"))
        {
            return false;
        }

        try
        {
            std::int64_t effective_head = clamped_head;
            std::int64_t effective_safe = clamped_safe;
            std::int64_t effective_finalized = clamped_finalized;

            storage::sqlite::Statement existing_finality_stmt(
                _write_db,
                "SELECT head_block, safe_block, finalized_block "
                "FROM finality_state WHERE chain_id=?1;");

            sqlite3_bind_int(existing_finality_stmt.get(), 1, chain_id);
            if(existing_finality_stmt.step() == SQLITE_ROW)
            {
                const std::int64_t previous_head =
                    static_cast<std::int64_t>(sqlite3_column_int64(existing_finality_stmt.get(), 0));
                const std::int64_t previous_safe =
                    static_cast<std::int64_t>(sqlite3_column_int64(existing_finality_stmt.get(), 1));
                const std::int64_t previous_finalized =
                    static_cast<std::int64_t>(sqlite3_column_int64(existing_finality_stmt.get(), 2));

                effective_head = std::max(previous_head, clamped_head);
                effective_safe = std::max(previous_safe, clamped_safe);
                effective_safe = std::clamp<std::int64_t>(effective_safe, 0, effective_head);
                effective_finalized = std::max(previous_finalized, clamped_finalized);
                effective_finalized = std::clamp<std::int64_t>(effective_finalized, 0, effective_safe);
            }

            storage::sqlite::Statement finality_stmt(
                _write_db,
                "INSERT INTO finality_state(chain_id, head_block, safe_block, finalized_block, updated_at_ms) "
                "VALUES(?1, ?2, ?3, ?4, ?5) "
                "ON CONFLICT(chain_id) DO UPDATE SET "
                "head_block=excluded.head_block, safe_block=excluded.safe_block, finalized_block=excluded.finalized_block, "
                "updated_at_ms=excluded.updated_at_ms;");

            sqlite3_bind_int(finality_stmt.get(), 1, chain_id);
            sqlite3_bind_int64(finality_stmt.get(), 2, static_cast<sqlite3_int64>(effective_head));
            sqlite3_bind_int64(finality_stmt.get(), 3, static_cast<sqlite3_int64>(effective_safe));
            sqlite3_bind_int64(finality_stmt.get(), 4, static_cast<sqlite3_int64>(effective_finalized));
            sqlite3_bind_int64(finality_stmt.get(), 5, static_cast<sqlite3_int64>(now_ms));

            if (finality_stmt.step() != SQLITE_DONE)
            {
                throw std::runtime_error(sqlite3_errmsg(_write_db));
            }

            storage::sqlite::Statement queue_observed_to_safe(
                _write_db,
                "INSERT OR IGNORE INTO projection_jobs(chain_id, block_hash, log_index, created_at_ms) "
                "SELECT chain_id, block_hash, log_index, ?1 "
                "FROM normalized_events_hot "
                "WHERE chain_id=?2 AND state='observed' AND block_number <= ?3;");

            sqlite3_bind_int64(queue_observed_to_safe.get(), 1, static_cast<sqlite3_int64>(now_ms));
            sqlite3_bind_int(queue_observed_to_safe.get(), 2, chain_id);
            sqlite3_bind_int64(queue_observed_to_safe.get(), 3, static_cast<sqlite3_int64>(effective_safe));

            if (queue_observed_to_safe.step() != SQLITE_DONE)
            {
                throw std::runtime_error(sqlite3_errmsg(_write_db));
            }

            storage::sqlite::Statement update_observed_to_safe_norm(
                _write_db,
                "UPDATE normalized_events_hot "
                "SET state='safe', updated_at_ms=?1, exported=0, projected_version=0, projected_at_ms=NULL "
                "WHERE chain_id=?2 AND state='observed' AND block_number <= ?3;");

            sqlite3_bind_int64(update_observed_to_safe_norm.get(), 1, static_cast<sqlite3_int64>(now_ms));
            sqlite3_bind_int(update_observed_to_safe_norm.get(), 2, chain_id);
            sqlite3_bind_int64(update_observed_to_safe_norm.get(), 3, static_cast<sqlite3_int64>(effective_safe));

            if (update_observed_to_safe_norm.step() != SQLITE_DONE)
            {
                throw std::runtime_error(sqlite3_errmsg(_write_db));
            }

            storage::sqlite::Statement update_observed_to_safe_raw(
                _write_db,
                "UPDATE raw_events_hot SET state='safe', updated_at_ms=?1 "
                "WHERE chain_id=?2 AND state='observed' AND block_number <= ?3;");

            sqlite3_bind_int64(update_observed_to_safe_raw.get(), 1, static_cast<sqlite3_int64>(now_ms));
            sqlite3_bind_int(update_observed_to_safe_raw.get(), 2, chain_id);
            sqlite3_bind_int64(update_observed_to_safe_raw.get(), 3, static_cast<sqlite3_int64>(effective_safe));

            if (update_observed_to_safe_raw.step() != SQLITE_DONE)
            {
                throw std::runtime_error(sqlite3_errmsg(_write_db));
            }

            storage::sqlite::Statement queue_safe_to_finalized(
                _write_db,
                "INSERT OR IGNORE INTO projection_jobs(chain_id, block_hash, log_index, created_at_ms) "
                "SELECT chain_id, block_hash, log_index, ?1 "
                "FROM normalized_events_hot "
                "WHERE chain_id=?2 AND state='safe' AND block_number <= ?3;");

            sqlite3_bind_int64(queue_safe_to_finalized.get(), 1, static_cast<sqlite3_int64>(now_ms));
            sqlite3_bind_int(queue_safe_to_finalized.get(), 2, chain_id);
            sqlite3_bind_int64(queue_safe_to_finalized.get(), 3, static_cast<sqlite3_int64>(effective_finalized));

            if (queue_safe_to_finalized.step() != SQLITE_DONE)
            {
                throw std::runtime_error(sqlite3_errmsg(_write_db));
            }

            storage::sqlite::Statement update_safe_to_finalized_norm(
                _write_db,
                "UPDATE normalized_events_hot "
                "SET state='finalized', updated_at_ms=?1, exported=0, projected_version=0, projected_at_ms=NULL "
                "WHERE chain_id=?2 AND state='safe' AND block_number <= ?3;");

            sqlite3_bind_int64(update_safe_to_finalized_norm.get(), 1, static_cast<sqlite3_int64>(now_ms));
            sqlite3_bind_int(update_safe_to_finalized_norm.get(), 2, chain_id);
            sqlite3_bind_int64(update_safe_to_finalized_norm.get(), 3, static_cast<sqlite3_int64>(effective_finalized));

            if (update_safe_to_finalized_norm.step() != SQLITE_DONE)
            {
                throw std::runtime_error(sqlite3_errmsg(_write_db));
            }

            storage::sqlite::Statement update_safe_to_finalized_raw(
                _write_db,
                "UPDATE raw_events_hot SET state='finalized', updated_at_ms=?1 "
                "WHERE chain_id=?2 AND state='safe' AND block_number <= ?3;");

            sqlite3_bind_int64(update_safe_to_finalized_raw.get(), 1, static_cast<sqlite3_int64>(now_ms));
            sqlite3_bind_int(update_safe_to_finalized_raw.get(), 2, chain_id);
            sqlite3_bind_int64(update_safe_to_finalized_raw.get(), 3, static_cast<sqlite3_int64>(effective_finalized));

            if (update_safe_to_finalized_raw.step() != SQLITE_DONE)
            {
                throw std::runtime_error(sqlite3_errmsg(_write_db));
            }

            const std::int64_t floor =
                (effective_head > static_cast<std::int64_t>(reorg_window_blocks))
                    ? (effective_head - static_cast<std::int64_t>(reorg_window_blocks))
                    : 0;

            storage::sqlite::Statement prune_reorg_stmt(
                _write_db,
                "DELETE FROM reorg_window WHERE chain_id=?1 AND block_number < ?2;");

            sqlite3_bind_int(prune_reorg_stmt.get(), 1, chain_id);
            sqlite3_bind_int64(prune_reorg_stmt.get(), 2, static_cast<sqlite3_int64>(floor));

            if (prune_reorg_stmt.step() != SQLITE_DONE)
            {
                throw std::runtime_error(sqlite3_errmsg(_write_db));
            }

            if (!storage::sqlite::exec(_write_db, "COMMIT;"))
            {
                throw std::runtime_error("commit failed");
            }

            return true;
        }
        catch (const std::exception& e)
        {
            spdlog::error("Events applyFinality failed: {}", e.what());
            (void)storage::sqlite::exec(_write_db, "ROLLBACK;");
            return false;
        }
    }

    std::size_t SQLiteHotStore::projectBatch(const std::size_t limit, const std::int64_t now_ms)
    {
        if (!storage::sqlite::exec(_write_db, "BEGIN IMMEDIATE TRANSACTION;"))
        {
            return 0;
        }

        std::size_t projected_count = 0;

        try
        {
            storage::sqlite::Statement cleanup_orphan_jobs(
                _write_db,
                "DELETE FROM projection_jobs "
                "WHERE NOT EXISTS ("
                "SELECT 1 FROM normalized_events_hot n "
                "WHERE n.chain_id=projection_jobs.chain_id "
                "AND n.block_hash=projection_jobs.block_hash "
                "AND n.log_index=projection_jobs.log_index"
                ");");

            if (cleanup_orphan_jobs.step() != SQLITE_DONE)
            {
                throw std::runtime_error(sqlite3_errmsg(_write_db));
            }

            storage::sqlite::Statement select_jobs(
                _write_db,
                "SELECT "
                "n.chain_id, n.block_hash, n.log_index, n.tx_hash, n.block_number, n.tx_index, n.block_time, "
                "n.event_type, n.name, n.owner, n.state, n.seen_at_ms "
                "FROM projection_jobs j "
                "JOIN normalized_events_hot n "
                "ON n.chain_id=j.chain_id AND n.block_hash=j.block_hash AND n.log_index=j.log_index "
                "ORDER BY n.block_number ASC, n.tx_index ASC, n.log_index ASC "
                "LIMIT ?1;");

            sqlite3_bind_int64(select_jobs.get(), 1, static_cast<sqlite3_int64>(limit));

            struct ProjectionJobRow
            {
                int chain_id = 0;
                std::string block_hash;
                std::int64_t log_index = 0;
                std::string tx_hash;
                std::int64_t block_number = 0;
                std::int64_t tx_index = 0;
                std::optional<std::int64_t> block_time = std::nullopt;
                std::string event_type;
                std::string name;
                std::string owner;
                std::string state;
                std::int64_t seen_at_ms = 0;
            };

            std::vector<ProjectionJobRow> jobs;
            jobs.reserve(limit);

            int select_jobs_rc = SQLITE_OK;
            while ((select_jobs_rc = select_jobs.step()) == SQLITE_ROW)
            {
                ProjectionJobRow row{};
                row.chain_id = sqlite3_column_int(select_jobs.get(), 0);
                row.block_hash = reinterpret_cast<const char*>(sqlite3_column_text(select_jobs.get(), 1));
                row.log_index = static_cast<std::int64_t>(sqlite3_column_int64(select_jobs.get(), 2));
                row.tx_hash = reinterpret_cast<const char*>(sqlite3_column_text(select_jobs.get(), 3));
                row.block_number = static_cast<std::int64_t>(sqlite3_column_int64(select_jobs.get(), 4));
                row.tx_index = static_cast<std::int64_t>(sqlite3_column_int64(select_jobs.get(), 5));
                row.block_time = _columnInt64Optional(select_jobs.get(), 6);
                row.event_type = reinterpret_cast<const char*>(sqlite3_column_text(select_jobs.get(), 7));
                row.name = reinterpret_cast<const char*>(sqlite3_column_text(select_jobs.get(), 8));
                row.owner = reinterpret_cast<const char*>(sqlite3_column_text(select_jobs.get(), 9));
                row.state = reinterpret_cast<const char*>(sqlite3_column_text(select_jobs.get(), 10));
                row.seen_at_ms = static_cast<std::int64_t>(sqlite3_column_int64(select_jobs.get(), 11));
                jobs.push_back(std::move(row));
            }
            if (select_jobs_rc != SQLITE_DONE)
            {
                throw std::runtime_error(sqlite3_errmsg(_write_db));
            }

            storage::sqlite::Statement existing_feed_stmt(
                _write_db,
                "SELECT created_at_ms, status, visible, history_cursor, payload_json, stream_emitted "
                "FROM feed_items_hot WHERE feed_id=?1;");

            storage::sqlite::Statement upsert_feed_stmt(
                _write_db,
                "INSERT INTO feed_items_hot("
                "feed_id, chain_id, tx_hash, log_index, block_number, tx_index, block_time, event_type, status, visible, "
                "history_cursor, payload_json, created_at_ms, updated_at_ms, projector_version, exported"
                ") VALUES("
                "?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, "
                "?11, ?12, ?13, ?14, ?15, 0"
                ") ON CONFLICT(feed_id) DO UPDATE SET "
                "chain_id=excluded.chain_id, tx_hash=excluded.tx_hash, log_index=excluded.log_index, "
                "block_number=excluded.block_number, tx_index=excluded.tx_index, "
                "block_time=excluded.block_time, event_type=excluded.event_type, status=excluded.status, "
                "visible=excluded.visible, history_cursor=excluded.history_cursor, "
                "payload_json=excluded.payload_json, updated_at_ms=excluded.updated_at_ms, "
                "projector_version=excluded.projector_version, "
                "stream_emitted=feed_items_hot.stream_emitted, "
                "exported=CASE "
                "WHEN feed_items_hot.status IS NOT excluded.status "
                "OR feed_items_hot.visible IS NOT excluded.visible "
                "OR feed_items_hot.history_cursor IS NOT excluded.history_cursor "
                "OR feed_items_hot.payload_json IS NOT excluded.payload_json "
                "THEN 0 ELSE feed_items_hot.exported END;");

            storage::sqlite::Statement insert_outbox_stmt(
                _write_db,
                "INSERT INTO global_outbox(stream_seq, feed_id, op, status, event_type, history_cursor, payload_json, "
                "created_at_ms) "
                "VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8);");

            storage::sqlite::Statement mark_stream_emitted_stmt(
                _write_db,
                "UPDATE feed_items_hot "
                "SET stream_emitted=1 "
                "WHERE feed_id=?1;");

            storage::sqlite::Statement outbox_seq_select_stmt(
                _write_db,
                "SELECT next_stream_seq FROM outbox_stream_state WHERE singleton=1;");

            storage::sqlite::Statement outbox_seq_advance_stmt(
                _write_db,
                "UPDATE outbox_stream_state SET next_stream_seq=?1 WHERE singleton=1;");

            storage::sqlite::Statement delete_job_stmt(
                _write_db,
                "DELETE FROM projection_jobs WHERE chain_id=?1 AND block_hash=?2 AND log_index=?3;");

            storage::sqlite::Statement mark_projected_stmt(
                _write_db,
                "UPDATE normalized_events_hot "
                "SET projected_version=MAX(projected_version, ?1), projected_at_ms=?2 "
                "WHERE chain_id=?3 AND block_hash=?4 AND log_index=?5;");

            for (const ProjectionJobRow& job : jobs)
            {
                const int chain_id = job.chain_id;
                const std::string& block_hash = job.block_hash;
                const std::int64_t log_index = job.log_index;
                const std::string& tx_hash = job.tx_hash;
                const std::int64_t block_number = job.block_number;
                const std::int64_t tx_index = job.tx_index;
                const std::optional<std::int64_t>& block_time = job.block_time;
                const std::string& event_type = job.event_type;
                const std::string& name = job.name;
                const std::string& owner = job.owner;
                const std::string& state = job.state;
                const std::int64_t seen_at_ms = job.seen_at_ms;

                const bool logical_feed_identity = _usesLogicalFeedIdentity(event_type);
                const std::string feed_id = _projectedFeedId(
                    _default_chain_namespace,
                    chain_id,
                    tx_hash,
                    log_index,
                    event_type,
                    name);
                const bool visible = (state != REMOVED_STATE);

                bool existed = false;
                bool stream_emitted = false;

                std::int64_t created_at_ms = seen_at_ms;
                std::string existing_status;
                int existing_visible = visible ? 1 : 0;
                std::string existing_history_cursor;
                std::string existing_payload_json;

                sqlite3_bind_text(
                    existing_feed_stmt.get(),
                    1,
                    feed_id.c_str(),
                    static_cast<int>(feed_id.size()),
                    SQLITE_TRANSIENT);

                const int existing_feed_rc = existing_feed_stmt.step();
                if (existing_feed_rc == SQLITE_ROW)
                {
                    existed = true;
                    created_at_ms = static_cast<std::int64_t>(sqlite3_column_int64(existing_feed_stmt.get(), 0));
                    existing_status = reinterpret_cast<const char*>(sqlite3_column_text(existing_feed_stmt.get(), 1));
                    existing_visible = sqlite3_column_int(existing_feed_stmt.get(), 2);
                    existing_history_cursor = reinterpret_cast<const char*>(sqlite3_column_text(existing_feed_stmt.get(), 3));
                    existing_payload_json = reinterpret_cast<const char*>(sqlite3_column_text(existing_feed_stmt.get(), 4));
                    stream_emitted = sqlite3_column_int(existing_feed_stmt.get(), 5) != 0;
                }
                else if (existing_feed_rc != SQLITE_DONE)
                {
                    throw std::runtime_error(sqlite3_errmsg(_write_db));
                }

                sqlite3_reset(existing_feed_stmt.get());
                sqlite3_clear_bindings(existing_feed_stmt.get());

                const std::string history_cursor = existed
                    ? existing_history_cursor
                    : std::format("{}", CursorKey{
                          .chain_id = chain_id,
                          .chain_namespace = _default_chain_namespace,
                          .created_at_ms = created_at_ms,
                          .block_number = block_number,
                          .tx_index = tx_index,
                          .feed_id = feed_id
                      });

                const std::string payload_json = _compactFeedPayloadJson(event_type, name, owner);

                const bool materially_changed =
                    !existed ||
                    existing_status != state ||
                    existing_visible != (visible ? 1 : 0) ||
                    (!logical_feed_identity && existing_history_cursor != history_cursor) ||
                    existing_payload_json != payload_json;

                sqlite3_bind_text(
                    upsert_feed_stmt.get(), 1, feed_id.c_str(), static_cast<int>(feed_id.size()), SQLITE_TRANSIENT);
                sqlite3_bind_int(upsert_feed_stmt.get(), 2, chain_id);
                sqlite3_bind_text(
                    upsert_feed_stmt.get(), 3, tx_hash.c_str(), static_cast<int>(tx_hash.size()), SQLITE_TRANSIENT);
                sqlite3_bind_int64(upsert_feed_stmt.get(), 4, static_cast<sqlite3_int64>(log_index));
                sqlite3_bind_int64(upsert_feed_stmt.get(), 5, static_cast<sqlite3_int64>(block_number));
                sqlite3_bind_int64(upsert_feed_stmt.get(), 6, static_cast<sqlite3_int64>(tx_index));
                _bindOptionalInt64(upsert_feed_stmt.get(), 7, block_time);
                sqlite3_bind_text(
                    upsert_feed_stmt.get(), 8, event_type.c_str(), static_cast<int>(event_type.size()), SQLITE_TRANSIENT);
                sqlite3_bind_text(
                    upsert_feed_stmt.get(), 9, state.c_str(), static_cast<int>(state.size()), SQLITE_TRANSIENT);
                sqlite3_bind_int(upsert_feed_stmt.get(), 10, visible ? 1 : 0);
                sqlite3_bind_text(
                    upsert_feed_stmt.get(), 11, history_cursor.c_str(), static_cast<int>(history_cursor.size()), SQLITE_TRANSIENT);
                sqlite3_bind_text(
                    upsert_feed_stmt.get(), 12, payload_json.c_str(), static_cast<int>(payload_json.size()), SQLITE_TRANSIENT);
                sqlite3_bind_int64(upsert_feed_stmt.get(), 13, static_cast<sqlite3_int64>(created_at_ms));
                sqlite3_bind_int64(upsert_feed_stmt.get(), 14, static_cast<sqlite3_int64>(now_ms));
                sqlite3_bind_int(upsert_feed_stmt.get(), 15, CURRENT_PROJECTOR_VERSION);

                if (upsert_feed_stmt.step() != SQLITE_DONE)
                {
                    throw std::runtime_error(sqlite3_errmsg(_write_db));
                }

                sqlite3_reset(upsert_feed_stmt.get());
                sqlite3_clear_bindings(upsert_feed_stmt.get());

                const bool should_emit_outbox =
                    (state == FINALIZED_STATE && (!stream_emitted || materially_changed)) ||
                    (state == REMOVED_STATE && stream_emitted && materially_changed);

                if (should_emit_outbox)
                {
                    std::int64_t stream_seq = 0;

                    const int outbox_seq_rc = outbox_seq_select_stmt.step();
                    if (outbox_seq_rc == SQLITE_ROW)
                    {
                        stream_seq = static_cast<std::int64_t>(sqlite3_column_int64(outbox_seq_select_stmt.get(), 0));
                    }
                    else if (outbox_seq_rc != SQLITE_DONE)
                    {
                        throw std::runtime_error(sqlite3_errmsg(_write_db));
                    }
                    sqlite3_reset(outbox_seq_select_stmt.get());

                    if (stream_seq <= 0)
                    {
                        throw std::runtime_error("invalid outbox stream sequencer state");
                    }

                    sqlite3_bind_int64(
                        outbox_seq_advance_stmt.get(),
                        1,
                        static_cast<sqlite3_int64>(stream_seq + 1));

                    if (outbox_seq_advance_stmt.step() != SQLITE_DONE)
                    {
                        throw std::runtime_error(sqlite3_errmsg(_write_db));
                    }

                    sqlite3_reset(outbox_seq_advance_stmt.get());
                    sqlite3_clear_bindings(outbox_seq_advance_stmt.get());

                    const std::string op =
                        (!visible) ? "remove" : (stream_emitted ? "update" : "insert");

                    sqlite3_bind_int64(insert_outbox_stmt.get(), 1, static_cast<sqlite3_int64>(stream_seq));
                    sqlite3_bind_text(
                        insert_outbox_stmt.get(), 2, feed_id.c_str(), static_cast<int>(feed_id.size()), SQLITE_TRANSIENT);
                    sqlite3_bind_text(
                        insert_outbox_stmt.get(), 3, op.c_str(), static_cast<int>(op.size()), SQLITE_TRANSIENT);
                    sqlite3_bind_text(
                        insert_outbox_stmt.get(), 4, state.c_str(), static_cast<int>(state.size()), SQLITE_TRANSIENT);
                    sqlite3_bind_text(
                        insert_outbox_stmt.get(), 5, event_type.c_str(), static_cast<int>(event_type.size()), SQLITE_TRANSIENT);
                    sqlite3_bind_text(
                        insert_outbox_stmt.get(), 6, history_cursor.c_str(), static_cast<int>(history_cursor.size()), SQLITE_TRANSIENT);
                    sqlite3_bind_text(
                        insert_outbox_stmt.get(), 7, payload_json.c_str(), static_cast<int>(payload_json.size()), SQLITE_TRANSIENT);
                    sqlite3_bind_int64(insert_outbox_stmt.get(), 8, static_cast<sqlite3_int64>(now_ms));

                    if (insert_outbox_stmt.step() != SQLITE_DONE)
                    {
                        throw std::runtime_error(sqlite3_errmsg(_write_db));
                    }

                    sqlite3_reset(insert_outbox_stmt.get());
                    sqlite3_clear_bindings(insert_outbox_stmt.get());

                    sqlite3_bind_text(
                        mark_stream_emitted_stmt.get(),
                        1,
                        feed_id.c_str(),
                        static_cast<int>(feed_id.size()),
                        SQLITE_TRANSIENT);
                    
                    if (mark_stream_emitted_stmt.step() != SQLITE_DONE)
                    {
                        throw std::runtime_error(sqlite3_errmsg(_write_db));
                    }
                
                    sqlite3_reset(mark_stream_emitted_stmt.get());
                    sqlite3_clear_bindings(mark_stream_emitted_stmt.get());
                
                    stream_emitted = true;
                }

                sqlite3_bind_int(mark_projected_stmt.get(), 1, CURRENT_PROJECTOR_VERSION);
                sqlite3_bind_int64(mark_projected_stmt.get(), 2, static_cast<sqlite3_int64>(now_ms));
                sqlite3_bind_int(mark_projected_stmt.get(), 3, chain_id);
                sqlite3_bind_text(
                    mark_projected_stmt.get(),
                    4,
                    block_hash.c_str(),
                    static_cast<int>(block_hash.size()),
                    SQLITE_TRANSIENT);
                sqlite3_bind_int64(mark_projected_stmt.get(), 5, static_cast<sqlite3_int64>(log_index));
                if (mark_projected_stmt.step() != SQLITE_DONE)
                {
                    throw std::runtime_error(sqlite3_errmsg(_write_db));
                }
                sqlite3_reset(mark_projected_stmt.get());
                sqlite3_clear_bindings(mark_projected_stmt.get());

                sqlite3_bind_int(delete_job_stmt.get(), 1, chain_id);
                sqlite3_bind_text(
                    delete_job_stmt.get(), 2, block_hash.c_str(), static_cast<int>(block_hash.size()), SQLITE_TRANSIENT);
                sqlite3_bind_int64(delete_job_stmt.get(), 3, static_cast<sqlite3_int64>(log_index));

                if (delete_job_stmt.step() != SQLITE_DONE)
                {
                    throw std::runtime_error(sqlite3_errmsg(_write_db));
                }

                sqlite3_reset(delete_job_stmt.get());
                sqlite3_clear_bindings(delete_job_stmt.get());

                ++projected_count;
            }

            if (_outbox_retention_ms > 0)
            {
                const std::int64_t cutoff_ms = now_ms - _outbox_retention_ms;
                std::int64_t max_pruned_seq = 0;

                storage::sqlite::Statement max_pruned_stmt(
                    _write_db,
                    "SELECT COALESCE(MAX(stream_seq), 0) FROM global_outbox WHERE created_at_ms < ?1;");

                sqlite3_bind_int64(max_pruned_stmt.get(), 1, static_cast<sqlite3_int64>(cutoff_ms));
                if (max_pruned_stmt.step() == SQLITE_ROW)
                {
                    max_pruned_seq = static_cast<std::int64_t>(sqlite3_column_int64(max_pruned_stmt.get(), 0));
                }

                storage::sqlite::Statement prune_outbox_stmt(
                    _write_db,
                    "DELETE FROM global_outbox WHERE created_at_ms < ?1;");

                sqlite3_bind_int64(prune_outbox_stmt.get(), 1, static_cast<sqlite3_int64>(cutoff_ms));
                if (prune_outbox_stmt.step() != SQLITE_DONE)
                {
                    throw std::runtime_error(sqlite3_errmsg(_write_db));
                }

                if (max_pruned_seq > 0)
                {
                    storage::sqlite::Statement update_floor_stmt(
                        _write_db,
                        "UPDATE outbox_stream_state "
                        "SET replay_floor_seq=MAX(replay_floor_seq, ?1) "
                        "WHERE singleton=1;");

                    sqlite3_bind_int64(
                        update_floor_stmt.get(),
                        1,
                        static_cast<sqlite3_int64>(max_pruned_seq + 1));

                    if (update_floor_stmt.step() != SQLITE_DONE)
                    {
                        throw std::runtime_error(sqlite3_errmsg(_write_db));
                    }
                }
            }

            storage::sqlite::Statement align_floor_stmt(
                _write_db,
                "UPDATE outbox_stream_state "
                "SET replay_floor_seq=CASE "
                "WHEN EXISTS(SELECT 1 FROM global_outbox) THEN "
                "MAX(replay_floor_seq, (SELECT MIN(stream_seq) FROM global_outbox)) "
                "ELSE replay_floor_seq "
                "END "
                "WHERE singleton=1;");

            if (align_floor_stmt.step() != SQLITE_DONE)
            {
                throw std::runtime_error(sqlite3_errmsg(_write_db));
            }

            if (!storage::sqlite::exec(_write_db, "COMMIT;"))
            {
                throw std::runtime_error("commit failed");
            }

            return projected_count;
        }
        catch (const std::exception& e)
        {
            spdlog::error("Events projectBatch failed: {}", e.what());
            (void)storage::sqlite::exec(_write_db, "ROLLBACK;");
            return 0;
        }
    }

    bool SQLiteHotStore::runArchiveCycle(const int chain_id, const std::size_t hot_window_days, const std::int64_t now_ms)
    {
        try
        {
            std::vector<std::string> months;
            {
                storage::sqlite::Statement months_stmt(
                    _write_db,
                    "SELECT month_token FROM ("
                    "SELECT DISTINCT strftime('%Y-%m', block_time, 'unixepoch') AS month_token "
                    "FROM normalized_events_hot n "
                    "WHERE n.chain_id=?1 AND n.state='finalized' AND n.projected_version>=?2 "
                    "AND n.exported=0 AND n.block_time IS NOT NULL "
                    "AND EXISTS ("
                    "SELECT 1 FROM feed_items_hot f "
                    "WHERE f.chain_id=n.chain_id AND f.tx_hash=n.tx_hash AND f.log_index=n.log_index"
                    ") "
                    "AND NOT EXISTS ("
                    "SELECT 1 FROM projection_jobs j "
                    "WHERE j.chain_id=n.chain_id AND j.block_hash=n.block_hash AND j.log_index=n.log_index"
                    ") "
                    "UNION "
                    "SELECT DISTINCT strftime('%Y-%m', block_time, 'unixepoch') AS month_token "
                    "FROM feed_items_hot f "
                    "WHERE f.chain_id=?1 AND f.status='finalized' AND f.exported=0 AND f.block_time IS NOT NULL "
                    "AND EXISTS ("
                    "SELECT 1 FROM normalized_events_hot n "
                    "WHERE n.chain_id=f.chain_id AND n.tx_hash=f.tx_hash AND n.log_index=f.log_index "
                    "AND n.projected_version>=?2"
                    ") "
                    "AND NOT EXISTS ("
                    "SELECT 1 "
                    "FROM normalized_events_hot n "
                    "JOIN projection_jobs j "
                    "ON j.chain_id=n.chain_id AND j.block_hash=n.block_hash AND j.log_index=n.log_index "
                    "WHERE n.chain_id=f.chain_id AND n.tx_hash=f.tx_hash AND n.log_index=f.log_index"
                    ")"
                    ") "
                    "WHERE month_token IS NOT NULL "
                    "ORDER BY month_token ASC "
                    "LIMIT 8;");

                sqlite3_bind_int(months_stmt.get(), 1, chain_id);
                sqlite3_bind_int(months_stmt.get(), 2, CURRENT_PROJECTOR_VERSION);

                int months_rc = SQLITE_OK;
                while ((months_rc = months_stmt.step()) == SQLITE_ROW)
                {
                    const unsigned char* month = sqlite3_column_text(months_stmt.get(), 0);
                    if (month != nullptr)
                    {
                        months.emplace_back(reinterpret_cast<const char*>(month));
                    }
                }
                if (months_rc != SQLITE_DONE)
                {
                    throw std::runtime_error(sqlite3_errmsg(_write_db));
                }
            }

            for (const std::string& month : months)
            {
                if (!_exportMonth(chain_id, month, now_ms))
                {
                    return false;
                }
            }

            const std::int64_t cutoff_seconds =
                static_cast<std::int64_t>(now_ms / 1000) -
                static_cast<std::int64_t>(hot_window_days) * 24 * 60 * 60;

            {
                if (!storage::sqlite::exec(_write_db, "BEGIN IMMEDIATE TRANSACTION;"))
                {
                    return false;
                }

                try
                {
                    if (!storage::sqlite::exec(_write_db, "DROP TABLE IF EXISTS temp.prune_keys;"))
                    {
                        throw std::runtime_error(sqlite3_errmsg(_write_db));
                    }

                    storage::sqlite::Statement create_prune_keys(
                        _write_db,
                        "CREATE TEMP TABLE prune_keys AS "
                        "SELECT chain_id, tx_hash, log_index, block_hash "
                        "FROM normalized_events_hot "
                        "WHERE chain_id=?1 AND state='finalized' AND projected_version>=?2 AND exported=1 "
                        "AND block_time IS NOT NULL AND block_time < ?3 "
                        "AND NOT EXISTS ("
                        "SELECT 1 FROM projection_jobs j "
                        "WHERE j.chain_id=normalized_events_hot.chain_id "
                        "AND j.block_hash=normalized_events_hot.block_hash "
                        "AND j.log_index=normalized_events_hot.log_index"
                        ") "
                        "AND EXISTS ("
                        "SELECT 1 FROM feed_items_hot f "
                        "WHERE f.chain_id=normalized_events_hot.chain_id "
                        "AND f.tx_hash=normalized_events_hot.tx_hash "
                        "AND f.log_index=normalized_events_hot.log_index "
                        "AND f.status='finalized' AND f.exported=1"
                        ");");

                    sqlite3_bind_int(create_prune_keys.get(), 1, chain_id);
                    sqlite3_bind_int(create_prune_keys.get(), 2, CURRENT_PROJECTOR_VERSION);
                    sqlite3_bind_int64(create_prune_keys.get(), 3, static_cast<sqlite3_int64>(cutoff_seconds));

                    if (create_prune_keys.step() != SQLITE_DONE)
                    {
                        throw std::runtime_error(sqlite3_errmsg(_write_db));
                    }

                    if (!storage::sqlite::exec(
                            _write_db,
                            "CREATE INDEX IF NOT EXISTS temp.idx_prune_keys_block_log "
                            "ON prune_keys(chain_id, block_hash, log_index);"))
                    {
                        throw std::runtime_error(sqlite3_errmsg(_write_db));
                    }

                    if (!storage::sqlite::exec(
                            _write_db,
                            "CREATE INDEX IF NOT EXISTS temp.idx_prune_keys_tx_log "
                            "ON prune_keys(chain_id, tx_hash, log_index);"))
                    {
                        throw std::runtime_error(sqlite3_errmsg(_write_db));
                    }

                    storage::sqlite::Statement prune_feed(
                        _write_db,
                        "DELETE FROM feed_items_hot "
                        "WHERE feed_items_hot.exported=1 "
                        "AND EXISTS ("
                        "SELECT 1 FROM temp.prune_keys k "
                        "WHERE k.chain_id=feed_items_hot.chain_id "
                        "AND k.tx_hash=feed_items_hot.tx_hash "
                        "AND k.log_index=feed_items_hot.log_index"
                        ");");

                    if (prune_feed.step() != SQLITE_DONE)
                    {
                        throw std::runtime_error(sqlite3_errmsg(_write_db));
                    }

                    storage::sqlite::Statement prune_raw(
                        _write_db,
                        "DELETE FROM raw_events_hot "
                        "WHERE EXISTS ("
                        "SELECT 1 FROM temp.prune_keys k "
                        "WHERE k.chain_id=raw_events_hot.chain_id "
                        "AND k.block_hash=raw_events_hot.block_hash "
                        "AND k.log_index=raw_events_hot.log_index"
                        ");");

                    if (prune_raw.step() != SQLITE_DONE)
                    {
                        throw std::runtime_error(sqlite3_errmsg(_write_db));
                    }

                    storage::sqlite::Statement prune_norm(
                        _write_db,
                        "DELETE FROM normalized_events_hot "
                        "WHERE EXISTS ("
                        "SELECT 1 "
                        "FROM temp.prune_keys k "
                        "WHERE k.chain_id=normalized_events_hot.chain_id "
                        "AND k.block_hash=normalized_events_hot.block_hash "
                        "AND k.log_index=normalized_events_hot.log_index"
                        ");");

                    if (prune_norm.step() != SQLITE_DONE)
                    {
                        throw std::runtime_error(sqlite3_errmsg(_write_db));
                    }

                    if (!storage::sqlite::exec(_write_db, "DROP TABLE IF EXISTS temp.prune_keys;"))
                    {
                        throw std::runtime_error(sqlite3_errmsg(_write_db));
                    }

                    if (!storage::sqlite::exec(_write_db, "COMMIT;"))
                    {
                        throw std::runtime_error("commit failed");
                    }

                    return true;
                }
                catch (const std::exception& e)
                {
                    spdlog::error("Events runArchiveCycle prune phase failed: {}", e.what());
                    (void)storage::sqlite::exec(_write_db, "ROLLBACK;");
                    return false;
                }
            }
        }
        catch (const std::exception& e)
        {
            spdlog::error("Events runArchiveCycle failed: {}", e.what());
            return false;
        }
    } 

    bool SQLiteHotStore::runCycle(const int chain_id, const std::size_t hot_window_days, const std::int64_t now_ms)
    {
        return runArchiveCycle(chain_id, hot_window_days, now_ms);
    }

    FeedPage SQLiteHotStore::getFeedPage(const FeedQuery& query) const
    {
        const std::size_t limit =
            std::clamp<std::size_t>((query.limit == 0) ? DEFAULT_FEED_LIMIT : query.limit, 1, MAX_FEED_LIMIT);

        parse::Result<CursorKey> before_key_res = {};
        std::optional<CursorKey> before_key = std::nullopt;

        if (query.before_cursor.has_value())
        {
            before_key_res = parse::parseHistoryCursor(*query.before_cursor);
            if (!before_key_res.has_value())
            {
                spdlog::error("Failed to parse before cursor: {}", *query.before_cursor);
                return FeedPage {};
            }
            else
            {
                before_key = before_key_res.value();
                if (before_key->chain_id != _default_chain_id
                    || before_key->chain_namespace != _default_chain_namespace)
                {
                    spdlog::warn(
                        "Ignoring feed cursor for chain={} on store default_chain={}:{}",
                        std::format("{}:{}", before_key->chain_namespace, before_key->chain_id),
                        _default_chain_namespace,
                        _default_chain_id);
                    return FeedPage {};
                }
            }
        }

        std::vector<FeedItem> all_items;
        all_items.reserve(limit + 16);

        std::unordered_set<std::string> seen_feed_ids;

        _appendFeedRowsFromDatabase(_read_db, "feed_items_hot", query, before_key, limit + 1, all_items, seen_feed_ids);

        const std::vector<std::filesystem::path> archive_paths = _candidateArchivePaths(before_key);

        for (const auto& archive_path : archive_paths)
        {
            sqlite3* archive_db = nullptr;
            const int open_rc = sqlite3_open_v2(
                archive_path.string().c_str(),
                &archive_db,
                SQLITE_OPEN_READONLY | SQLITE_OPEN_FULLMUTEX,
                nullptr);

            if (open_rc != SQLITE_OK)
            {
                if (archive_db != nullptr)
                {
                    sqlite3_close(archive_db);
                }
                continue;
            }

            sqlite3_busy_timeout(archive_db, 10'000);

            _appendFeedRowsFromDatabase(
                archive_db,
                "feed_items_archive",
                query,
                before_key,
                limit + 1,
                all_items,
                seen_feed_ids);

            sqlite3_close(archive_db);
            archive_db = nullptr;
        }

        std::ranges::sort(all_items, _feedDescComparator);

        FeedPage page {};
        page.has_more = all_items.size() > limit;

        if (page.has_more)
        {
            all_items.resize(limit);
        }

        page.items = std::move(all_items);

        if (page.has_more && !page.items.empty())
        {
            page.next_before_cursor = page.items.back().history_cursor;
        }

        return page;
    }

    StreamPage SQLiteHotStore::getStreamPage(const StreamQuery& query) const
    {
        const std::size_t limit =
            std::clamp<std::size_t>((query.limit == 0) ? DEFAULT_STREAM_LIMIT : query.limit, 1, MAX_STREAM_LIMIT);
        const std::string chain_prefix = std::format("{}:{}:%", _default_chain_namespace, _default_chain_id);

        StreamPage page {};

        {
            storage::sqlite::Statement replay_floor_stmt(
                _read_db,
                "SELECT replay_floor_seq FROM outbox_stream_state WHERE singleton=1;");
            const int replay_floor_rc = replay_floor_stmt.step();
            if (replay_floor_rc == SQLITE_ROW)
            {
                page.replay_floor_seq = static_cast<std::int64_t>(sqlite3_column_int64(replay_floor_stmt.get(), 0));
            }
            else if (replay_floor_rc != SQLITE_DONE)
            {
                throw std::runtime_error(sqlite3_errmsg(_read_db));
            }

            storage::sqlite::Statement min_seq_stmt(_read_db, "SELECT MIN(stream_seq) FROM global_outbox WHERE feed_id LIKE ?1;");
            sqlite3_bind_text(
                min_seq_stmt.get(),
                1,
                chain_prefix.c_str(),
                static_cast<int>(chain_prefix.size()),
                SQLITE_TRANSIENT);
            std::int64_t min_stream_seq = 0;
            const int min_seq_rc = min_seq_stmt.step();
            if (min_seq_rc == SQLITE_ROW)
            {
                min_stream_seq = (sqlite3_column_type(min_seq_stmt.get(), 0) == SQLITE_NULL)
                    ? 0
                    : static_cast<std::int64_t>(sqlite3_column_int64(min_seq_stmt.get(), 0));
            }
            else if (min_seq_rc != SQLITE_DONE)
            {
                throw std::runtime_error(sqlite3_errmsg(_read_db));
            }
            page.min_available_seq = std::max(page.replay_floor_seq, min_stream_seq);
        }

        page.stale_since_seq = query.since_seq > 0 && query.since_seq < page.min_available_seq;

        storage::sqlite::Statement stream_stmt(
            _read_db,
            "SELECT "
            "o.stream_seq, o.status, o.feed_id, o.history_cursor, o.payload_json, o.created_at_ms, o.event_type "
            "FROM global_outbox o "
            "WHERE o.stream_seq > ?1 AND o.feed_id LIKE ?2 "
            "ORDER BY o.stream_seq ASC "
            "LIMIT ?3;");
        sqlite3_bind_int64(stream_stmt.get(), 1, static_cast<sqlite3_int64>(query.since_seq));
        sqlite3_bind_text(
            stream_stmt.get(),
            2,
            chain_prefix.c_str(),
            static_cast<int>(chain_prefix.size()),
            SQLITE_TRANSIENT);
        sqlite3_bind_int64(stream_stmt.get(), 3, static_cast<sqlite3_int64>(limit + 1));

        int stream_rc = SQLITE_OK;
        while ((stream_rc = stream_stmt.step()) == SQLITE_ROW)
        {
            StreamDelta delta {};
            delta.stream_seq = static_cast<std::int64_t>(sqlite3_column_int64(stream_stmt.get(), 0));
            delta.status = reinterpret_cast<const char*>(sqlite3_column_text(stream_stmt.get(), 1));
            delta.feed_id = reinterpret_cast<const char*>(sqlite3_column_text(stream_stmt.get(), 2));
            delta.history_cursor = reinterpret_cast<const char*>(sqlite3_column_text(stream_stmt.get(), 3));
            const std::string payload_json = reinterpret_cast<const char*>(sqlite3_column_text(stream_stmt.get(), 4));
            delta.created_at_ms = static_cast<std::int64_t>(sqlite3_column_int64(stream_stmt.get(), 5));
            const unsigned char * event_type_txt = sqlite3_column_text(stream_stmt.get(), 6);
            delta.event_type = (event_type_txt == nullptr)
                ? std::string{}
                : std::string(reinterpret_cast<const char *>(event_type_txt));
            delta.payload = json::parse(payload_json, nullptr, false);
            if (delta.payload.is_discarded())
            {
                delta.payload = json::object();
            }
            page.deltas.push_back(std::move(delta));
        }
        if (stream_rc != SQLITE_DONE)
        {
            throw std::runtime_error(sqlite3_errmsg(_read_db));
        }

        page.has_more = page.deltas.size() > limit;
        if (page.has_more)
        {
            page.deltas.resize(limit);
        }
        if (!page.deltas.empty())
        {
            page.last_seq = page.deltas.back().stream_seq;
        }
        return page;
    }

    std::int64_t SQLiteHotStore::minAvailableStreamSeq() const
    {
        const std::string chain_prefix = std::format("{}:{}:%", _default_chain_namespace, _default_chain_id);
        std::int64_t replay_floor_seq = 0;
        {
            storage::sqlite::Statement floor_stmt(_read_db, "SELECT replay_floor_seq FROM outbox_stream_state WHERE singleton=1;");
            const int floor_rc = floor_stmt.step();
            if (floor_rc == SQLITE_ROW)
            {
                replay_floor_seq = static_cast<std::int64_t>(sqlite3_column_int64(floor_stmt.get(), 0));
            }
            else if (floor_rc != SQLITE_DONE)
            {
                throw std::runtime_error(sqlite3_errmsg(_read_db));
            }
        }
        storage::sqlite::Statement min_stmt(_read_db, "SELECT MIN(stream_seq) FROM global_outbox WHERE feed_id LIKE ?1;");
        sqlite3_bind_text(
            min_stmt.get(),
            1,
            chain_prefix.c_str(),
            static_cast<int>(chain_prefix.size()),
            SQLITE_TRANSIENT);
        const int min_rc = min_stmt.step();
        if (min_rc == SQLITE_ROW)
        {
            const std::int64_t min_stream_seq = (sqlite3_column_type(min_stmt.get(), 0) == SQLITE_NULL)
                ? 0
                : static_cast<std::int64_t>(sqlite3_column_int64(min_stmt.get(), 0));
            return std::max(replay_floor_seq, min_stream_seq);
        }
        if (min_rc != SQLITE_DONE)
        {
            throw std::runtime_error(sqlite3_errmsg(_read_db));
        }
        return replay_floor_seq;
    }

    bool SQLiteHotStore::_initializeHotSchema()
    {
        const bool schema_ok =
                storage::sqlite::exec(_write_db,
                    "CREATE TABLE IF NOT EXISTS raw_events_hot ("
                    "chain_id INTEGER NOT NULL,"
                    "block_hash TEXT NOT NULL,"
                    "log_index INTEGER NOT NULL,"
                    "tx_hash TEXT NOT NULL,"
                    "block_number INTEGER NOT NULL,"
                    "tx_index INTEGER NOT NULL,"
                    "block_time INTEGER,"
                    "address TEXT NOT NULL,"
                    "topic0 TEXT NOT NULL,"
                    "topic1 TEXT,"
                    "topic2 TEXT,"
                    "topic3 TEXT,"
                    "data_hex TEXT NOT NULL,"
                    "removed INTEGER NOT NULL DEFAULT 0,"
                    "state TEXT NOT NULL,"
                    "seen_at_ms INTEGER NOT NULL,"
                    "updated_at_ms INTEGER NOT NULL,"
                    "removed_at_ms INTEGER,"
                    "PRIMARY KEY(chain_id, block_hash, log_index)"
                    ");") &&
                storage::sqlite::exec(_write_db,
                     "CREATE TABLE IF NOT EXISTS normalized_events_hot ("
                    "chain_id INTEGER NOT NULL,"
                    "block_hash TEXT NOT NULL,"
                    "log_index INTEGER NOT NULL,"
                    "tx_hash TEXT NOT NULL,"
                    "block_number INTEGER NOT NULL,"
                    "tx_index INTEGER NOT NULL,"
                    "block_time INTEGER,"
                    "event_type TEXT NOT NULL,"
                    "name TEXT NOT NULL,"
                    "caller TEXT NOT NULL,"
                    "owner TEXT NOT NULL,"
                    "entity_address TEXT NOT NULL,"
                    "args_count INTEGER,"
                    "format_hash TEXT,"
                    "state TEXT NOT NULL,"
                    "seen_at_ms INTEGER NOT NULL,"
                    "updated_at_ms INTEGER NOT NULL,"
                    "projected_version INTEGER NOT NULL DEFAULT 0,"
                    "projected_at_ms INTEGER,"
                     "exported INTEGER NOT NULL DEFAULT 0,"
                     "PRIMARY KEY(chain_id, block_hash, log_index)"
                     ");") &&
               storage::sqlite::exec(_write_db,
                    "CREATE TABLE IF NOT EXISTS decode_failures_hot ("
                    "chain_id INTEGER NOT NULL,"
                    "block_hash TEXT NOT NULL,"
                    "log_index INTEGER NOT NULL,"
                    "last_error TEXT NOT NULL,"
                    "attempts INTEGER NOT NULL,"
                    "first_seen_at_ms INTEGER NOT NULL,"
                    "last_seen_at_ms INTEGER NOT NULL,"
                    "retryable INTEGER NOT NULL DEFAULT 1,"
                    "dead_letter INTEGER NOT NULL DEFAULT 0,"
                    "PRIMARY KEY(chain_id, block_hash, log_index)"
                    ");") &&
               storage::sqlite::exec(_write_db,
                    "CREATE TABLE IF NOT EXISTS projection_jobs ("
                    "chain_id INTEGER NOT NULL,"
                    "block_hash TEXT NOT NULL,"
                    "log_index INTEGER NOT NULL,"
                    "created_at_ms INTEGER NOT NULL,"
                    "PRIMARY KEY(chain_id, block_hash, log_index)"
                    ");") &&
               storage::sqlite::exec(_write_db,
                    "CREATE TABLE IF NOT EXISTS feed_items_hot ("
                    "feed_id TEXT PRIMARY KEY,"
                    "chain_id INTEGER NOT NULL,"
                    "tx_hash TEXT NOT NULL,"
                    "log_index INTEGER NOT NULL,"
                    "block_number INTEGER NOT NULL,"
                    "tx_index INTEGER NOT NULL,"
                    "block_time INTEGER,"
                    "event_type TEXT NOT NULL,"
                    "status TEXT NOT NULL,"
                    "visible INTEGER NOT NULL,"
                    "history_cursor TEXT NOT NULL,"
                    "payload_json TEXT NOT NULL,"
                    "created_at_ms INTEGER NOT NULL,"
                    "updated_at_ms INTEGER NOT NULL,"
                    "projector_version INTEGER NOT NULL,"
                    "exported INTEGER NOT NULL DEFAULT 0,"
                    "stream_emitted INTEGER NOT NULL DEFAULT 0"
                    ");") &&
               storage::sqlite::exec(_write_db,
                    "CREATE TABLE IF NOT EXISTS global_outbox ("
                    "stream_seq INTEGER PRIMARY KEY,"
                    "feed_id TEXT NOT NULL,"
                    "op TEXT NOT NULL,"
                    "status TEXT NOT NULL,"
                    "event_type TEXT NOT NULL,"
                    "history_cursor TEXT NOT NULL,"
                    "payload_json TEXT NOT NULL,"
                    "created_at_ms INTEGER NOT NULL"
                    ");") &&
               storage::sqlite::exec(_write_db,
                    "CREATE TABLE IF NOT EXISTS outbox_stream_state ("
                    "singleton INTEGER PRIMARY KEY CHECK(singleton=1),"
                    "next_stream_seq INTEGER NOT NULL,"
                    "replay_floor_seq INTEGER NOT NULL DEFAULT 0"
                    ");") &&
               storage::sqlite::exec(_write_db,
                    "INSERT OR IGNORE INTO outbox_stream_state(singleton, next_stream_seq, replay_floor_seq) "
                    "VALUES(1, "
                    "COALESCE((SELECT MAX(stream_seq) + 1 FROM global_outbox), 1), "
                    "COALESCE((SELECT MIN(stream_seq) FROM global_outbox), 0)"
                    ");") &&
               storage::sqlite::exec(_write_db,
                    "UPDATE outbox_stream_state "
                    "SET next_stream_seq=MAX(next_stream_seq, COALESCE((SELECT MAX(stream_seq) + 1 FROM global_outbox), 1)), "
                    "replay_floor_seq=MAX(replay_floor_seq, COALESCE((SELECT MIN(stream_seq) FROM global_outbox), 0)) "
                    "WHERE singleton=1;") &&
               storage::sqlite::exec(_write_db,
                    "CREATE TABLE IF NOT EXISTS ingest_resume_state ("
                    "chain_id INTEGER PRIMARY KEY,"
                    "next_from_block INTEGER NOT NULL,"
                    "updated_at_ms INTEGER NOT NULL"
                    ");") &&
               storage::sqlite::exec(_write_db,
                    "CREATE TABLE IF NOT EXISTS local_ingest_resume_state ("
                    "chain_id INTEGER PRIMARY KEY,"
                    "next_seq INTEGER NOT NULL,"
                    "updated_at_ms INTEGER NOT NULL"
                    ");") &&
               storage::sqlite::exec(_write_db,
                    "CREATE TABLE IF NOT EXISTS finality_state ("
                    "chain_id INTEGER PRIMARY KEY,"
                    "head_block INTEGER NOT NULL,"
                    "safe_block INTEGER NOT NULL,"
                    "finalized_block INTEGER NOT NULL,"
                    "updated_at_ms INTEGER NOT NULL"
                    ");") &&
               storage::sqlite::exec(_write_db,
                    "CREATE TABLE IF NOT EXISTS reorg_window ("
                    "chain_id INTEGER NOT NULL,"
                    "block_number INTEGER NOT NULL,"
                    "block_hash TEXT NOT NULL,"
                    "parent_hash TEXT,"
                    "seen_at_ms INTEGER NOT NULL,"
                    "PRIMARY KEY(chain_id, block_number)"
                    ");") &&
               storage::sqlite::exec(_write_db,
                    "CREATE TABLE IF NOT EXISTS archive_jobs ("
                    "job_id INTEGER PRIMARY KEY,"
                    "chain_id INTEGER NOT NULL,"
                    "archive_month TEXT NOT NULL,"
                    "state TEXT NOT NULL,"
                    "details_json TEXT NOT NULL,"
                    "created_at_ms INTEGER NOT NULL,"
                    "updated_at_ms INTEGER NOT NULL"
                    ");") &&
               storage::sqlite::exec(_write_db,
                    "CREATE TABLE IF NOT EXISTS shard_catalog ("
                    "chain_id INTEGER NOT NULL,"
                    "archive_month TEXT NOT NULL,"
                    "path TEXT NOT NULL,"
                    "state TEXT NOT NULL,"
                    "min_block INTEGER NOT NULL,"
                    "max_block INTEGER NOT NULL,"
                    "row_count INTEGER NOT NULL,"
                    "last_export_ms INTEGER NOT NULL,"
                    "PRIMARY KEY(chain_id, archive_month)"
                    ");") &&
               storage::sqlite::exec(_write_db,
                    "CREATE INDEX IF NOT EXISTS idx_raw_chain_tx_log ON raw_events_hot(chain_id, tx_hash, log_index);") &&
               storage::sqlite::exec(_write_db, "CREATE INDEX IF NOT EXISTS idx_raw_block ON raw_events_hot(chain_id, block_number);") &&
               storage::sqlite::exec(_write_db,
                    "CREATE INDEX IF NOT EXISTS idx_norm_chain_tx_log ON normalized_events_hot(chain_id, tx_hash, "
                    "log_index);") &&
               storage::sqlite::exec(_write_db,
                    "CREATE INDEX IF NOT EXISTS idx_norm_chain_block ON normalized_events_hot(chain_id, block_number);") &&
               storage::sqlite::exec(_write_db,
                    "CREATE INDEX IF NOT EXISTS idx_decode_failures_retry "
                    "ON decode_failures_hot(chain_id, dead_letter, retryable, last_seen_at_ms);") &&
               storage::sqlite::exec(_write_db,
                    "CREATE INDEX IF NOT EXISTS idx_feed_visible_order ON feed_items_hot(block_number DESC, tx_index DESC, "
                    "log_index DESC, feed_id DESC) WHERE visible=1;") &&
               storage::sqlite::exec(_write_db,
                    "CREATE INDEX IF NOT EXISTS idx_feed_chain_visible_order "
                    "ON feed_items_hot(chain_id, block_number DESC, tx_index DESC, log_index DESC, feed_id DESC) "
                    "WHERE visible=1;") &&
               storage::sqlite::exec(_write_db,
                    "CREATE INDEX IF NOT EXISTS idx_feed_event_type_order ON feed_items_hot(event_type, block_number DESC, "
                    "tx_index DESC, log_index DESC, feed_id DESC);") &&
                storage::sqlite::exec(_write_db,
                    "CREATE INDEX IF NOT EXISTS idx_feed_visible_updated_order "
                    "ON feed_items_hot(created_at_ms DESC, block_number DESC, tx_index DESC, feed_id DESC) "
                    "WHERE visible=1;") &&
                storage::sqlite::exec(_write_db,
                    "CREATE INDEX IF NOT EXISTS idx_feed_chain_visible_updated_order "
                    "ON feed_items_hot(chain_id, created_at_ms DESC, block_number DESC, tx_index DESC, "
                    "feed_id DESC) WHERE visible=1;") &&
                storage::sqlite::exec(_write_db,
                    "CREATE INDEX IF NOT EXISTS idx_feed_event_type_updated_order "
                    "ON feed_items_hot(event_type, created_at_ms DESC, block_number DESC, tx_index DESC, "
                    "feed_id DESC);") &&
               storage::sqlite::exec(_write_db,
                    "CREATE INDEX IF NOT EXISTS idx_feed_chain_tx_log "
                    "ON feed_items_hot(chain_id, tx_hash, log_index);") &&
               storage::sqlite::exec(_write_db,
                    "CREATE INDEX IF NOT EXISTS idx_norm_archive_ready "
                    "ON normalized_events_hot(chain_id, state, projected_version, exported, block_time);") &&
               storage::sqlite::exec(_write_db,
                    "CREATE INDEX IF NOT EXISTS idx_feed_archive_ready "
                    "ON feed_items_hot(chain_id, status, exported, block_time);") &&
               storage::sqlite::exec(_write_db, "CREATE INDEX IF NOT EXISTS idx_outbox_feed_id ON global_outbox(feed_id);") &&
               storage::sqlite::exec(_write_db, "CREATE INDEX IF NOT EXISTS idx_outbox_created_at ON global_outbox(created_at_ms);") &&
               storage::sqlite::exec(
                   _write_db,
                   "CREATE INDEX IF NOT EXISTS idx_catalog_state_block ON shard_catalog(chain_id, state, max_block DESC);");

        if (!schema_ok)
        {
            return false;
        }

        return true;
    }

    bool SQLiteHotStore::_initializeArchiveSchema(sqlite3* archive_db) const
    {
        return storage::sqlite::exec(archive_db,
                    "CREATE TABLE IF NOT EXISTS normalized_events_archive ("
                    "chain_id INTEGER NOT NULL,"
                    "block_hash TEXT NOT NULL,"
                    "log_index INTEGER NOT NULL,"
                    "tx_hash TEXT NOT NULL,"
                    "block_number INTEGER NOT NULL,"
                    "tx_index INTEGER NOT NULL,"
                    "block_time INTEGER,"
                    "event_type TEXT NOT NULL,"
                    "name TEXT NOT NULL,"
                    "caller TEXT NOT NULL,"
                    "owner TEXT NOT NULL,"
                    "entity_address TEXT NOT NULL,"
                    "args_count INTEGER,"
                    "format_hash TEXT,"
                    "state TEXT NOT NULL,"
                    "seen_at_ms INTEGER NOT NULL,"
                    "updated_at_ms INTEGER NOT NULL,"
                    "PRIMARY KEY(chain_id, block_hash, log_index)"
                    ");") &&
               storage::sqlite::exec(archive_db,
                    "CREATE TABLE IF NOT EXISTS feed_items_archive ("
                    "feed_id TEXT PRIMARY KEY,"
                    "chain_id INTEGER NOT NULL,"
                    "tx_hash TEXT NOT NULL,"
                    "log_index INTEGER NOT NULL,"
                    "block_number INTEGER NOT NULL,"
                    "tx_index INTEGER NOT NULL,"
                    "block_time INTEGER,"
                    "event_type TEXT NOT NULL,"
                    "status TEXT NOT NULL,"
                    "visible INTEGER NOT NULL,"
                    "history_cursor TEXT NOT NULL,"
                    "payload_json TEXT NOT NULL,"
                    "created_at_ms INTEGER NOT NULL,"
                    "updated_at_ms INTEGER NOT NULL,"
                    "projector_version INTEGER NOT NULL"
                    ");") &&
               storage::sqlite::exec(archive_db,
                    "CREATE INDEX IF NOT EXISTS idx_archive_feed_visible_order ON feed_items_archive(block_number DESC, "
                    "tx_index DESC, log_index DESC, feed_id DESC) WHERE visible=1;") &&
               storage::sqlite::exec(archive_db,
                    "CREATE INDEX IF NOT EXISTS idx_archive_feed_chain_visible_order "
                    "ON feed_items_archive(chain_id, block_number DESC, tx_index DESC, log_index DESC, feed_id DESC) "
                    "WHERE visible=1;") &&
               storage::sqlite::exec(archive_db,
                    "CREATE INDEX IF NOT EXISTS idx_archive_feed_type_order ON feed_items_archive(event_type, block_number "
                    "DESC, tx_index DESC, log_index DESC, feed_id DESC);") &&
               storage::sqlite::exec(archive_db,
                    "CREATE INDEX IF NOT EXISTS idx_archive_feed_visible_updated_order "
                    "ON feed_items_archive(created_at_ms DESC, block_number DESC, tx_index DESC, "
                    "feed_id DESC) WHERE visible=1;") &&
               storage::sqlite::exec(archive_db,
                    "CREATE INDEX IF NOT EXISTS idx_archive_feed_chain_visible_updated_order "
                    "ON feed_items_archive(chain_id, created_at_ms DESC, block_number DESC, tx_index DESC, "
                    "feed_id DESC) WHERE visible=1;") &&
               storage::sqlite::exec(archive_db,
                    "CREATE INDEX IF NOT EXISTS idx_archive_feed_type_updated_order "
                    "ON feed_items_archive(event_type, created_at_ms DESC, block_number DESC, tx_index DESC, "
                    "feed_id DESC);");
    }


    bool SQLiteHotStore::_exportMonth(
        const int chain_id,
        const std::string& month_token,
        const std::int64_t now_ms)
    {
        

        const auto month_bounds = parseMonthBounds(month_token);
        if (!month_bounds.has_value())
        {
            return false;
        }

        EventShardId shard_id{.chain_id = chain_id, .year = month_bounds->year, .month = month_bounds->month};
        const std::filesystem::path archive_path = _shard_router->filenameFor(shard_id);

        std::vector<NormalizedArchiveRow> normalized_rows;
        std::vector<NormalizedHotKey> normalized_hot_keys;
        std::vector<FeedArchiveRow> feed_rows;
        std::vector<FeedHotKey> feed_hot_keys;
        {
            {
                storage::sqlite::Statement select_norm(_write_db,
                                      "SELECT "
                                      "chain_id, block_hash, log_index, tx_hash, block_number, tx_index, block_time, "
                                      "event_type, name, caller, owner, entity_address, args_count, format_hash, "
                                      "state, seen_at_ms, updated_at_ms, projected_version "
                                      "FROM normalized_events_hot n "
                                      "WHERE n.chain_id=?1 AND n.state='finalized' AND n.projected_version>=?2 AND n.exported=0 "
                                      "AND n.block_time IS NOT NULL AND n.block_time>=?3 AND n.block_time<?4 "
                                      "AND EXISTS ("
                                      "SELECT 1 FROM feed_items_hot f "
                                      "WHERE f.chain_id=n.chain_id AND f.tx_hash=n.tx_hash AND f.log_index=n.log_index"
                                      ") "
                                      "AND NOT EXISTS ("
                                      "SELECT 1 FROM projection_jobs j "
                                      "WHERE j.chain_id=n.chain_id AND j.block_hash=n.block_hash AND j.log_index=n.log_index"
                                      ");");

                sqlite3_bind_int(select_norm.get(), 1, chain_id);
                sqlite3_bind_int(select_norm.get(), 2, CURRENT_PROJECTOR_VERSION);
                sqlite3_bind_int64(select_norm.get(), 3, static_cast<sqlite3_int64>(month_bounds->start_block_time));
                sqlite3_bind_int64(select_norm.get(), 4, static_cast<sqlite3_int64>(month_bounds->end_block_time));

                int select_norm_rc = SQLITE_OK;
                while ((select_norm_rc = select_norm.step()) == SQLITE_ROW)
                {
                    NormalizedArchiveRow row{};
                    row.chain_id = sqlite3_column_int(select_norm.get(), 0);
                    row.block_hash = reinterpret_cast<const char*>(sqlite3_column_text(select_norm.get(), 1));
                    row.log_index = static_cast<std::int64_t>(sqlite3_column_int64(select_norm.get(), 2));
                    row.tx_hash = reinterpret_cast<const char*>(sqlite3_column_text(select_norm.get(), 3));
                    row.block_number = static_cast<std::int64_t>(sqlite3_column_int64(select_norm.get(), 4));
                    row.tx_index = static_cast<std::int64_t>(sqlite3_column_int64(select_norm.get(), 5));
                    row.block_time = _columnInt64Optional(select_norm.get(), 6);
                    row.event_type = reinterpret_cast<const char*>(sqlite3_column_text(select_norm.get(), 7));
                    row.name = reinterpret_cast<const char*>(sqlite3_column_text(select_norm.get(), 8));
                    row.caller = reinterpret_cast<const char*>(sqlite3_column_text(select_norm.get(), 9));
                    row.owner = reinterpret_cast<const char*>(sqlite3_column_text(select_norm.get(), 10));
                    row.entity_address = reinterpret_cast<const char*>(sqlite3_column_text(select_norm.get(), 11));
                    row.args_count = _columnInt64Optional(select_norm.get(), 12);
                    row.format_hash = _columnTextOptional(select_norm.get(), 13);
                    row.state = reinterpret_cast<const char*>(sqlite3_column_text(select_norm.get(), 14));
                    row.seen_at_ms = static_cast<std::int64_t>(sqlite3_column_int64(select_norm.get(), 15));
                    row.updated_at_ms = static_cast<std::int64_t>(sqlite3_column_int64(select_norm.get(), 16));
                    const int selected_projected_version = sqlite3_column_int(select_norm.get(), 17);
                    normalized_hot_keys.push_back(NormalizedHotKey{
                        .chain_id = row.chain_id,
                        .block_hash = row.block_hash,
                        .log_index = row.log_index,
                        .projected_version = selected_projected_version,
                        .updated_at_ms = row.updated_at_ms
                        });
                    normalized_rows.push_back(std::move(row));
                }
                if (select_norm_rc != SQLITE_DONE)
                {
                    throw std::runtime_error(sqlite3_errmsg(_write_db));
                }
            }

            {
                storage::sqlite::Statement select_feed(_write_db,
                                      "SELECT "
                                      "feed_id, chain_id, tx_hash, log_index, block_number, tx_index, block_time, "
                                      "event_type, status, visible, history_cursor, payload_json, "
                                      "created_at_ms, updated_at_ms, projector_version "
                                      "FROM feed_items_hot f "
                                      "WHERE f.chain_id=?1 AND f.status='finalized' AND f.exported=0 "
                                      "AND f.block_time IS NOT NULL AND f.block_time>=?2 AND f.block_time<?3 "
                                      "AND EXISTS ("
                                      "SELECT 1 FROM normalized_events_hot n2 "
                                      "WHERE n2.chain_id=f.chain_id AND n2.tx_hash=f.tx_hash AND n2.log_index=f.log_index "
                                      "AND n2.projected_version>=?4"
                                      ") "
                                      "AND NOT EXISTS ("
                                      "SELECT 1 "
                                      "FROM normalized_events_hot n "
                                      "JOIN projection_jobs j "
                                      "ON j.chain_id=n.chain_id AND j.block_hash=n.block_hash AND j.log_index=n.log_index "
                                      "WHERE n.chain_id=f.chain_id AND n.tx_hash=f.tx_hash AND n.log_index=f.log_index"
                                      ");");

                sqlite3_bind_int(select_feed.get(), 1, chain_id);
                sqlite3_bind_int64(select_feed.get(), 2, static_cast<sqlite3_int64>(month_bounds->start_block_time));
                sqlite3_bind_int64(select_feed.get(), 3, static_cast<sqlite3_int64>(month_bounds->end_block_time));
                sqlite3_bind_int(select_feed.get(), 4, CURRENT_PROJECTOR_VERSION);

                int select_feed_rc = SQLITE_OK;
                while ((select_feed_rc = select_feed.step()) == SQLITE_ROW)
                {
                    FeedArchiveRow row{};
                    row.feed_id = reinterpret_cast<const char*>(sqlite3_column_text(select_feed.get(), 0));
                    row.chain_id = sqlite3_column_int(select_feed.get(), 1);
                    row.tx_hash = reinterpret_cast<const char*>(sqlite3_column_text(select_feed.get(), 2));
                    row.log_index = static_cast<std::int64_t>(sqlite3_column_int64(select_feed.get(), 3));
                    row.block_number = static_cast<std::int64_t>(sqlite3_column_int64(select_feed.get(), 4));
                    row.tx_index = static_cast<std::int64_t>(sqlite3_column_int64(select_feed.get(), 5));
                    row.block_time = _columnInt64Optional(select_feed.get(), 6);
                    row.event_type = reinterpret_cast<const char*>(sqlite3_column_text(select_feed.get(), 7));
                    row.status = reinterpret_cast<const char*>(sqlite3_column_text(select_feed.get(), 8));
                    row.visible = sqlite3_column_int(select_feed.get(), 9) != 0;
                    row.history_cursor = reinterpret_cast<const char*>(sqlite3_column_text(select_feed.get(), 10));
                    row.payload_json = reinterpret_cast<const char*>(sqlite3_column_text(select_feed.get(), 11));
                    row.created_at_ms = static_cast<std::int64_t>(sqlite3_column_int64(select_feed.get(), 12));
                    row.updated_at_ms = static_cast<std::int64_t>(sqlite3_column_int64(select_feed.get(), 13));
                    row.projector_version = sqlite3_column_int(select_feed.get(), 14);
                    feed_hot_keys.push_back(FeedHotKey{
                        .chain_id = row.chain_id,
                        .feed_id = row.feed_id,
                        .projector_version = row.projector_version,
                        .updated_at_ms = row.updated_at_ms,
                        .history_cursor = row.history_cursor,
                        .payload_json = row.payload_json});
                    feed_rows.push_back(std::move(row));
                }
                if (select_feed_rc != SQLITE_DONE)
                {
                    throw std::runtime_error(sqlite3_errmsg(_write_db));
                }
            }
        }

        if (normalized_rows.empty() && feed_rows.empty())
        {
            return true;
        }

        const bool archive_write_ok =
            [this,
             month_token,
             archive_path,
             normalized_rows = std::move(normalized_rows),
             feed_rows = std::move(feed_rows)]() mutable -> bool
            {
                std::error_code dir_ec;
                std::filesystem::create_directories(archive_path.parent_path(), dir_ec);
                if (dir_ec)
                {
                    spdlog::error(
                        "Failed to create archive shard directory '{}': {}",
                        archive_path.parent_path().string(),
                        dir_ec.message());
                    return false;
                }

                sqlite3* archive_db = nullptr;
                const int open_rc = sqlite3_open_v2(archive_path.string().c_str(),
                                                    &archive_db,
                                                    SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_FULLMUTEX,
                                                    nullptr);
                if (open_rc != SQLITE_OK)
                {
                    const std::string err = (archive_db == nullptr) ? "sqlite open failed" : sqlite3_errmsg(archive_db);
                    if (archive_db != nullptr)
                    {
                        sqlite3_close(archive_db);
                        archive_db = nullptr;
                    }
                    spdlog::error("Failed to open archive shard '{}': {}", archive_path.string(), err);
                    return false;
                }

                sqlite3_busy_timeout(archive_db, 10'000);

                if (!storage::sqlite::exec(archive_db, "PRAGMA journal_mode=WAL;") 
                    || !storage::sqlite::exec(archive_db, "PRAGMA synchronous=NORMAL;") 
                    || !storage::sqlite::exec(archive_db, "PRAGMA temp_store=MEMORY;") 
                    || !storage::sqlite::exec(archive_db, "PRAGMA foreign_keys=OFF;"))
                {
                    sqlite3_close(archive_db);
                    archive_db = nullptr;
                    return false;
                }

                if (!_initializeArchiveSchema(archive_db))
                {
                    sqlite3_close(archive_db);
                    archive_db = nullptr;
                    return false;
                }

                if (!storage::sqlite::exec(archive_db, "BEGIN IMMEDIATE TRANSACTION;"))
                {
                    sqlite3_close(archive_db);
                    archive_db = nullptr;
                    return false;
                }

                try
                {
                    storage::sqlite::Statement insert_norm(archive_db,
                                          "INSERT INTO normalized_events_archive("
                                          "chain_id, block_hash, log_index, tx_hash, block_number, tx_index, block_time, "
                                          "event_type, name, caller, owner, entity_address, args_count, format_hash, "
                                          "state, seen_at_ms, updated_at_ms"
                                          ") VALUES("
                                          "?1, ?2, ?3, ?4, ?5, ?6, ?7, "
                                          "?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17"
                                          ") ON CONFLICT(chain_id, block_hash, log_index) DO UPDATE SET "
                                          "tx_hash=excluded.tx_hash, block_number=excluded.block_number, "
                                          "tx_index=excluded.tx_index, block_time=excluded.block_time, "
                                          "event_type=excluded.event_type, name=excluded.name, caller=excluded.caller, "
                                          "owner=excluded.owner, entity_address=excluded.entity_address, "
                                          "args_count=excluded.args_count, format_hash=excluded.format_hash, "
                                          "state=excluded.state, "
                                          "seen_at_ms=excluded.seen_at_ms, updated_at_ms=excluded.updated_at_ms;");

                    for (const auto& row : normalized_rows)
                    {
                        sqlite3_bind_int(insert_norm.get(), 1, row.chain_id);
                        sqlite3_bind_text(insert_norm.get(),
                                          2,
                                          row.block_hash.c_str(),
                                          static_cast<int>(row.block_hash.size()),
                                          SQLITE_TRANSIENT);
                        sqlite3_bind_int64(insert_norm.get(), 3, static_cast<sqlite3_int64>(row.log_index));
                        sqlite3_bind_text(insert_norm.get(), 4, row.tx_hash.c_str(), static_cast<int>(row.tx_hash.size()), SQLITE_TRANSIENT);
                        sqlite3_bind_int64(insert_norm.get(), 5, static_cast<sqlite3_int64>(row.block_number));
                        sqlite3_bind_int64(insert_norm.get(), 6, static_cast<sqlite3_int64>(row.tx_index));
                        _bindOptionalInt64(insert_norm.get(), 7, row.block_time);
                        sqlite3_bind_text(insert_norm.get(),
                                          8,
                                          row.event_type.c_str(),
                                          static_cast<int>(row.event_type.size()),
                                          SQLITE_TRANSIENT);
                        sqlite3_bind_text(insert_norm.get(), 9, row.name.c_str(), static_cast<int>(row.name.size()), SQLITE_TRANSIENT);
                        sqlite3_bind_text(insert_norm.get(), 10, row.caller.c_str(), static_cast<int>(row.caller.size()), SQLITE_TRANSIENT);
                        sqlite3_bind_text(insert_norm.get(), 11, row.owner.c_str(), static_cast<int>(row.owner.size()), SQLITE_TRANSIENT);
                        sqlite3_bind_text(insert_norm.get(),
                                          12,
                                          row.entity_address.c_str(),
                                          static_cast<int>(row.entity_address.size()),
                                          SQLITE_TRANSIENT);
                        _bindOptionalInt64(insert_norm.get(), 13, row.args_count);
                        _bindOptionalText(insert_norm.get(), 14, row.format_hash);
                        sqlite3_bind_text(insert_norm.get(), 15, row.state.c_str(), static_cast<int>(row.state.size()), SQLITE_TRANSIENT);
                        sqlite3_bind_int64(insert_norm.get(), 16, static_cast<sqlite3_int64>(row.seen_at_ms));
                        sqlite3_bind_int64(insert_norm.get(), 17, static_cast<sqlite3_int64>(row.updated_at_ms));

                        if (insert_norm.step() != SQLITE_DONE)
                        {
                            throw std::runtime_error(sqlite3_errmsg(archive_db));
                        }
                        sqlite3_reset(insert_norm.get());
                        sqlite3_clear_bindings(insert_norm.get());
                    }

                    storage::sqlite::Statement insert_feed(archive_db,
                                          "INSERT INTO feed_items_archive("
                                          "feed_id, chain_id, tx_hash, log_index, block_number, tx_index, block_time, "
                                          "event_type, status, visible, history_cursor, payload_json, "
                                          "created_at_ms, updated_at_ms, projector_version"
                                          ") VALUES("
                                          "?1, ?2, ?3, ?4, ?5, ?6, ?7, "
                                          "?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15"
                                          ") ON CONFLICT(feed_id) DO UPDATE SET "
                                          "chain_id=excluded.chain_id, tx_hash=excluded.tx_hash, log_index=excluded.log_index, "
                                          "block_number=excluded.block_number, tx_index=excluded.tx_index, "
                                          "block_time=excluded.block_time, event_type=excluded.event_type, status=excluded.status, "
                                          "visible=excluded.visible, history_cursor=excluded.history_cursor, "
                                          "payload_json=excluded.payload_json, "
                                          "created_at_ms=excluded.created_at_ms, updated_at_ms=excluded.updated_at_ms, "
                                          "projector_version=excluded.projector_version;");

                    for (const auto& row : feed_rows)
                    {
                        sqlite3_bind_text(
                            insert_feed.get(), 1, row.feed_id.c_str(), static_cast<int>(row.feed_id.size()), SQLITE_TRANSIENT);
                        sqlite3_bind_int(insert_feed.get(), 2, row.chain_id);
                        sqlite3_bind_text(
                            insert_feed.get(), 3, row.tx_hash.c_str(), static_cast<int>(row.tx_hash.size()), SQLITE_TRANSIENT);
                        sqlite3_bind_int64(insert_feed.get(), 4, static_cast<sqlite3_int64>(row.log_index));
                        sqlite3_bind_int64(insert_feed.get(), 5, static_cast<sqlite3_int64>(row.block_number));
                        sqlite3_bind_int64(insert_feed.get(), 6, static_cast<sqlite3_int64>(row.tx_index));
                        _bindOptionalInt64(insert_feed.get(), 7, row.block_time);
                        sqlite3_bind_text(insert_feed.get(),
                                          8,
                                          row.event_type.c_str(),
                                          static_cast<int>(row.event_type.size()),
                                          SQLITE_TRANSIENT);
                        sqlite3_bind_text(
                            insert_feed.get(), 9, row.status.c_str(), static_cast<int>(row.status.size()), SQLITE_TRANSIENT);
                        sqlite3_bind_int(insert_feed.get(), 10, row.visible ? 1 : 0);
                        sqlite3_bind_text(insert_feed.get(),
                                          11,
                                          row.history_cursor.c_str(),
                                          static_cast<int>(row.history_cursor.size()),
                                          SQLITE_TRANSIENT);
                        sqlite3_bind_text(insert_feed.get(),
                                          12,
                                          row.payload_json.c_str(),
                                          static_cast<int>(row.payload_json.size()),
                                          SQLITE_TRANSIENT);
                        sqlite3_bind_int64(insert_feed.get(), 13, static_cast<sqlite3_int64>(row.created_at_ms));
                        sqlite3_bind_int64(insert_feed.get(), 14, static_cast<sqlite3_int64>(row.updated_at_ms));
                        sqlite3_bind_int(insert_feed.get(), 15, row.projector_version);
                        if (insert_feed.step() != SQLITE_DONE)
                        {
                            throw std::runtime_error(sqlite3_errmsg(archive_db));
                        }
                        sqlite3_reset(insert_feed.get());
                        sqlite3_clear_bindings(insert_feed.get());
                    }

                    if (!storage::sqlite::exec(archive_db, "COMMIT;"))
                    {
                        throw std::runtime_error("archive commit failed");
                    }
                }
                catch (const std::exception& e)
                {
                    spdlog::error("Archive export write failed for month {}: {}", month_token, e.what());
                    (void)storage::sqlite::exec(archive_db, "ROLLBACK;");
                    sqlite3_close(archive_db);
                    archive_db = nullptr;
                    return false;
                }

                sqlite3_close(archive_db);
                archive_db = nullptr;
                return true;
            }();

        if (!archive_write_ok)
        {
            return false;
        }

        {
            if (!storage::sqlite::exec(_write_db, "BEGIN IMMEDIATE TRANSACTION;"))
            {
                return false;
            }

            try
            {
                storage::sqlite::Statement mark_norm_exported(
                    _write_db,
                    "UPDATE normalized_events_hot SET exported=1, updated_at_ms=?1 "
                    "WHERE chain_id=?2 AND block_hash=?3 AND log_index=?4 "
                    "AND state='finalized' AND projected_version=?5 "
                    "AND updated_at_ms=?6 AND exported=0 "
                    "AND NOT EXISTS ("
                    "SELECT 1 FROM projection_jobs j "
                    "WHERE j.chain_id=normalized_events_hot.chain_id "
                    "AND j.block_hash=normalized_events_hot.block_hash "
                    "AND j.log_index=normalized_events_hot.log_index"
                    ");");

                std::size_t normalized_snapshot_mismatch = 0;
                for (const auto& key : normalized_hot_keys)
                {
                    sqlite3_bind_int64(mark_norm_exported.get(), 1, static_cast<sqlite3_int64>(now_ms));
                    sqlite3_bind_int(mark_norm_exported.get(), 2, key.chain_id);
                    sqlite3_bind_text(mark_norm_exported.get(),
                                      3,
                                      key.block_hash.c_str(),
                                      static_cast<int>(key.block_hash.size()),
                                      SQLITE_TRANSIENT);
                    sqlite3_bind_int64(mark_norm_exported.get(), 4, static_cast<sqlite3_int64>(key.log_index));
                    sqlite3_bind_int(mark_norm_exported.get(), 5, key.projected_version);
                    sqlite3_bind_int64(mark_norm_exported.get(), 6, static_cast<sqlite3_int64>(key.updated_at_ms));

                    if (mark_norm_exported.step() != SQLITE_DONE)
                    {
                        throw std::runtime_error(sqlite3_errmsg(_write_db));
                    }
                    if (sqlite3_changes(_write_db) == 0)
                    {
                        ++normalized_snapshot_mismatch;
                    }
                    sqlite3_reset(mark_norm_exported.get());
                    sqlite3_clear_bindings(mark_norm_exported.get());
                }

                if (normalized_snapshot_mismatch > 0)
                {
                    spdlog::warn(
                        "Archive export finalization: {}/{} normalized rows had snapshot mismatches for month {}; "
                        "leaving them unexported for next cycle",
                        normalized_snapshot_mismatch,
                        normalized_hot_keys.size(),
                        month_token);
                }

                storage::sqlite::Statement mark_feed_exported(
                    _write_db,
                    "UPDATE feed_items_hot SET exported=1, updated_at_ms=?1 "
                    "WHERE feed_id=?2 AND chain_id=?3 AND status='finalized' "
                    "AND projector_version=?4 AND updated_at_ms=?5 "
                    "AND history_cursor=?6 AND payload_json=?7 AND exported=0;");

                std::size_t feed_snapshot_mismatch = 0;
                for (const auto& key : feed_hot_keys)
                {
                    sqlite3_bind_int64(mark_feed_exported.get(), 1, static_cast<sqlite3_int64>(now_ms));
                    sqlite3_bind_text(mark_feed_exported.get(),
                                      2,
                                      key.feed_id.c_str(),
                                      static_cast<int>(key.feed_id.size()),
                                      SQLITE_TRANSIENT);
                    sqlite3_bind_int(mark_feed_exported.get(), 3, key.chain_id);
                    sqlite3_bind_int(mark_feed_exported.get(), 4, key.projector_version);
                    sqlite3_bind_int64(mark_feed_exported.get(), 5, static_cast<sqlite3_int64>(key.updated_at_ms));
                    sqlite3_bind_text(mark_feed_exported.get(),
                                      6,
                                      key.history_cursor.c_str(),
                                      static_cast<int>(key.history_cursor.size()),
                                      SQLITE_TRANSIENT);
                    sqlite3_bind_text(mark_feed_exported.get(),
                                      7,
                                      key.payload_json.c_str(),
                                      static_cast<int>(key.payload_json.size()),
                                      SQLITE_TRANSIENT);
                    if (mark_feed_exported.step() != SQLITE_DONE)
                    {
                        throw std::runtime_error(sqlite3_errmsg(_write_db));
                    }
                    if (sqlite3_changes(_write_db) == 0)
                    {
                        ++feed_snapshot_mismatch;
                    }
                    sqlite3_reset(mark_feed_exported.get());
                    sqlite3_clear_bindings(mark_feed_exported.get());
                }

                if (feed_snapshot_mismatch > 0)
                {
                    spdlog::warn(
                        "Archive export finalization: {}/{} feed rows had snapshot mismatches for month {}; "
                        "leaving them unexported for next cycle",
                        feed_snapshot_mismatch,
                        feed_hot_keys.size(),
                        month_token);
                }

                std::int64_t min_block = 0;
                std::int64_t max_block = 0;
                std::int64_t row_count = 0;
                {
                    storage::sqlite::Statement stats_stmt(_write_db,
                                         "SELECT COALESCE(MIN(block_number), 0), COALESCE(MAX(block_number), 0), COUNT(1) "
                                         "FROM normalized_events_hot "
                                         "WHERE chain_id=?1 AND state='finalized' AND projected_version>=?2 AND exported=1 "
                                         "AND block_time IS NOT NULL AND block_time>=?3 AND block_time<?4;");
                    sqlite3_bind_int(stats_stmt.get(), 1, chain_id);
                    sqlite3_bind_int(stats_stmt.get(), 2, CURRENT_PROJECTOR_VERSION);
                    sqlite3_bind_int64(stats_stmt.get(), 3, static_cast<sqlite3_int64>(month_bounds->start_block_time));
                    sqlite3_bind_int64(stats_stmt.get(), 4, static_cast<sqlite3_int64>(month_bounds->end_block_time));

                    const int stats_rc = stats_stmt.step();
                    if (stats_rc == SQLITE_ROW)
                    {
                        min_block = static_cast<std::int64_t>(sqlite3_column_int64(stats_stmt.get(), 0));
                        max_block = static_cast<std::int64_t>(sqlite3_column_int64(stats_stmt.get(), 1));
                        row_count = static_cast<std::int64_t>(sqlite3_column_int64(stats_stmt.get(), 2));
                    }
                    else if (stats_rc != SQLITE_DONE)
                    {
                        throw std::runtime_error(sqlite3_errmsg(_write_db));
                    }
                }

                storage::sqlite::Statement catalog_stmt(
                    _write_db,
                    "INSERT INTO shard_catalog(chain_id, archive_month, path, state, min_block, max_block, row_count, "
                    "last_export_ms) "
                    "VALUES(?1, ?2, ?3, 'READY', ?4, ?5, ?6, ?7) "
                    "ON CONFLICT(chain_id, archive_month) DO UPDATE SET "
                    "path=excluded.path, state='READY', min_block=excluded.min_block, max_block=excluded.max_block, "
                    "row_count=excluded.row_count, last_export_ms=excluded.last_export_ms;");

                sqlite3_bind_int(catalog_stmt.get(), 1, chain_id);
                sqlite3_bind_text(catalog_stmt.get(), 2, month_token.c_str(), static_cast<int>(month_token.size()), SQLITE_TRANSIENT);

                const std::string archive_path_str = archive_path.string();
                sqlite3_bind_text(catalog_stmt.get(),
                                  3,
                                  archive_path_str.c_str(),
                                  static_cast<int>(archive_path_str.size()),
                                  SQLITE_TRANSIENT);
                sqlite3_bind_int64(catalog_stmt.get(), 4, static_cast<sqlite3_int64>(min_block));
                sqlite3_bind_int64(catalog_stmt.get(), 5, static_cast<sqlite3_int64>(max_block));
                sqlite3_bind_int64(catalog_stmt.get(), 6, static_cast<sqlite3_int64>(row_count));
                sqlite3_bind_int64(catalog_stmt.get(), 7, static_cast<sqlite3_int64>(now_ms));

                if (catalog_stmt.step() != SQLITE_DONE)
                {
                    throw std::runtime_error(sqlite3_errmsg(_write_db));
                }

                if (!storage::sqlite::exec(_write_db, "COMMIT;"))
                {
                    throw std::runtime_error("commit failed");
                }
                return true;
            }
            catch (const std::exception& e)
            {
                spdlog::error("Archive export finalization failed for month {}: {}", month_token, e.what());
                (void)storage::sqlite::exec(_write_db, "ROLLBACK;");
                return false;
            }
        }
    }

    std::vector<std::filesystem::path> SQLiteHotStore::_candidateArchivePaths(
        const std::optional<CursorKey>& before_key) const
    {
        std::vector<std::filesystem::path> paths;

        std::string sql = "SELECT path FROM shard_catalog WHERE chain_id=?1 AND state='READY' ";
        if (before_key.has_value())
        {
            sql += "AND min_block <= ?2 ";
        }
        sql += "ORDER BY max_block DESC;";

        storage::sqlite::Statement stmt(_read_db, sql.c_str());
        sqlite3_bind_int(stmt.get(), 1, before_key.has_value() ? before_key->chain_id : _default_chain_id);
        if (before_key.has_value())
        {
            sqlite3_bind_int64(stmt.get(), 2, static_cast<sqlite3_int64>(before_key->block_number));
        }

        int rc = SQLITE_OK;
        while ((rc = stmt.step()) == SQLITE_ROW)
        {
            const unsigned char* txt = sqlite3_column_text(stmt.get(), 0);
            if (txt == nullptr)
            {
                continue;
            }
            paths.emplace_back(reinterpret_cast<const char*>(txt));
        }
        if (rc != SQLITE_DONE)
        {
            throw std::runtime_error(sqlite3_errmsg(_read_db));
        }
        return paths;
    }

    void SQLiteHotStore::_appendFeedRowsFromDatabase(sqlite3* db,
                                                     const char* table_name,
                                                     const FeedQuery& query,
                                                     const std::optional<CursorKey>& before_key,
                                                     const std::size_t limit,
                                                     std::vector<FeedItem>& out_items,
                                                     std::unordered_set<std::string>& seen_feed_ids) const
    {
        const int chain_id = before_key.has_value() ? before_key->chain_id : _default_chain_id;

        std::string sql = std::format(
            "SELECT "
            "feed_id, event_type, status, visible, tx_hash, block_number, tx_index, log_index, "
            "history_cursor, payload_json, created_at_ms, updated_at_ms, projector_version "
            "FROM {} WHERE chain_id=?1 AND visible=1",
            table_name);

        if (!query.include_unfinalized)
        {
            sql += " AND status='finalized'";
        }
        if (query.event_type.has_value())
        {
            sql += " AND event_type=?2";
        }

        int before_param_base = query.event_type.has_value() ? 3 : 2;
        if (before_key.has_value())
        {
            sql += std::format(" AND ("
                               "created_at_ms < ?{} "
                               "OR (created_at_ms = ?{} AND block_number < ?{}) "
                               "OR (created_at_ms = ?{} AND block_number = ?{} AND tx_index < ?{}) "
                               "OR (created_at_ms = ?{} AND block_number = ?{} AND tx_index = ?{} AND feed_id < ?{})"
                               ")",
                               before_param_base,
                               before_param_base + 1,
                               before_param_base + 2,
                               before_param_base + 3,
                               before_param_base + 4,
                               before_param_base + 5,
                               before_param_base + 6,
                               before_param_base + 7,
                               before_param_base + 8,
                               before_param_base + 9);
        }

        int limit_param = before_param_base;
        if (before_key.has_value())
        {
            limit_param += 10;
        }
        sql +=
            std::format(" ORDER BY created_at_ms DESC, block_number DESC, tx_index DESC, feed_id DESC LIMIT ?{};", limit_param);

        storage::sqlite::Statement stmt(db, sql.c_str());

        int param_index = 1;
        sqlite3_bind_int(stmt.get(), param_index++, chain_id);
        if (query.event_type.has_value())
        {
            sqlite3_bind_text(stmt.get(),
                              param_index,
                              query.event_type->c_str(),
                              static_cast<int>(query.event_type->size()),
                              SQLITE_TRANSIENT);
            ++param_index;
        }

        if (before_key.has_value())
        {
            const sqlite3_int64 before_created = static_cast<sqlite3_int64>(before_key->created_at_ms);
            const sqlite3_int64 before_block = static_cast<sqlite3_int64>(before_key->block_number);
            const sqlite3_int64 before_tx = static_cast<sqlite3_int64>(before_key->tx_index);

            sqlite3_bind_int64(stmt.get(), param_index++, before_created);
            sqlite3_bind_int64(stmt.get(), param_index++, before_created);
            sqlite3_bind_int64(stmt.get(), param_index++, before_block);
            sqlite3_bind_int64(stmt.get(), param_index++, before_created);
            sqlite3_bind_int64(stmt.get(), param_index++, before_block);
            sqlite3_bind_int64(stmt.get(), param_index++, before_tx);
            sqlite3_bind_int64(stmt.get(), param_index++, before_created);
            sqlite3_bind_int64(stmt.get(), param_index++, before_block);
            sqlite3_bind_int64(stmt.get(), param_index++, before_tx);
            sqlite3_bind_text(stmt.get(),
                              param_index++,
                              before_key->feed_id.c_str(),
                              static_cast<int>(before_key->feed_id.size()),
                              SQLITE_TRANSIENT);
        }
        sqlite3_bind_int64(stmt.get(), param_index, static_cast<sqlite3_int64>(limit));

        int rc = SQLITE_OK;
        while ((rc = stmt.step()) == SQLITE_ROW)
        {
            FeedItem item {};
            item.feed_id = reinterpret_cast<const char*>(sqlite3_column_text(stmt.get(), 0));

            if (seen_feed_ids.contains(item.feed_id))
            {
                continue;
            }

            seen_feed_ids.insert(item.feed_id);
            item.event_type = reinterpret_cast<const char*>(sqlite3_column_text(stmt.get(), 1));
            item.status = reinterpret_cast<const char*>(sqlite3_column_text(stmt.get(), 2));
            item.visible = sqlite3_column_int(stmt.get(), 3) != 0;
            item.tx_hash = reinterpret_cast<const char*>(sqlite3_column_text(stmt.get(), 4));
            item.block_number = static_cast<std::int64_t>(sqlite3_column_int64(stmt.get(), 5));
            item.tx_index = static_cast<std::int64_t>(sqlite3_column_int64(stmt.get(), 6));
            item.log_index = static_cast<std::int64_t>(sqlite3_column_int64(stmt.get(), 7));
            item.history_cursor = reinterpret_cast<const char*>(sqlite3_column_text(stmt.get(), 8));
            const std::string payload_json = reinterpret_cast<const char*>(sqlite3_column_text(stmt.get(), 9));
            item.created_at_ms = static_cast<std::int64_t>(sqlite3_column_int64(stmt.get(), 10));
            item.updated_at_ms = static_cast<std::int64_t>(sqlite3_column_int64(stmt.get(), 11));
            item.projector_version = sqlite3_column_int(stmt.get(), 12);
            item.payload = json::parse(payload_json, nullptr, false);

            if (item.payload.is_discarded())
            {
                item.payload = json::object();
            }

            if (before_key.has_value() && !_cursorLessInDescOrder(item, *before_key))
            {
                continue;
            }

            out_items.push_back(std::move(item));
        }
        if (rc != SQLITE_DONE)
        {
            throw std::runtime_error(sqlite3_errmsg(db));
        }
    }
} // namespace dcn::events
