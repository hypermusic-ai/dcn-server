#include <ranges>

#include <spdlog/spdlog.h>

#include "events_hot_store.hpp"
#include "utils.hpp"
#include "parser.hpp"

namespace dcn::parse
{
    Result<events::CursorKey> parseHistoryCursor(const std::string & cursor)
    {
        std::size_t p0 = cursor.find(':');
        if(p0 == std::string::npos)
        {
            return std::unexpected(ParseError{ParseError::Kind::INVALID_VALUE});
        }

        std::size_t p1 = cursor.find(':', p0 + 1);
        if(p1 == std::string::npos)
        {
            return std::unexpected(ParseError{ParseError::Kind::INVALID_VALUE});
        }

        std::size_t p2 = cursor.find(':', p1 + 1);
        if(p2 == std::string::npos)
        {
            return std::unexpected(ParseError{ParseError::Kind::INVALID_VALUE});
        }

        std::size_t p3 = cursor.find(':', p2 + 1);
        if(p3 == std::string::npos)
        {
            return std::unexpected(ParseError{ParseError::Kind::INVALID_VALUE});
        }

        events::CursorKey key{};
        try
        {
            key.chain_id = std::stoi(cursor.substr(0, p0));
            key.block_number = std::stoll(cursor.substr(p0 + 1, p1 - (p0 + 1)));
            key.tx_index = std::stoll(cursor.substr(p1 + 1, p2 - (p1 + 1)));
            key.log_index = std::stoll(cursor.substr(p2 + 1, p3 - (p2 + 1)));
            key.feed_id = cursor.substr(p3 + 1);
        }
        catch(...)
        {
            return std::unexpected(ParseError{ParseError::Kind::INVALID_VALUE});
        }

        if(key.feed_id.empty())
        {
            return std::unexpected(ParseError{ParseError::Kind::INVALID_VALUE});
        }

        return key;
    }
}

namespace dcn::events
{
    //TODO: move to utils?



    static std::string _makeHistoryCursor(
        const int chain_id,
        const std::int64_t block_number,
        const std::int64_t tx_index,
        const std::int64_t log_index,
        const std::string & feed_id)
    {
        return std::format("{}:{}:{}:{}:{}", chain_id, block_number, tx_index, log_index, feed_id);
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
        if(lhs.block_number != rhs.block_number)
        {
            return lhs.block_number > rhs.block_number;
        }
        if(lhs.tx_index != rhs.tx_index)
        {
            return lhs.tx_index > rhs.tx_index;
        }
        if(lhs.log_index != rhs.log_index)
        {
            return lhs.log_index > rhs.log_index;
        }
        return lhs.feed_id > rhs.feed_id;
    }

    static bool _cursorLessInDescOrder(const FeedItem & lhs, const CursorKey & rhs)
    {
        if(lhs.block_number != rhs.block_number)
        {
            return lhs.block_number < rhs.block_number;
        }
        if(lhs.tx_index != rhs.tx_index)
        {
            return lhs.tx_index < rhs.tx_index;
        }
        if(lhs.log_index != rhs.log_index)
        {
            return lhs.log_index < rhs.log_index;
        }
        return lhs.feed_id < rhs.feed_id;
    }

    class Statement final
    {
        public:

            Statement(sqlite3 * db, const char * sql)
            {
                if(sqlite3_prepare_v2(db, sql, -1, &_stmt, nullptr) != SQLITE_OK)
                {
                    throw std::runtime_error(std::format("sqlite prepare failed: {}", sqlite3_errmsg(db)));
                }
            }

            ~Statement()
            {
                if(_stmt != nullptr)
                {
                    sqlite3_finalize(_stmt);
                    _stmt = nullptr;
                }
            }

            sqlite3_stmt * get() const
            {
                return _stmt;
            }

        private:
            sqlite3_stmt * _stmt = nullptr;
    };
    
    bool exec(sqlite3 * db, const char * sql)
    {
        char * err = nullptr;
        const int rc = sqlite3_exec(db, sql, nullptr, nullptr, &err);
        if(rc == SQLITE_OK)
        {
            return true;
        }

        const std::string err_msg = (err == nullptr) ? std::string(sqlite3_errmsg(db)) : std::string(err);
        if(err != nullptr)
        {
            sqlite3_free(err);
        }

        spdlog::error("SQLite exec failed: {} | sql={}", err_msg, sql);
        return false;
    }

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
        std::string decoded_json;
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
    

    SQLiteHotStore::SQLiteHotStore(const std::filesystem::path& hot_db_path,
                                   const std::filesystem::path& archive_root,
                                   const std::int64_t outbox_retention_ms,
                                   const int default_chain_id)
        : _hot_db_path(hot_db_path)
        , _archive_root(archive_root)
        , _outbox_retention_ms(outbox_retention_ms)
        , _default_chain_id(default_chain_id)
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

        if (!exec(_write_db, "PRAGMA journal_mode=WAL;") || !exec(_write_db, "PRAGMA synchronous=NORMAL;") ||
            !exec(_write_db, "PRAGMA temp_store=MEMORY;") || !exec(_write_db, "PRAGMA foreign_keys=OFF;") ||
            !exec(_write_db, "PRAGMA wal_autocheckpoint=0;") || !exec(_write_db, "PRAGMA busy_timeout=10000;"))
        {
            throw std::runtime_error("Failed to configure events hot write DB pragmas");
        }

        if (!exec(_read_db, "PRAGMA journal_mode=WAL;") || !exec(_read_db, "PRAGMA synchronous=NORMAL;") ||
            !exec(_read_db, "PRAGMA temp_store=MEMORY;") || !exec(_read_db, "PRAGMA foreign_keys=OFF;") ||
            !exec(_read_db, "PRAGMA busy_timeout=10000;") || !exec(_read_db, "PRAGMA query_only=ON;"))
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

    std::optional<std::int64_t> SQLiteHotStore::loadNextFromBlock(const int chain_id)
    {
        Statement stmt(_write_db, "SELECT next_from_block FROM ingest_resume_state WHERE chain_id=?1;");
        sqlite3_bind_int(stmt.get(), 1, chain_id);

        const int rc = sqlite3_step(stmt.get());
        if (rc != SQLITE_ROW)
        {
            return std::nullopt;
        }
        return static_cast<std::int64_t>(sqlite3_column_int64(stmt.get(), 0));
    }

    std::optional<std::uint64_t> SQLiteHotStore::loadNextLocalSeq(const int chain_id)
    {
        Statement stmt(_write_db, "SELECT next_seq FROM local_ingest_resume_state WHERE chain_id=?1;");
        sqlite3_bind_int(stmt.get(), 1, chain_id);

        if (sqlite3_step(stmt.get()) != SQLITE_ROW)
        {
            return std::nullopt;
        }

        const auto value = static_cast<std::uint64_t>(sqlite3_column_int64(stmt.get(), 0));
        return value;
    }

    bool SQLiteHotStore::saveNextLocalSeq(const int chain_id, const std::uint64_t next_seq, const std::int64_t now_ms)
    {
        Statement stmt(_write_db,
                       "INSERT INTO local_ingest_resume_state(chain_id, next_seq, updated_at_ms) "
                       "VALUES(?1, ?2, ?3) "
                       "ON CONFLICT(chain_id) DO UPDATE SET "
                       "next_seq=excluded.next_seq, updated_at_ms=excluded.updated_at_ms;");
        sqlite3_bind_int(stmt.get(), 1, chain_id);
        sqlite3_bind_int64(stmt.get(), 2, static_cast<sqlite3_int64>(next_seq));
        sqlite3_bind_int64(stmt.get(), 3, static_cast<sqlite3_int64>(now_ms));
        return sqlite3_step(stmt.get()) == SQLITE_DONE;
    }

    std::vector<std::int64_t> SQLiteHotStore::loadReorgWindowBlocks(const int chain_id,
                                                                    const std::int64_t from_block,
                                                                    const std::int64_t to_block) const
    {
        std::vector<std::int64_t> block_numbers;
        std::lock_guard lock(_read_mutex);

        Statement stmt(_read_db,
                       "SELECT block_number "
                       "FROM reorg_window "
                       "WHERE chain_id=?1 AND block_number>=?2 AND block_number<=?3 "
                       "ORDER BY block_number ASC;");
        sqlite3_bind_int(stmt.get(), 1, chain_id);
        sqlite3_bind_int64(stmt.get(), 2, static_cast<sqlite3_int64>(from_block));
        sqlite3_bind_int64(stmt.get(), 3, static_cast<sqlite3_int64>(to_block));

        while (sqlite3_step(stmt.get()) == SQLITE_ROW)
        {
            block_numbers.push_back(static_cast<std::int64_t>(sqlite3_column_int64(stmt.get(), 0)));
        }
        return block_numbers;
    }

    bool SQLiteHotStore::ingestBatch(const int chain_id,
                                     const std::vector<DecodedEvent>& events,
                                     const std::vector<ChainBlockInfo>& block_infos,
                                     const std::int64_t next_from_block,
                                     const std::int64_t now_ms)
    {
        if (!exec(_write_db, "BEGIN IMMEDIATE TRANSACTION;"))
        {
            return false;
        }

        try
        {
            Statement block_window_stmt(
                _write_db,
                "INSERT INTO reorg_window(chain_id, block_number, block_hash, parent_hash, seen_at_ms) "
                "VALUES(?1, ?2, ?3, ?4, ?5) "
                "ON CONFLICT(chain_id, block_number) DO UPDATE SET "
                "block_hash=excluded.block_hash, parent_hash=excluded.parent_hash, seen_at_ms=excluded.seen_at_ms;");
            Statement queue_reorg_removed_jobs_stmt(
                _write_db,
                "INSERT OR IGNORE INTO projection_jobs(chain_id, block_hash, log_index, created_at_ms) "
                "SELECT chain_id, block_hash, log_index, ?1 "
                "FROM normalized_events_hot "
                "WHERE chain_id=?2 AND block_number=?3 AND block_hash<>?4 AND state!='removed';");
            Statement mark_reorg_removed_norm_stmt(
                _write_db,
                "UPDATE normalized_events_hot "
                "SET state='removed', updated_at_ms=?1, exported=0 "
                "WHERE chain_id=?2 AND block_number=?3 AND block_hash<>?4 AND state!='removed';");
            Statement mark_reorg_removed_raw_stmt(
                _write_db,
                "UPDATE raw_events_hot "
                "SET removed=1, state='removed', removed_at_ms=COALESCE(removed_at_ms, ?1), updated_at_ms=?2, exported=0 "
                "WHERE chain_id=?3 AND block_number=?4 AND block_hash<>?5 AND state!='removed';");

            for (const ChainBlockInfo& block_info : block_infos)
            {
                sqlite3_bind_int(block_window_stmt.get(), 1, block_info.chain_id);
                sqlite3_bind_int64(block_window_stmt.get(), 2, static_cast<sqlite3_int64>(block_info.block_number));
                sqlite3_bind_text(block_window_stmt.get(),
                                  3,
                                  block_info.block_hash.c_str(),
                                  static_cast<int>(block_info.block_hash.size()),
                                  SQLITE_TRANSIENT);
                sqlite3_bind_text(block_window_stmt.get(),
                                  4,
                                  block_info.parent_hash.c_str(),
                                  static_cast<int>(block_info.parent_hash.size()),
                                  SQLITE_TRANSIENT);
                sqlite3_bind_int64(block_window_stmt.get(), 5, static_cast<sqlite3_int64>(block_info.seen_at_ms));

                if (sqlite3_step(block_window_stmt.get()) != SQLITE_DONE)
                {
                    throw std::runtime_error(sqlite3_errmsg(_write_db));
                }
                sqlite3_reset(block_window_stmt.get());
                sqlite3_clear_bindings(block_window_stmt.get());

                sqlite3_bind_int64(queue_reorg_removed_jobs_stmt.get(), 1, static_cast<sqlite3_int64>(now_ms));
                sqlite3_bind_int(queue_reorg_removed_jobs_stmt.get(), 2, block_info.chain_id);
                sqlite3_bind_int64(
                    queue_reorg_removed_jobs_stmt.get(), 3, static_cast<sqlite3_int64>(block_info.block_number));
                sqlite3_bind_text(queue_reorg_removed_jobs_stmt.get(),
                                  4,
                                  block_info.block_hash.c_str(),
                                  static_cast<int>(block_info.block_hash.size()),
                                  SQLITE_TRANSIENT);
                if (sqlite3_step(queue_reorg_removed_jobs_stmt.get()) != SQLITE_DONE)
                {
                    throw std::runtime_error(sqlite3_errmsg(_write_db));
                }
                sqlite3_reset(queue_reorg_removed_jobs_stmt.get());
                sqlite3_clear_bindings(queue_reorg_removed_jobs_stmt.get());

                sqlite3_bind_int64(mark_reorg_removed_norm_stmt.get(), 1, static_cast<sqlite3_int64>(now_ms));
                sqlite3_bind_int(mark_reorg_removed_norm_stmt.get(), 2, block_info.chain_id);
                sqlite3_bind_int64(
                    mark_reorg_removed_norm_stmt.get(), 3, static_cast<sqlite3_int64>(block_info.block_number));
                sqlite3_bind_text(mark_reorg_removed_norm_stmt.get(),
                                  4,
                                  block_info.block_hash.c_str(),
                                  static_cast<int>(block_info.block_hash.size()),
                                  SQLITE_TRANSIENT);
                if (sqlite3_step(mark_reorg_removed_norm_stmt.get()) != SQLITE_DONE)
                {
                    throw std::runtime_error(sqlite3_errmsg(_write_db));
                }
                sqlite3_reset(mark_reorg_removed_norm_stmt.get());
                sqlite3_clear_bindings(mark_reorg_removed_norm_stmt.get());

                sqlite3_bind_int64(mark_reorg_removed_raw_stmt.get(), 1, static_cast<sqlite3_int64>(now_ms));
                sqlite3_bind_int64(mark_reorg_removed_raw_stmt.get(), 2, static_cast<sqlite3_int64>(now_ms));
                sqlite3_bind_int(mark_reorg_removed_raw_stmt.get(), 3, block_info.chain_id);
                sqlite3_bind_int64(
                    mark_reorg_removed_raw_stmt.get(), 4, static_cast<sqlite3_int64>(block_info.block_number));
                sqlite3_bind_text(mark_reorg_removed_raw_stmt.get(),
                                  5,
                                  block_info.block_hash.c_str(),
                                  static_cast<int>(block_info.block_hash.size()),
                                  SQLITE_TRANSIENT);
                if (sqlite3_step(mark_reorg_removed_raw_stmt.get()) != SQLITE_DONE)
                {
                    throw std::runtime_error(sqlite3_errmsg(_write_db));
                }
                sqlite3_reset(mark_reorg_removed_raw_stmt.get());
                sqlite3_clear_bindings(mark_reorg_removed_raw_stmt.get());
            }

            Statement raw_stmt(_write_db,
                               "INSERT INTO raw_events_hot("
                               "chain_id, block_hash, log_index, tx_hash, block_number, tx_index, block_time, "
                               "address, topic0, topic1, topic2, topic3, data_hex, removed, state, seen_at_ms, "
                               "updated_at_ms, removed_at_ms, exported"
                               ") VALUES("
                               "?1, ?2, ?3, ?4, ?5, ?6, ?7, "
                               "?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, 0"
                               ") "
                               "ON CONFLICT(chain_id, block_hash, log_index) DO UPDATE SET "
                               "tx_hash=excluded.tx_hash, block_number=excluded.block_number, tx_index=excluded.tx_index, "
                               "block_time=excluded.block_time, "
                               "address=excluded.address, topic0=excluded.topic0, topic1=excluded.topic1, "
                               "topic2=excluded.topic2, topic3=excluded.topic3, "
                               "data_hex=excluded.data_hex, removed=excluded.removed, state=excluded.state, "
                               "seen_at_ms=excluded.seen_at_ms, updated_at_ms=excluded.updated_at_ms, "
                               "removed_at_ms=excluded.removed_at_ms, exported=0;");

            Statement normalized_stmt(
                _write_db,
                "INSERT INTO normalized_events_hot("
                "chain_id, block_hash, log_index, tx_hash, block_number, tx_index, block_time, "
                "event_type, name, caller, owner, entity_address, args_count, format_hash, decoded_json, state, "
                "seen_at_ms, updated_at_ms, exported"
                ") VALUES("
                "?1, ?2, ?3, ?4, ?5, ?6, ?7, "
                "?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, 0"
                ") "
                "ON CONFLICT(chain_id, block_hash, log_index) DO UPDATE SET "
                "tx_hash=excluded.tx_hash, block_number=excluded.block_number, tx_index=excluded.tx_index, "
                "block_time=excluded.block_time, "
                "event_type=excluded.event_type, name=excluded.name, caller=excluded.caller, owner=excluded.owner, "
                "entity_address=excluded.entity_address, "
                "args_count=excluded.args_count, format_hash=excluded.format_hash, decoded_json=excluded.decoded_json, "
                "state=excluded.state, seen_at_ms=excluded.seen_at_ms, updated_at_ms=excluded.updated_at_ms, exported=0;");

            Statement job_stmt(
                _write_db,
                "INSERT INTO projection_jobs(chain_id, block_hash, log_index, created_at_ms) "
                "VALUES(?1, ?2, ?3, ?4) "
                "ON CONFLICT(chain_id, block_hash, log_index) DO UPDATE SET created_at_ms=excluded.created_at_ms;");

            for (const DecodedEvent& ev : events)
            {
                const std::optional<std::string> topic1 = ev.raw.topics[1];
                const std::optional<std::string> topic2 = ev.raw.topics[2];
                const std::optional<std::string> topic3 = ev.raw.topics[3];
                const std::optional<std::string> topic0 = ev.raw.topics[0];
                const std::optional<std::int64_t> removed_at =
                    ev.raw.removed ? std::optional<std::int64_t>(now_ms) : std::nullopt;

                sqlite3_bind_int(raw_stmt.get(), 1, chain_id);
                sqlite3_bind_text(raw_stmt.get(),
                                  2,
                                  ev.raw.block_hash.c_str(),
                                  static_cast<int>(ev.raw.block_hash.size()),
                                  SQLITE_TRANSIENT);
                sqlite3_bind_int64(raw_stmt.get(), 3, static_cast<sqlite3_int64>(ev.raw.log_index));
                sqlite3_bind_text(raw_stmt.get(), 4, ev.raw.tx_hash.c_str(), static_cast<int>(ev.raw.tx_hash.size()), SQLITE_TRANSIENT);
                sqlite3_bind_int64(raw_stmt.get(), 5, static_cast<sqlite3_int64>(ev.raw.block_number));
                sqlite3_bind_int64(raw_stmt.get(), 6, static_cast<sqlite3_int64>(ev.raw.tx_index));
                _bindOptionalInt64(raw_stmt.get(), 7, ev.raw.block_time);
                sqlite3_bind_text(raw_stmt.get(), 8, ev.raw.address.c_str(), static_cast<int>(ev.raw.address.size()), SQLITE_TRANSIENT);
                _bindOptionalText(raw_stmt.get(), 9, topic0);
                _bindOptionalText(raw_stmt.get(), 10, topic1);
                _bindOptionalText(raw_stmt.get(), 11, topic2);
                _bindOptionalText(raw_stmt.get(), 12, topic3);
                sqlite3_bind_text(raw_stmt.get(),
                                  13,
                                  ev.raw.data_hex.c_str(),
                                  static_cast<int>(ev.raw.data_hex.size()),
                                  SQLITE_TRANSIENT);
                sqlite3_bind_int(raw_stmt.get(), 14, ev.raw.removed ? 1 : 0);
                sqlite3_bind_text(raw_stmt.get(),
                                  15,
                                  toString(ev.state).c_str(),
                                  static_cast<int>(toString(ev.state).size()),
                                  SQLITE_TRANSIENT);
                sqlite3_bind_int64(raw_stmt.get(), 16, static_cast<sqlite3_int64>(ev.raw.seen_at_ms));
                sqlite3_bind_int64(raw_stmt.get(), 17, static_cast<sqlite3_int64>(now_ms));
                _bindOptionalInt64(raw_stmt.get(), 18, removed_at);

                if (sqlite3_step(raw_stmt.get()) != SQLITE_DONE)
                {
                    throw std::runtime_error(sqlite3_errmsg(_write_db));
                }

                sqlite3_reset(raw_stmt.get());
                sqlite3_clear_bindings(raw_stmt.get());

                sqlite3_bind_int(normalized_stmt.get(), 1, chain_id);
                sqlite3_bind_text(normalized_stmt.get(),
                                  2,
                                  ev.raw.block_hash.c_str(),
                                  static_cast<int>(ev.raw.block_hash.size()),
                                  SQLITE_TRANSIENT);
                sqlite3_bind_int64(normalized_stmt.get(), 3, static_cast<sqlite3_int64>(ev.raw.log_index));
                sqlite3_bind_text(normalized_stmt.get(),
                                  4,
                                  ev.raw.tx_hash.c_str(),
                                  static_cast<int>(ev.raw.tx_hash.size()),
                                  SQLITE_TRANSIENT);
                sqlite3_bind_int64(normalized_stmt.get(), 5, static_cast<sqlite3_int64>(ev.raw.block_number));
                sqlite3_bind_int64(normalized_stmt.get(), 6, static_cast<sqlite3_int64>(ev.raw.tx_index));
                _bindOptionalInt64(normalized_stmt.get(), 7, ev.raw.block_time);
                sqlite3_bind_text(normalized_stmt.get(),
                                  8,
                                  toString(ev.event_type).c_str(),
                                  static_cast<int>(toString(ev.event_type).size()),
                                  SQLITE_TRANSIENT);
                sqlite3_bind_text(normalized_stmt.get(), 9, ev.name.c_str(), static_cast<int>(ev.name.size()), SQLITE_TRANSIENT);
                sqlite3_bind_text(normalized_stmt.get(), 10, ev.caller.c_str(), static_cast<int>(ev.caller.size()), SQLITE_TRANSIENT);
                sqlite3_bind_text(normalized_stmt.get(), 11, ev.owner.c_str(), static_cast<int>(ev.owner.size()), SQLITE_TRANSIENT);
                sqlite3_bind_text(normalized_stmt.get(),
                                  12,
                                  ev.entity_address.c_str(),
                                  static_cast<int>(ev.entity_address.size()),
                                  SQLITE_TRANSIENT);
                
                if (ev.args_count.has_value())
                {
                    sqlite3_bind_int64(normalized_stmt.get(), 13, static_cast<sqlite3_int64>(*ev.args_count));
                }
                else
                {
                    sqlite3_bind_null(normalized_stmt.get(), 13);
                }

                _bindOptionalText(normalized_stmt.get(), 14, ev.format_hash);
                sqlite3_bind_text(normalized_stmt.get(),
                                  15,
                                  ev.decoded_json.c_str(),
                                  static_cast<int>(ev.decoded_json.size()),
                                  SQLITE_TRANSIENT);
                sqlite3_bind_text(normalized_stmt.get(),
                                  16,
                                  toString(ev.state).c_str(),
                                  static_cast<int>(toString(ev.state).size()),
                                  SQLITE_TRANSIENT);
                sqlite3_bind_int64(normalized_stmt.get(), 17, static_cast<sqlite3_int64>(ev.raw.seen_at_ms));
                sqlite3_bind_int64(normalized_stmt.get(), 18, static_cast<sqlite3_int64>(now_ms));

                if (sqlite3_step(normalized_stmt.get()) != SQLITE_DONE)
                {
                    throw std::runtime_error(sqlite3_errmsg(_write_db));
                }

                sqlite3_reset(normalized_stmt.get());
                sqlite3_clear_bindings(normalized_stmt.get());

                sqlite3_bind_int(job_stmt.get(), 1, chain_id);
                sqlite3_bind_text(job_stmt.get(),
                                  2,
                                  ev.raw.block_hash.c_str(),
                                  static_cast<int>(ev.raw.block_hash.size()),
                                  SQLITE_TRANSIENT);
                sqlite3_bind_int64(job_stmt.get(), 3, static_cast<sqlite3_int64>(ev.raw.log_index));
                sqlite3_bind_int64(job_stmt.get(), 4, static_cast<sqlite3_int64>(now_ms));

                if (sqlite3_step(job_stmt.get()) != SQLITE_DONE)
                {
                    throw std::runtime_error(sqlite3_errmsg(_write_db));
                }

                sqlite3_reset(job_stmt.get());
                sqlite3_clear_bindings(job_stmt.get());
            }

            Statement resume_stmt(_write_db,
                                  "INSERT INTO ingest_resume_state(chain_id, next_from_block, updated_at_ms) "
                                  "VALUES(?1, ?2, ?3) "
                                  "ON CONFLICT(chain_id) DO UPDATE SET "
                                  "next_from_block=excluded.next_from_block, updated_at_ms=excluded.updated_at_ms;");
            
            sqlite3_bind_int(resume_stmt.get(), 1, chain_id);
            sqlite3_bind_int64(resume_stmt.get(), 2, static_cast<sqlite3_int64>(next_from_block));
            sqlite3_bind_int64(resume_stmt.get(), 3, static_cast<sqlite3_int64>(now_ms));

            if (sqlite3_step(resume_stmt.get()) != SQLITE_DONE)
            {
                throw std::runtime_error(sqlite3_errmsg(_write_db));
            }

            if (!exec(_write_db, "COMMIT;"))
            {
                throw std::runtime_error("commit failed");
            }

            return true;
        }
        catch (const std::exception& e)
        {
            spdlog::error("Events ingestBatch failed: {}", e.what());
            (void)exec(_write_db, "ROLLBACK;");
            return false;
        }
    }

    bool SQLiteHotStore::applyFinality(const int chain_id,
                                       const FinalityHeights& heights,
                                       const std::int64_t now_ms,
                                       const std::size_t reorg_window_blocks)
    {
        if (!exec(_write_db, "BEGIN IMMEDIATE TRANSACTION;"))
        {
            return false;
        }

        try
        {
            Statement finality_stmt(
                _write_db,
                "INSERT INTO finality_state(chain_id, head_block, safe_block, finalized_block, updated_at_ms) "
                "VALUES(?1, ?2, ?3, ?4, ?5) "
                "ON CONFLICT(chain_id) DO UPDATE SET "
                "head_block=excluded.head_block, safe_block=excluded.safe_block, finalized_block=excluded.finalized_block, "
                "updated_at_ms=excluded.updated_at_ms;");
            
            sqlite3_bind_int(finality_stmt.get(), 1, chain_id);
            sqlite3_bind_int64(finality_stmt.get(), 2, static_cast<sqlite3_int64>(heights.head));
            sqlite3_bind_int64(finality_stmt.get(), 3, static_cast<sqlite3_int64>(heights.safe));
            sqlite3_bind_int64(finality_stmt.get(), 4, static_cast<sqlite3_int64>(heights.finalized));
            sqlite3_bind_int64(finality_stmt.get(), 5, static_cast<sqlite3_int64>(now_ms));

            if (sqlite3_step(finality_stmt.get()) != SQLITE_DONE)
            {
                throw std::runtime_error(sqlite3_errmsg(_write_db));
            }

            Statement queue_observed_to_safe(
                _write_db,
                "INSERT OR IGNORE INTO projection_jobs(chain_id, block_hash, log_index, created_at_ms) "
                "SELECT chain_id, block_hash, log_index, ?1 "
                "FROM normalized_events_hot "
                "WHERE chain_id=?2 AND state='observed' AND block_number <= ?3;");
            
            sqlite3_bind_int64(queue_observed_to_safe.get(), 1, static_cast<sqlite3_int64>(now_ms));
            sqlite3_bind_int(queue_observed_to_safe.get(), 2, chain_id);
            sqlite3_bind_int64(queue_observed_to_safe.get(), 3, static_cast<sqlite3_int64>(heights.safe));

            if (sqlite3_step(queue_observed_to_safe.get()) != SQLITE_DONE)
            {
                throw std::runtime_error(sqlite3_errmsg(_write_db));
            }

            Statement update_observed_to_safe_norm(
                _write_db,
                "UPDATE normalized_events_hot SET state='safe', updated_at_ms=?1, exported=0 "
                "WHERE chain_id=?2 AND state='observed' AND block_number <= ?3;");
            
            sqlite3_bind_int64(update_observed_to_safe_norm.get(), 1, static_cast<sqlite3_int64>(now_ms));
            sqlite3_bind_int(update_observed_to_safe_norm.get(), 2, chain_id);
            sqlite3_bind_int64(update_observed_to_safe_norm.get(), 3, static_cast<sqlite3_int64>(heights.safe));

            if (sqlite3_step(update_observed_to_safe_norm.get()) != SQLITE_DONE)
            {
                throw std::runtime_error(sqlite3_errmsg(_write_db));
            }

            Statement update_observed_to_safe_raw(_write_db,
                                                  "UPDATE raw_events_hot SET state='safe', updated_at_ms=?1, exported=0 "
                                                  "WHERE chain_id=?2 AND state='observed' AND block_number <= ?3;");
            
            sqlite3_bind_int64(update_observed_to_safe_raw.get(), 1, static_cast<sqlite3_int64>(now_ms));
            sqlite3_bind_int(update_observed_to_safe_raw.get(), 2, chain_id);
            sqlite3_bind_int64(update_observed_to_safe_raw.get(), 3, static_cast<sqlite3_int64>(heights.safe));

            if (sqlite3_step(update_observed_to_safe_raw.get()) != SQLITE_DONE)
            {
                throw std::runtime_error(sqlite3_errmsg(_write_db));
            }

            Statement queue_safe_to_finalized(
                _write_db,
                "INSERT OR IGNORE INTO projection_jobs(chain_id, block_hash, log_index, created_at_ms) "
                "SELECT chain_id, block_hash, log_index, ?1 "
                "FROM normalized_events_hot "
                "WHERE chain_id=?2 AND state='safe' AND block_number <= ?3;");
            
            sqlite3_bind_int64(queue_safe_to_finalized.get(), 1, static_cast<sqlite3_int64>(now_ms));
            sqlite3_bind_int(queue_safe_to_finalized.get(), 2, chain_id);
            sqlite3_bind_int64(queue_safe_to_finalized.get(), 3, static_cast<sqlite3_int64>(heights.finalized));

            if (sqlite3_step(queue_safe_to_finalized.get()) != SQLITE_DONE)
            {
                throw std::runtime_error(sqlite3_errmsg(_write_db));
            }

            Statement update_safe_to_finalized_norm(
                _write_db,
                "UPDATE normalized_events_hot SET state='finalized', updated_at_ms=?1, exported=0 "
                "WHERE chain_id=?2 AND state='safe' AND block_number <= ?3;");
            
            sqlite3_bind_int64(update_safe_to_finalized_norm.get(), 1, static_cast<sqlite3_int64>(now_ms));
            sqlite3_bind_int(update_safe_to_finalized_norm.get(), 2, chain_id);
            sqlite3_bind_int64(update_safe_to_finalized_norm.get(), 3, static_cast<sqlite3_int64>(heights.finalized));

            if (sqlite3_step(update_safe_to_finalized_norm.get()) != SQLITE_DONE)
            {
                throw std::runtime_error(sqlite3_errmsg(_write_db));
            }

            Statement update_safe_to_finalized_raw(
                _write_db,
                "UPDATE raw_events_hot SET state='finalized', updated_at_ms=?1, exported=0 "
                "WHERE chain_id=?2 AND state='safe' AND block_number <= ?3;");
            
            sqlite3_bind_int64(update_safe_to_finalized_raw.get(), 1, static_cast<sqlite3_int64>(now_ms));
            sqlite3_bind_int(update_safe_to_finalized_raw.get(), 2, chain_id);
            sqlite3_bind_int64(update_safe_to_finalized_raw.get(), 3, static_cast<sqlite3_int64>(heights.finalized));
            
            if (sqlite3_step(update_safe_to_finalized_raw.get()) != SQLITE_DONE)
            {
                throw std::runtime_error(sqlite3_errmsg(_write_db));
            }

            const std::int64_t floor = (heights.head > static_cast<std::int64_t>(reorg_window_blocks))
                                           ? (heights.head - static_cast<std::int64_t>(reorg_window_blocks))
                                           : 0;
            
            Statement prune_reorg_stmt(_write_db, "DELETE FROM reorg_window WHERE chain_id=?1 AND block_number < ?2;");

            sqlite3_bind_int(prune_reorg_stmt.get(), 1, chain_id);
            sqlite3_bind_int64(prune_reorg_stmt.get(), 2, static_cast<sqlite3_int64>(floor));

            if (sqlite3_step(prune_reorg_stmt.get()) != SQLITE_DONE)
            {
                throw std::runtime_error(sqlite3_errmsg(_write_db));
            }

            if (!exec(_write_db, "COMMIT;"))
            {
                throw std::runtime_error("commit failed");
            }

            return true;
        }
        catch (const std::exception& e)
        {
            spdlog::error("Events applyFinality failed: {}", e.what());
            (void)exec(_write_db, "ROLLBACK;");
            return false;
        }
    }

    std::size_t SQLiteHotStore::projectBatch(const std::size_t limit, const std::int64_t now_ms)
    {
        if (!exec(_write_db, "BEGIN IMMEDIATE TRANSACTION;"))
        {
            return 0;
        }

        std::size_t projected_count = 0;
        try
        {
            Statement select_jobs(
                _write_db,
                "SELECT "
                "n.chain_id, n.block_hash, n.log_index, n.tx_hash, n.block_number, n.tx_index, n.block_time, "
                "n.event_type, n.decoded_json, n.state, n.seen_at_ms "
                "FROM projection_jobs j "
                "JOIN normalized_events_hot n "
                "ON n.chain_id=j.chain_id AND n.block_hash=j.block_hash AND n.log_index=j.log_index "
                "ORDER BY n.block_number ASC, n.tx_index ASC, n.log_index ASC "
                "LIMIT ?1;");
            
            sqlite3_bind_int64(select_jobs.get(), 1, static_cast<sqlite3_int64>(limit));

            Statement existing_feed_stmt(_write_db, "SELECT created_at_ms FROM feed_items_hot WHERE feed_id=?1;");

            Statement upsert_feed_stmt(
                _write_db,
                "INSERT INTO feed_items_hot("
                "feed_id, chain_id, tx_hash, log_index, block_number, tx_index, block_time, event_type, status, visible, "
                "history_cursor, payload_json, created_at_ms, updated_at_ms, "
                "projector_version, exported"
                ") VALUES("
                "?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, "
                "?11, ?12, ?13, ?14, ?15, 0"
                ") ON CONFLICT(feed_id) DO UPDATE SET "
                "chain_id=excluded.chain_id, tx_hash=excluded.tx_hash, log_index=excluded.log_index, "
                "block_number=excluded.block_number, tx_index=excluded.tx_index, "
                "block_time=excluded.block_time, event_type=excluded.event_type, status=excluded.status, "
                "visible=excluded.visible, "
                "history_cursor=excluded.history_cursor, "
                "payload_json=excluded.payload_json, updated_at_ms=excluded.updated_at_ms, "
                "projector_version=excluded.projector_version, exported=0;");

            Statement insert_outbox_stmt(
                _write_db,
                "INSERT INTO global_outbox(feed_id, op, status, history_cursor, payload_json, created_at_ms) "
                "VALUES(?1, ?2, ?3, ?4, ?5, ?6);");

            Statement delete_job_stmt(_write_db,
                                      "DELETE FROM projection_jobs WHERE chain_id=?1 AND block_hash=?2 AND log_index=?3;");

            while (sqlite3_step(select_jobs.get()) == SQLITE_ROW)
            {
                const int chain_id = sqlite3_column_int(select_jobs.get(), 0);
                const std::string block_hash = reinterpret_cast<const char*>(sqlite3_column_text(select_jobs.get(), 1));
                const std::int64_t log_index = static_cast<std::int64_t>(sqlite3_column_int64(select_jobs.get(), 2));
                const std::string tx_hash = reinterpret_cast<const char*>(sqlite3_column_text(select_jobs.get(), 3));
                const std::int64_t block_number = static_cast<std::int64_t>(sqlite3_column_int64(select_jobs.get(), 4));
                const std::int64_t tx_index = static_cast<std::int64_t>(sqlite3_column_int64(select_jobs.get(), 5));
                const std::optional<std::int64_t> block_time = _columnInt64Optional(select_jobs.get(), 6);
                const std::string event_type = reinterpret_cast<const char*>(sqlite3_column_text(select_jobs.get(), 7));
                const std::string decoded_json = reinterpret_cast<const char*>(sqlite3_column_text(select_jobs.get(), 8));
                const std::string state = reinterpret_cast<const char*>(sqlite3_column_text(select_jobs.get(), 9));
                const std::int64_t seen_at_ms = static_cast<std::int64_t>(sqlite3_column_int64(select_jobs.get(), 10));

                const std::string feed_id = std::format("{}:{}:{}", chain_id, tx_hash, log_index);
                const std::string history_cursor = _makeHistoryCursor(chain_id, block_number, tx_index, log_index, feed_id);

                const bool visible = (state != REMOVED_STATE);

                bool existed = false;
                std::int64_t created_at_ms = seen_at_ms;
                sqlite3_bind_text(existing_feed_stmt.get(), 1, feed_id.c_str(), static_cast<int>(feed_id.size()), SQLITE_TRANSIENT);

                if (sqlite3_step(existing_feed_stmt.get()) == SQLITE_ROW)
                {
                    existed = true;
                    created_at_ms = static_cast<std::int64_t>(sqlite3_column_int64(existing_feed_stmt.get(), 0));
                }

                sqlite3_reset(existing_feed_stmt.get());
                sqlite3_clear_bindings(existing_feed_stmt.get());

                const std::string op = (!visible) ? "remove" : (existed ? "update" : "insert");

                json decoded_payload = json::parse(decoded_json, nullptr, false);
                if (decoded_payload.is_discarded())
                {
                    decoded_payload = json::object();
                }

                const std::string payload_json = decoded_payload.dump(-1, ' ', false, json::error_handler_t::replace);

                sqlite3_bind_text(upsert_feed_stmt.get(), 1, feed_id.c_str(), static_cast<int>(feed_id.size()), SQLITE_TRANSIENT);
                sqlite3_bind_int(upsert_feed_stmt.get(), 2, chain_id);
                sqlite3_bind_text(upsert_feed_stmt.get(), 3, tx_hash.c_str(), static_cast<int>(tx_hash.size()), SQLITE_TRANSIENT);
                sqlite3_bind_int64(upsert_feed_stmt.get(), 4, static_cast<sqlite3_int64>(log_index));
                sqlite3_bind_int64(upsert_feed_stmt.get(), 5, static_cast<sqlite3_int64>(block_number));
                sqlite3_bind_int64(upsert_feed_stmt.get(), 6, static_cast<sqlite3_int64>(tx_index));
                _bindOptionalInt64(upsert_feed_stmt.get(), 7, block_time);
                sqlite3_bind_text(upsert_feed_stmt.get(), 8, event_type.c_str(), static_cast<int>(event_type.size()), SQLITE_TRANSIENT);
                sqlite3_bind_text(upsert_feed_stmt.get(), 9, state.c_str(), static_cast<int>(state.size()), SQLITE_TRANSIENT);
                sqlite3_bind_int(upsert_feed_stmt.get(), 10, visible ? 1 : 0);
                sqlite3_bind_text(upsert_feed_stmt.get(), 11, history_cursor.c_str(), static_cast<int>(history_cursor.size()), SQLITE_TRANSIENT);
                sqlite3_bind_text(upsert_feed_stmt.get(), 12, payload_json.c_str(), static_cast<int>(payload_json.size()), SQLITE_TRANSIENT);
                sqlite3_bind_int64(upsert_feed_stmt.get(), 13, static_cast<sqlite3_int64>(created_at_ms));
                sqlite3_bind_int64(upsert_feed_stmt.get(), 14, static_cast<sqlite3_int64>(now_ms));
                sqlite3_bind_int(upsert_feed_stmt.get(), 15, 1);

                if (sqlite3_step(upsert_feed_stmt.get()) != SQLITE_DONE)
                {
                    throw std::runtime_error(sqlite3_errmsg(_write_db));
                }

                sqlite3_reset(upsert_feed_stmt.get());
                sqlite3_clear_bindings(upsert_feed_stmt.get());

                sqlite3_bind_text(insert_outbox_stmt.get(), 1, feed_id.c_str(), static_cast<int>(feed_id.size()), SQLITE_TRANSIENT);
                sqlite3_bind_text(insert_outbox_stmt.get(), 2, op.c_str(), static_cast<int>(op.size()), SQLITE_TRANSIENT);
                sqlite3_bind_text(insert_outbox_stmt.get(), 3, state.c_str(), static_cast<int>(state.size()), SQLITE_TRANSIENT);
                sqlite3_bind_text(insert_outbox_stmt.get(), 4, history_cursor.c_str(), static_cast<int>(history_cursor.size()), SQLITE_TRANSIENT);
                sqlite3_bind_text(insert_outbox_stmt.get(), 5, payload_json.c_str(), static_cast<int>(payload_json.size()), SQLITE_TRANSIENT);
                sqlite3_bind_int64(insert_outbox_stmt.get(), 6, static_cast<sqlite3_int64>(now_ms));

                if (sqlite3_step(insert_outbox_stmt.get()) != SQLITE_DONE)
                {
                    throw std::runtime_error(sqlite3_errmsg(_write_db));
                }

                sqlite3_reset(insert_outbox_stmt.get());
                sqlite3_clear_bindings(insert_outbox_stmt.get());

                sqlite3_bind_int(delete_job_stmt.get(), 1, chain_id);
                sqlite3_bind_text(delete_job_stmt.get(), 2, block_hash.c_str(), static_cast<int>(block_hash.size()), SQLITE_TRANSIENT);
                sqlite3_bind_int64(delete_job_stmt.get(), 3, static_cast<sqlite3_int64>(log_index));
                if (sqlite3_step(delete_job_stmt.get()) != SQLITE_DONE)
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
                Statement prune_outbox_stmt(_write_db, "DELETE FROM global_outbox WHERE created_at_ms < ?1;");
                sqlite3_bind_int64(prune_outbox_stmt.get(), 1, static_cast<sqlite3_int64>(cutoff_ms));
                if (sqlite3_step(prune_outbox_stmt.get()) != SQLITE_DONE)
                {
                    throw std::runtime_error(sqlite3_errmsg(_write_db));
                }
            }

            if (!exec(_write_db, "COMMIT;"))
            {
                throw std::runtime_error("commit failed");
            }

            return projected_count;
        }
        catch (const std::exception& e)
        {
            spdlog::error("Events projectBatch failed: {}", e.what());
            (void)exec(_write_db, "ROLLBACK;");
            return 0;
        }
    }

    bool SQLiteHotStore::runArchiveCycle(const int chain_id, const std::size_t hot_window_days, const std::int64_t now_ms)
    {
        try
        {
            std::vector<std::string> months;
            {
                Statement months_stmt(_write_db,
                                      "SELECT DISTINCT strftime('%Y-%m', block_time, 'unixepoch') "
                                      "FROM normalized_events_hot "
                                      "WHERE chain_id=?1 AND state='finalized' AND exported=0 AND block_time IS NOT NULL "
                                      "ORDER BY 1 ASC "
                                      "LIMIT 8;");
                sqlite3_bind_int(months_stmt.get(), 1, chain_id);

                while (sqlite3_step(months_stmt.get()) == SQLITE_ROW)
                {
                    const unsigned char* month = sqlite3_column_text(months_stmt.get(), 0);
                    if (month != nullptr)
                    {
                        months.emplace_back(reinterpret_cast<const char*>(month));
                    }
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
                static_cast<std::int64_t>(now_ms / 1000) - static_cast<std::int64_t>(hot_window_days) * 24 * 60 * 60;

            if (!exec(_write_db, "BEGIN IMMEDIATE TRANSACTION;"))
            {
                return false;
            }

            Statement prune_raw(
                _write_db,
                "DELETE FROM raw_events_hot "
                "WHERE chain_id=?1 AND state='finalized' AND exported=1 AND block_time IS NOT NULL AND block_time < ?2;");
            sqlite3_bind_int(prune_raw.get(), 1, chain_id);
            sqlite3_bind_int64(prune_raw.get(), 2, static_cast<sqlite3_int64>(cutoff_seconds));
            if (sqlite3_step(prune_raw.get()) != SQLITE_DONE)
            {
                throw std::runtime_error(sqlite3_errmsg(_write_db));
            }

            Statement prune_norm(
                _write_db,
                "DELETE FROM normalized_events_hot "
                "WHERE chain_id=?1 AND state='finalized' AND exported=1 AND block_time IS NOT NULL AND block_time < ?2;");
            sqlite3_bind_int(prune_norm.get(), 1, chain_id);
            sqlite3_bind_int64(prune_norm.get(), 2, static_cast<sqlite3_int64>(cutoff_seconds));
            if (sqlite3_step(prune_norm.get()) != SQLITE_DONE)
            {
                throw std::runtime_error(sqlite3_errmsg(_write_db));
            }

            Statement prune_feed(
                _write_db,
                "DELETE FROM feed_items_hot "
                "WHERE chain_id=?1 AND status='finalized' AND exported=1 AND block_time IS NOT NULL AND block_time < ?2;");
            sqlite3_bind_int(prune_feed.get(), 1, chain_id);
            sqlite3_bind_int64(prune_feed.get(), 2, static_cast<sqlite3_int64>(cutoff_seconds));
            if (sqlite3_step(prune_feed.get()) != SQLITE_DONE)
            {
                throw std::runtime_error(sqlite3_errmsg(_write_db));
            }

            if (!exec(_write_db, "COMMIT;"))
            {
                throw std::runtime_error("commit failed");
            }
            return true;
        }
        catch (const std::exception& e)
        {
            spdlog::error("Events runArchiveCycle failed: {}", e.what());
            (void)exec(_write_db, "ROLLBACK;");
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
            }
        }

        std::lock_guard lock(_read_mutex);
        std::vector<FeedItem> all_items;
        all_items.reserve(limit + 16);
        std::set<std::string> seen_feed_ids;

        _appendFeedRowsFromDatabase(_read_db, "feed_items_hot", query, before_key, limit + 1, all_items, seen_feed_ids);

        if (all_items.size() <= limit)
        {
            const std::vector<std::filesystem::path> archive_paths = _candidateArchivePaths(before_key);
            for (const auto& archive_path : archive_paths)
            {
                sqlite3* archive_db = nullptr;
                const int open_rc = sqlite3_open_v2(
                    archive_path.string().c_str(), &archive_db, SQLITE_OPEN_READONLY | SQLITE_OPEN_FULLMUTEX, nullptr);
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
                    archive_db, "feed_items_archive", query, before_key, limit + 1, all_items, seen_feed_ids);
                sqlite3_close(archive_db);
                archive_db = nullptr;

                if (all_items.size() > limit)
                {
                    break;
                }
            }
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

        std::lock_guard lock(_read_mutex);
        StreamPage page {};

        {
            Statement min_seq_stmt(_read_db, "SELECT COALESCE(MIN(stream_seq), 0) FROM global_outbox;");
            if (sqlite3_step(min_seq_stmt.get()) == SQLITE_ROW)
            {
                page.min_available_seq = static_cast<std::int64_t>(sqlite3_column_int64(min_seq_stmt.get(), 0));
            }
        }

        Statement stream_stmt(_read_db,
                              "SELECT stream_seq, op, status, feed_id, history_cursor, payload_json, created_at_ms "
                              "FROM global_outbox "
                              "WHERE stream_seq > ?1 "
                              "ORDER BY stream_seq ASC "
                              "LIMIT ?2;");
        sqlite3_bind_int64(stream_stmt.get(), 1, static_cast<sqlite3_int64>(query.since_seq));
        sqlite3_bind_int64(stream_stmt.get(), 2, static_cast<sqlite3_int64>(limit + 1));

        while (sqlite3_step(stream_stmt.get()) == SQLITE_ROW)
        {
            StreamDelta delta {};
            delta.stream_seq = static_cast<std::int64_t>(sqlite3_column_int64(stream_stmt.get(), 0));
            delta.op = reinterpret_cast<const char*>(sqlite3_column_text(stream_stmt.get(), 1));
            delta.status = reinterpret_cast<const char*>(sqlite3_column_text(stream_stmt.get(), 2));
            delta.feed_id = reinterpret_cast<const char*>(sqlite3_column_text(stream_stmt.get(), 3));
            delta.history_cursor = reinterpret_cast<const char*>(sqlite3_column_text(stream_stmt.get(), 4));
            const std::string payload_json = reinterpret_cast<const char*>(sqlite3_column_text(stream_stmt.get(), 5));
            delta.created_at_ms = static_cast<std::int64_t>(sqlite3_column_int64(stream_stmt.get(), 6));
            delta.payload = json::parse(payload_json, nullptr, false);
            if (delta.payload.is_discarded())
            {
                delta.payload = json::object();
            }
            page.deltas.push_back(std::move(delta));
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
        std::lock_guard lock(_read_mutex);
        Statement stmt(_read_db, "SELECT COALESCE(MIN(stream_seq), 0) FROM global_outbox;");
        if (sqlite3_step(stmt.get()) == SQLITE_ROW)
        {
            return static_cast<std::int64_t>(sqlite3_column_int64(stmt.get(), 0));
        }
        return 0;
    }

    bool SQLiteHotStore::_initializeHotSchema()
    {
        return exec(_write_db,
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
                    "exported INTEGER NOT NULL DEFAULT 0,"
                    "PRIMARY KEY(chain_id, block_hash, log_index)"
                    ");") &&
               exec(_write_db,
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
                    "decoded_json TEXT NOT NULL,"
                    "state TEXT NOT NULL,"
                    "seen_at_ms INTEGER NOT NULL,"
                    "updated_at_ms INTEGER NOT NULL,"
                    "exported INTEGER NOT NULL DEFAULT 0,"
                    "PRIMARY KEY(chain_id, block_hash, log_index)"
                    ");") &&
               exec(_write_db,
                    "CREATE TABLE IF NOT EXISTS projection_jobs ("
                    "chain_id INTEGER NOT NULL,"
                    "block_hash TEXT NOT NULL,"
                    "log_index INTEGER NOT NULL,"
                    "created_at_ms INTEGER NOT NULL,"
                    "PRIMARY KEY(chain_id, block_hash, log_index)"
                    ");") &&
               exec(_write_db,
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
                    "exported INTEGER NOT NULL DEFAULT 0"
                    ");") &&
               exec(_write_db,
                    "CREATE TABLE IF NOT EXISTS global_outbox ("
                    "stream_seq INTEGER PRIMARY KEY,"
                    "feed_id TEXT NOT NULL,"
                    "op TEXT NOT NULL,"
                    "status TEXT NOT NULL,"
                    "history_cursor TEXT NOT NULL,"
                    "payload_json TEXT NOT NULL,"
                    "created_at_ms INTEGER NOT NULL"
                    ");") &&
               exec(_write_db,
                    "CREATE TABLE IF NOT EXISTS ingest_resume_state ("
                    "chain_id INTEGER PRIMARY KEY,"
                    "next_from_block INTEGER NOT NULL,"
                    "updated_at_ms INTEGER NOT NULL"
                    ");") &&
               exec(_write_db,
                    "CREATE TABLE IF NOT EXISTS local_ingest_resume_state ("
                    "chain_id INTEGER PRIMARY KEY,"
                    "next_seq INTEGER NOT NULL,"
                    "updated_at_ms INTEGER NOT NULL"
                    ");") &&
               exec(_write_db,
                    "CREATE TABLE IF NOT EXISTS finality_state ("
                    "chain_id INTEGER PRIMARY KEY,"
                    "head_block INTEGER NOT NULL,"
                    "safe_block INTEGER NOT NULL,"
                    "finalized_block INTEGER NOT NULL,"
                    "updated_at_ms INTEGER NOT NULL"
                    ");") &&
               exec(_write_db,
                    "CREATE TABLE IF NOT EXISTS reorg_window ("
                    "chain_id INTEGER NOT NULL,"
                    "block_number INTEGER NOT NULL,"
                    "block_hash TEXT NOT NULL,"
                    "parent_hash TEXT,"
                    "seen_at_ms INTEGER NOT NULL,"
                    "PRIMARY KEY(chain_id, block_number)"
                    ");") &&
               exec(_write_db,
                    "CREATE TABLE IF NOT EXISTS archive_jobs ("
                    "job_id INTEGER PRIMARY KEY,"
                    "chain_id INTEGER NOT NULL,"
                    "archive_month TEXT NOT NULL,"
                    "state TEXT NOT NULL,"
                    "details_json TEXT NOT NULL,"
                    "created_at_ms INTEGER NOT NULL,"
                    "updated_at_ms INTEGER NOT NULL"
                    ");") &&
               exec(_write_db,
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
               exec(_write_db,
                    "CREATE INDEX IF NOT EXISTS idx_raw_chain_tx_log ON raw_events_hot(chain_id, tx_hash, log_index);") &&
               exec(_write_db, "CREATE INDEX IF NOT EXISTS idx_raw_block ON raw_events_hot(chain_id, block_number);") &&
               exec(_write_db,
                    "CREATE INDEX IF NOT EXISTS idx_norm_chain_tx_log ON normalized_events_hot(chain_id, tx_hash, "
                    "log_index);") &&
               exec(_write_db,
                    "CREATE INDEX IF NOT EXISTS idx_norm_chain_block ON normalized_events_hot(chain_id, block_number);") &&
               exec(_write_db,
                    "CREATE INDEX IF NOT EXISTS idx_feed_visible_order ON feed_items_hot(block_number DESC, tx_index DESC, "
                    "log_index DESC, feed_id DESC) WHERE visible=1;") &&
               exec(_write_db,
                    "CREATE INDEX IF NOT EXISTS idx_feed_event_type_order ON feed_items_hot(event_type, block_number DESC, "
                    "tx_index DESC, log_index DESC, feed_id DESC);") &&
               exec(_write_db, "CREATE INDEX IF NOT EXISTS idx_outbox_created_at ON global_outbox(created_at_ms);") &&
               exec(
                   _write_db,
                   "CREATE INDEX IF NOT EXISTS idx_catalog_state_block ON shard_catalog(chain_id, state, max_block DESC);");
    }

    bool SQLiteHotStore::_initializeArchiveSchema(sqlite3* archive_db) const
    {
        return exec(archive_db,
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
                    "decoded_json TEXT NOT NULL,"
                    "state TEXT NOT NULL,"
                    "seen_at_ms INTEGER NOT NULL,"
                    "updated_at_ms INTEGER NOT NULL,"
                    "PRIMARY KEY(chain_id, block_hash, log_index)"
                    ");") &&
               exec(archive_db,
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
               exec(archive_db,
                    "CREATE INDEX IF NOT EXISTS idx_archive_feed_visible_order ON feed_items_archive(block_number DESC, "
                    "tx_index DESC, log_index DESC, feed_id DESC) WHERE visible=1;") &&
               exec(archive_db,
                    "CREATE INDEX IF NOT EXISTS idx_archive_feed_type_order ON feed_items_archive(event_type, block_number "
                    "DESC, tx_index DESC, log_index DESC, feed_id DESC);");
    }

    bool SQLiteHotStore::_exportMonth(const int chain_id, const std::string& month_token, const std::int64_t now_ms)
    {
        if (month_token.size() != 7 || month_token[4] != '-')
        {
            return false;
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
            return false;
        }

        EventShardId shard_id {.chain_id = chain_id, .year = year, .month = month};
        const std::filesystem::path archive_path = _shard_router->filenameFor(shard_id);
        std::error_code dir_ec;
        std::filesystem::create_directories(archive_path.parent_path(), dir_ec);
        if (dir_ec)
        {
            spdlog::error(
                "Failed to create archive shard directory '{}': {}", archive_path.parent_path().string(), dir_ec.message());
            return false;
        }

        std::vector<NormalizedArchiveRow> normalized_rows;
        {
            Statement select_norm(_write_db,
                                  "SELECT "
                                  "chain_id, block_hash, log_index, tx_hash, block_number, tx_index, block_time, "
                                  "event_type, name, caller, owner, entity_address, args_count, format_hash, decoded_json, "
                                  "state, seen_at_ms, updated_at_ms "
                                  "FROM normalized_events_hot "
                                  "WHERE chain_id=?1 AND state='finalized' AND exported=0 "
                                  "AND block_time IS NOT NULL "
                                  "AND strftime('%Y-%m', block_time, 'unixepoch')=?2;");
            sqlite3_bind_int(select_norm.get(), 1, chain_id);
            sqlite3_bind_text(
                select_norm.get(), 2, month_token.c_str(), static_cast<int>(month_token.size()), SQLITE_TRANSIENT);

            while (sqlite3_step(select_norm.get()) == SQLITE_ROW)
            {
                NormalizedArchiveRow row {};
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
                row.decoded_json = reinterpret_cast<const char*>(sqlite3_column_text(select_norm.get(), 14));
                row.state = reinterpret_cast<const char*>(sqlite3_column_text(select_norm.get(), 15));
                row.seen_at_ms = static_cast<std::int64_t>(sqlite3_column_int64(select_norm.get(), 16));
                row.updated_at_ms = static_cast<std::int64_t>(sqlite3_column_int64(select_norm.get(), 17));
                normalized_rows.push_back(std::move(row));
            }
        }

        std::vector<FeedArchiveRow> feed_rows;
        {
            Statement select_feed(_write_db,
                                  "SELECT "
                                  "feed_id, chain_id, tx_hash, log_index, block_number, tx_index, block_time, "
                                  "event_type, status, visible, history_cursor, payload_json, "
                                  "created_at_ms, updated_at_ms, projector_version "
                                  "FROM feed_items_hot "
                                  "WHERE chain_id=?1 AND status='finalized' AND exported=0 AND block_time IS NOT NULL "
                                  "AND strftime('%Y-%m', block_time, 'unixepoch')=?2;");
            sqlite3_bind_int(select_feed.get(), 1, chain_id);
            sqlite3_bind_text(
                select_feed.get(), 2, month_token.c_str(), static_cast<int>(month_token.size()), SQLITE_TRANSIENT);

            while (sqlite3_step(select_feed.get()) == SQLITE_ROW)
            {
                FeedArchiveRow row {};
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
                feed_rows.push_back(std::move(row));
            }
        }

        if (normalized_rows.empty() && feed_rows.empty())
        {
            return true;
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
        if (!exec(archive_db, "PRAGMA journal_mode=WAL;") || !exec(archive_db, "PRAGMA synchronous=NORMAL;") ||
            !exec(archive_db, "PRAGMA temp_store=MEMORY;") || !exec(archive_db, "PRAGMA foreign_keys=OFF;"))
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

        if (!exec(archive_db, "BEGIN IMMEDIATE TRANSACTION;"))
        {
            sqlite3_close(archive_db);
            archive_db = nullptr;
            return false;
        }

        try
        {
            Statement insert_norm(archive_db,
                                  "INSERT INTO normalized_events_archive("
                                  "chain_id, block_hash, log_index, tx_hash, block_number, tx_index, block_time, "
                                  "event_type, name, caller, owner, entity_address, args_count, format_hash, decoded_json, "
                                  "state, seen_at_ms, updated_at_ms"
                                  ") VALUES("
                                  "?1, ?2, ?3, ?4, ?5, ?6, ?7, "
                                  "?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18"
                                  ") ON CONFLICT(chain_id, block_hash, log_index) DO UPDATE SET "
                                  "tx_hash=excluded.tx_hash, block_number=excluded.block_number, "
                                  "tx_index=excluded.tx_index, block_time=excluded.block_time, "
                                  "event_type=excluded.event_type, name=excluded.name, caller=excluded.caller, "
                                  "owner=excluded.owner, entity_address=excluded.entity_address, "
                                  "args_count=excluded.args_count, format_hash=excluded.format_hash, "
                                  "decoded_json=excluded.decoded_json, state=excluded.state, "
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
                sqlite3_bind_text(
                    insert_norm.get(), 4, row.tx_hash.c_str(), static_cast<int>(row.tx_hash.size()), SQLITE_TRANSIENT);
                sqlite3_bind_int64(insert_norm.get(), 5, static_cast<sqlite3_int64>(row.block_number));
                sqlite3_bind_int64(insert_norm.get(), 6, static_cast<sqlite3_int64>(row.tx_index));
                _bindOptionalInt64(insert_norm.get(), 7, row.block_time);
                sqlite3_bind_text(insert_norm.get(),
                                  8,
                                  row.event_type.c_str(),
                                  static_cast<int>(row.event_type.size()),
                                  SQLITE_TRANSIENT);
                sqlite3_bind_text(
                    insert_norm.get(), 9, row.name.c_str(), static_cast<int>(row.name.size()), SQLITE_TRANSIENT);
                sqlite3_bind_text(
                    insert_norm.get(), 10, row.caller.c_str(), static_cast<int>(row.caller.size()), SQLITE_TRANSIENT);
                sqlite3_bind_text(
                    insert_norm.get(), 11, row.owner.c_str(), static_cast<int>(row.owner.size()), SQLITE_TRANSIENT);
                sqlite3_bind_text(insert_norm.get(),
                                  12,
                                  row.entity_address.c_str(),
                                  static_cast<int>(row.entity_address.size()),
                                  SQLITE_TRANSIENT);
                _bindOptionalInt64(insert_norm.get(), 13, row.args_count);
                _bindOptionalText(insert_norm.get(), 14, row.format_hash);
                sqlite3_bind_text(insert_norm.get(),
                                  15,
                                  row.decoded_json.c_str(),
                                  static_cast<int>(row.decoded_json.size()),
                                  SQLITE_TRANSIENT);
                sqlite3_bind_text(
                    insert_norm.get(), 16, row.state.c_str(), static_cast<int>(row.state.size()), SQLITE_TRANSIENT);
                sqlite3_bind_int64(insert_norm.get(), 17, static_cast<sqlite3_int64>(row.seen_at_ms));
                sqlite3_bind_int64(insert_norm.get(), 18, static_cast<sqlite3_int64>(row.updated_at_ms));

                if (sqlite3_step(insert_norm.get()) != SQLITE_DONE)
                {
                    throw std::runtime_error(sqlite3_errmsg(archive_db));
                }
                sqlite3_reset(insert_norm.get());
                sqlite3_clear_bindings(insert_norm.get());
            }

            Statement insert_feed(archive_db,
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
                if (sqlite3_step(insert_feed.get()) != SQLITE_DONE)
                {
                    throw std::runtime_error(sqlite3_errmsg(archive_db));
                }
                sqlite3_reset(insert_feed.get());
                sqlite3_clear_bindings(insert_feed.get());
            }

            if (!exec(archive_db, "COMMIT;"))
            {
                throw std::runtime_error("archive commit failed");
            }
        }
        catch (const std::exception& e)
        {
            spdlog::error("Archive export write failed for month {}: {}", month_token, e.what());
            (void)exec(archive_db, "ROLLBACK;");
            sqlite3_close(archive_db);
            archive_db = nullptr;
            return false;
        }

        sqlite3_close(archive_db);
        archive_db = nullptr;

        if (!exec(_write_db, "BEGIN IMMEDIATE TRANSACTION;"))
        {
            return false;
        }

        try
        {
            Statement mark_norm_exported(_write_db,
                                         "UPDATE normalized_events_hot SET exported=1, updated_at_ms=?1 "
                                         "WHERE chain_id=?2 AND state='finalized' AND exported=0 "
                                         "AND block_time IS NOT NULL AND strftime('%Y-%m', block_time, 'unixepoch')=?3;");
            sqlite3_bind_int64(mark_norm_exported.get(), 1, static_cast<sqlite3_int64>(now_ms));
            sqlite3_bind_int(mark_norm_exported.get(), 2, chain_id);
            sqlite3_bind_text(
                mark_norm_exported.get(), 3, month_token.c_str(), static_cast<int>(month_token.size()), SQLITE_TRANSIENT);
            if (sqlite3_step(mark_norm_exported.get()) != SQLITE_DONE)
            {
                throw std::runtime_error(sqlite3_errmsg(_write_db));
            }

            Statement mark_raw_exported(_write_db,
                                        "UPDATE raw_events_hot SET exported=1, updated_at_ms=?1 "
                                        "WHERE chain_id=?2 AND state='finalized' AND exported=0 "
                                        "AND block_time IS NOT NULL AND strftime('%Y-%m', block_time, 'unixepoch')=?3;");
            sqlite3_bind_int64(mark_raw_exported.get(), 1, static_cast<sqlite3_int64>(now_ms));
            sqlite3_bind_int(mark_raw_exported.get(), 2, chain_id);
            sqlite3_bind_text(
                mark_raw_exported.get(), 3, month_token.c_str(), static_cast<int>(month_token.size()), SQLITE_TRANSIENT);
            if (sqlite3_step(mark_raw_exported.get()) != SQLITE_DONE)
            {
                throw std::runtime_error(sqlite3_errmsg(_write_db));
            }

            Statement mark_feed_exported(_write_db,
                                         "UPDATE feed_items_hot SET exported=1, updated_at_ms=?1 "
                                         "WHERE chain_id=?2 AND status='finalized' AND exported=0 "
                                         "AND block_time IS NOT NULL AND strftime('%Y-%m', block_time, 'unixepoch')=?3;");
            sqlite3_bind_int64(mark_feed_exported.get(), 1, static_cast<sqlite3_int64>(now_ms));
            sqlite3_bind_int(mark_feed_exported.get(), 2, chain_id);
            sqlite3_bind_text(
                mark_feed_exported.get(), 3, month_token.c_str(), static_cast<int>(month_token.size()), SQLITE_TRANSIENT);
            if (sqlite3_step(mark_feed_exported.get()) != SQLITE_DONE)
            {
                throw std::runtime_error(sqlite3_errmsg(_write_db));
            }

            std::int64_t min_block = 0;
            std::int64_t max_block = 0;
            std::int64_t row_count = 0;
            {
                Statement stats_stmt(_write_db,
                                     "SELECT COALESCE(MIN(block_number), 0), COALESCE(MAX(block_number), 0), COUNT(1) "
                                     "FROM normalized_events_hot "
                                     "WHERE chain_id=?1 AND state='finalized' "
                                     "AND block_time IS NOT NULL AND strftime('%Y-%m', block_time, 'unixepoch')=?2;");
                sqlite3_bind_int(stats_stmt.get(), 1, chain_id);
                sqlite3_bind_text(
                    stats_stmt.get(), 2, month_token.c_str(), static_cast<int>(month_token.size()), SQLITE_TRANSIENT);
                if (sqlite3_step(stats_stmt.get()) == SQLITE_ROW)
                {
                    min_block = static_cast<std::int64_t>(sqlite3_column_int64(stats_stmt.get(), 0));
                    max_block = static_cast<std::int64_t>(sqlite3_column_int64(stats_stmt.get(), 1));
                    row_count = static_cast<std::int64_t>(sqlite3_column_int64(stats_stmt.get(), 2));
                }
            }

            Statement catalog_stmt(
                _write_db,
                "INSERT INTO shard_catalog(chain_id, archive_month, path, state, min_block, max_block, row_count, "
                "last_export_ms) "
                "VALUES(?1, ?2, ?3, 'READY', ?4, ?5, ?6, ?7) "
                "ON CONFLICT(chain_id, archive_month) DO UPDATE SET "
                "path=excluded.path, state='READY', min_block=excluded.min_block, max_block=excluded.max_block, "
                "row_count=excluded.row_count, last_export_ms=excluded.last_export_ms;");
            sqlite3_bind_int(catalog_stmt.get(), 1, chain_id);
            sqlite3_bind_text(
                catalog_stmt.get(), 2, month_token.c_str(), static_cast<int>(month_token.size()), SQLITE_TRANSIENT);
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
            if (sqlite3_step(catalog_stmt.get()) != SQLITE_DONE)
            {
                throw std::runtime_error(sqlite3_errmsg(_write_db));
            }

            if (!exec(_write_db, "COMMIT;"))
            {
                throw std::runtime_error("commit failed");
            }
            return true;
        }
        catch (const std::exception& e)
        {
            spdlog::error("Archive export finalization failed for month {}: {}", month_token, e.what());
            (void)exec(_write_db, "ROLLBACK;");
            return false;
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

        Statement stmt(_read_db, sql.c_str());
        sqlite3_bind_int(stmt.get(), 1, before_key.has_value() ? before_key->chain_id : _default_chain_id);
        if (before_key.has_value())
        {
            sqlite3_bind_int64(stmt.get(), 2, static_cast<sqlite3_int64>(before_key->block_number));
        }

        while (sqlite3_step(stmt.get()) == SQLITE_ROW)
        {
            const unsigned char* txt = sqlite3_column_text(stmt.get(), 0);
            if (txt == nullptr)
            {
                continue;
            }
            paths.emplace_back(reinterpret_cast<const char*>(txt));
        }
        return paths;
    }

    void SQLiteHotStore::_appendFeedRowsFromDatabase(sqlite3* db,
                                                     const char* table_name,
                                                     const FeedQuery& query,
                                                     const std::optional<CursorKey>& before_key,
                                                     const std::size_t limit,
                                                     std::vector<FeedItem>& out_items,
                                                     std::set<std::string>& seen_feed_ids) const
    {
        std::string sql = std::format(
            "SELECT "
            "feed_id, event_type, status, visible, tx_hash, block_number, tx_index, log_index, block_time, "
            "history_cursor, payload_json, created_at_ms, updated_at_ms, projector_version "
            "FROM {} WHERE visible=1",
            table_name);

        if (!query.include_unfinalized)
        {
            sql += " AND status='finalized'";
        }
        if (query.event_type.has_value())
        {
            sql += " AND event_type=?1";
        }

        int before_param_base = query.event_type.has_value() ? 2 : 1;
        if (before_key.has_value())
        {
            sql += std::format(" AND ("
                               "block_number < ?{} "
                               "OR (block_number = ?{} AND tx_index < ?{}) "
                               "OR (block_number = ?{} AND tx_index = ?{} AND log_index < ?{}) "
                               "OR (block_number = ?{} AND tx_index = ?{} AND log_index = ?{} AND feed_id < ?{})"
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
            std::format(" ORDER BY block_number DESC, tx_index DESC, log_index DESC, feed_id DESC LIMIT ?{};", limit_param);

        Statement stmt(db, sql.c_str());

        int param_index = 1;
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
            sqlite3_bind_int64(stmt.get(), param_index++, static_cast<sqlite3_int64>(before_key->block_number));
            sqlite3_bind_int64(stmt.get(), param_index++, static_cast<sqlite3_int64>(before_key->block_number));
            sqlite3_bind_int64(stmt.get(), param_index++, static_cast<sqlite3_int64>(before_key->tx_index));
            sqlite3_bind_int64(stmt.get(), param_index++, static_cast<sqlite3_int64>(before_key->block_number));
            sqlite3_bind_int64(stmt.get(), param_index++, static_cast<sqlite3_int64>(before_key->tx_index));
            sqlite3_bind_int64(stmt.get(), param_index++, static_cast<sqlite3_int64>(before_key->log_index));
            sqlite3_bind_int64(stmt.get(), param_index++, static_cast<sqlite3_int64>(before_key->block_number));
            sqlite3_bind_int64(stmt.get(), param_index++, static_cast<sqlite3_int64>(before_key->tx_index));
            sqlite3_bind_int64(stmt.get(), param_index++, static_cast<sqlite3_int64>(before_key->log_index));
            sqlite3_bind_text(stmt.get(),
                              param_index++,
                              before_key->feed_id.c_str(),
                              static_cast<int>(before_key->feed_id.size()),
                              SQLITE_TRANSIENT);
        }
        sqlite3_bind_int64(stmt.get(), param_index, static_cast<sqlite3_int64>(limit));

        while (sqlite3_step(stmt.get()) == SQLITE_ROW)
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
            item.history_cursor = reinterpret_cast<const char*>(sqlite3_column_text(stmt.get(), 9));
            item.created_at_ms = static_cast<std::int64_t>(sqlite3_column_int64(stmt.get(), 11));
            item.updated_at_ms = static_cast<std::int64_t>(sqlite3_column_int64(stmt.get(), 12));
            item.projector_version = sqlite3_column_int(stmt.get(), 13);
            const std::string payload_json = reinterpret_cast<const char*>(sqlite3_column_text(stmt.get(), 10));
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
    }
} // namespace dcn::events
