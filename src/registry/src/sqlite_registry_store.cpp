
#include "sqlite_registry_store.hpp"

#include <algorithm>
#include <array>
#include <cstdint>
#include <cstring>
#include <limits>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>

#include <evmc/hex.hpp>
#include <spdlog/spdlog.h>
#include <sqlite3.h>

namespace dcn::storage
{
    namespace
    {
        constexpr std::size_t ADDRESS_BYTES_SIZE = 20;
        constexpr std::size_t BYTES32_SIZE = 32;
        constexpr int SQLITE_DEFAULT_BUSY_TIMEOUT_MS = 5000;
        constexpr int SQLITE_CHECKPOINT_BUSY_TIMEOUT_MS = 250;

        class Statement
        {
            public:
                Statement(sqlite3 * db, const char * sql)
                {
                    if(sqlite3_prepare_v2(db, sql, -1, &_stmt, nullptr) != SQLITE_OK)
                    {
                        throw std::runtime_error(std::string("sqlite prepare failed: ") + sqlite3_errmsg(db));
                    }
                }

                ~Statement()
                {
                    if(_stmt != nullptr)
                    {
                        sqlite3_finalize(_stmt);
                    }
                }

                Statement(const Statement &) = delete;
                Statement & operator=(const Statement &) = delete;

                sqlite3_stmt * get() const
                {
                    return _stmt;
                }

                int step() const
                {
                    return sqlite3_step(_stmt);
                }

                void reset() const
                {
                    sqlite3_reset(_stmt);
                    sqlite3_clear_bindings(_stmt);
                }

            private:
                sqlite3_stmt * _stmt = nullptr;
        };

        static int toSqliteInt(std::size_t value)
        {
            constexpr std::size_t SQLITE_INT_MAX = static_cast<std::size_t>(std::numeric_limits<int>::max());
            return static_cast<int>(std::min(value, SQLITE_INT_MAX));
        }

        static int bindAddress(sqlite3_stmt * stmt, int index, const chain::Address & address)
        {
            return sqlite3_bind_blob(stmt, index, address.bytes, static_cast<int>(ADDRESS_BYTES_SIZE), SQLITE_TRANSIENT);
        }

        static int bindBytes32(sqlite3_stmt * stmt, int index, const evmc::bytes32 & value)
        {
            return sqlite3_bind_blob(stmt, index, value.bytes, static_cast<int>(BYTES32_SIZE), SQLITE_TRANSIENT);
        }

        static std::optional<evmc::bytes32> columnBytes32(sqlite3_stmt * stmt, int index)
        {
            const void * blob = sqlite3_column_blob(stmt, index);
            const int bytes = sqlite3_column_bytes(stmt, index);
            if(blob == nullptr || bytes != static_cast<int>(BYTES32_SIZE))
            {
                return std::nullopt;
            }

            evmc::bytes32 out{};
            std::memcpy(out.bytes, blob, BYTES32_SIZE);
            return out;
        }

        template <typename TRecord>
        static std::optional<TRecord> parseRecordBlob(sqlite3_stmt * stmt, int index)
        {
            const void * payload_blob = sqlite3_column_blob(stmt, index);
            const int payload_size = sqlite3_column_bytes(stmt, index);
            if(payload_blob == nullptr || payload_size <= 0)
            {
                return std::nullopt;
            }

            TRecord record;
            if(!record.ParseFromArray(payload_blob, payload_size))
            {
                return std::nullopt;
            }

            return record;
        }

        static const char * checkpointModeToString(const WalCheckpointMode mode)
        {
            switch(mode)
            {
                case WalCheckpointMode::PASSIVE:
                    return "PASSIVE";
                case WalCheckpointMode::FULL:
                    return "FULL";
                case WalCheckpointMode::TRUNCATE:
                    return "TRUNCATE";
                default:
                    return "UNKNOWN";
            }
        }

        static int toSqliteCheckpointMode(const WalCheckpointMode mode)
        {
            switch(mode)
            {
                case WalCheckpointMode::PASSIVE:
                    return SQLITE_CHECKPOINT_PASSIVE;
                case WalCheckpointMode::FULL:
                    return SQLITE_CHECKPOINT_FULL;
                case WalCheckpointMode::TRUNCATE:
                    return SQLITE_CHECKPOINT_TRUNCATE;
                default:
                    return SQLITE_CHECKPOINT_PASSIVE;
            }
        }
    }

    SQLiteRegistryStore::SQLiteRegistryStore(const std::string & db_path)
    {
        const int open_res = sqlite3_open_v2(
            db_path.empty() ? ":memory:" : db_path.c_str(),
            &_db,
            SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_FULLMUTEX,
            nullptr);
        if(open_res != SQLITE_OK)
        {
            const std::string msg = (_db == nullptr) ? "sqlite open failed" : sqlite3_errmsg(_db);
            if(_db != nullptr)
            {
                sqlite3_close(_db);
                _db = nullptr;
            }

            throw std::runtime_error(msg);
        }

        sqlite3_busy_timeout(_db, SQLITE_DEFAULT_BUSY_TIMEOUT_MS);

        if(!_exec("PRAGMA journal_mode=WAL;") || !_exec("PRAGMA synchronous=NORMAL;") ||
            !_exec("PRAGMA temp_store=MEMORY;") || !_exec("PRAGMA foreign_keys=OFF;"))
        {
            throw std::runtime_error("Failed to configure SQLite pragmas");
        }

        if(!_initializeSchema())
        {
            throw std::runtime_error("Failed to initialize SQLite registry schema");
        }
    }

    SQLiteRegistryStore::~SQLiteRegistryStore()
    {
        if(_db != nullptr)
        {
            sqlite3_close(_db);
            _db = nullptr;
        }
    }

    bool SQLiteRegistryStore::_exec(const char * sql) const
    {
        char * error_message = nullptr;
        const int res = sqlite3_exec(_db, sql, nullptr, nullptr, &error_message);
        if(res == SQLITE_OK)
        {
            return true;
        }

        const std::string error =
            (error_message == nullptr) ? std::string(sqlite3_errmsg(_db)) : std::string(error_message);
        if(error_message != nullptr)
        {
            sqlite3_free(error_message);
        }

        spdlog::error("SQLite exec failed: {} | sql={}", error, sql);
        return false;
    }

    bool SQLiteRegistryStore::_beginTransaction() const
    {
        return _exec("BEGIN IMMEDIATE TRANSACTION;");
    }

    bool SQLiteRegistryStore::_commitTransaction() const
    {
        return _exec("COMMIT;");
    }

    void SQLiteRegistryStore::_rollbackTransaction() const
    {
        (void)_exec("ROLLBACK;");
    }

    bool SQLiteRegistryStore::_initializeSchema() const
    {
        const bool base_schema_ok =
            _exec(
                "CREATE TABLE IF NOT EXISTS connectors ("
                "name TEXT PRIMARY KEY,"
                "owner BLOB NOT NULL,"
                "format_hash BLOB NOT NULL,"
                "payload_blob BLOB NOT NULL,"
                "created_at INTEGER NOT NULL DEFAULT (CAST(strftime('%s','now') AS INTEGER))"
                ");") &&
            _exec(
                "CREATE TABLE IF NOT EXISTS transformations ("
                "name TEXT PRIMARY KEY,"
                "owner BLOB NOT NULL,"
                "payload_blob BLOB NOT NULL,"
                "created_at INTEGER NOT NULL DEFAULT (CAST(strftime('%s','now') AS INTEGER))"
                ");") &&
            _exec(
                "CREATE TABLE IF NOT EXISTS conditions ("
                "name TEXT PRIMARY KEY,"
                "owner BLOB NOT NULL,"
                "payload_blob BLOB NOT NULL,"
                "created_at INTEGER NOT NULL DEFAULT (CAST(strftime('%s','now') AS INTEGER))"
                ");") &&
            _exec(
                "CREATE TABLE IF NOT EXISTS format_members ("
                "format_hash BLOB NOT NULL,"
                "name TEXT NOT NULL,"
                "PRIMARY KEY(format_hash, name)"
                ");") &&
            _exec(
                "CREATE TABLE IF NOT EXISTS scalar_labels_by_format ("
                "format_hash BLOB NOT NULL,"
                "scalar TEXT NOT NULL,"
                "path_hash BLOB NOT NULL,"
                "tail_id INTEGER NOT NULL,"
                "PRIMARY KEY(format_hash, scalar, path_hash, tail_id)"
                ");") &&
            _exec(
                "CREATE TABLE IF NOT EXISTS owned_connectors ("
                "owner BLOB NOT NULL,"
                "name TEXT NOT NULL,"
                "PRIMARY KEY(owner, name)"
                ");") &&
            _exec(
                "CREATE TABLE IF NOT EXISTS owned_transformations ("
                "owner BLOB NOT NULL,"
                "name TEXT NOT NULL,"
                "PRIMARY KEY(owner, name)"
                ");") &&
            _exec(
                "CREATE TABLE IF NOT EXISTS owned_conditions ("
                "owner BLOB NOT NULL,"
                "name TEXT NOT NULL,"
                "PRIMARY KEY(owner, name)"
                ");") &&
            _exec("CREATE INDEX IF NOT EXISTS idx_format_members_format_name ON format_members(format_hash, name);") &&
            _exec("CREATE INDEX IF NOT EXISTS idx_scalar_labels_format ON scalar_labels_by_format(format_hash);") &&
            _exec("CREATE INDEX IF NOT EXISTS idx_owned_connectors_owner_name ON owned_connectors(owner, name);") &&
            _exec("CREATE INDEX IF NOT EXISTS idx_owned_transformations_owner_name ON owned_transformations(owner, name);") &&
            _exec("CREATE INDEX IF NOT EXISTS idx_owned_conditions_owner_name ON owned_conditions(owner, name);");

        if(!base_schema_ok)
        {
            return false;
        }

        return true;
    }

    bool SQLiteRegistryStore::hasConnector(const std::string & name) const
    {
        try
        {
            Statement stmt(_db, "SELECT 1 FROM connectors WHERE name = ?1 LIMIT 1;");
            sqlite3_bind_text(stmt.get(), 1, name.c_str(), static_cast<int>(name.size()), SQLITE_TRANSIENT);
            return stmt.step() == SQLITE_ROW;
        }
        catch(const std::exception & e)
        {
            spdlog::error("SQLite hasConnector query failed for name={}: {}", name, e.what());
            return false;
        }
    }

    std::optional<ConnectorRecordHandle> SQLiteRegistryStore::getConnectorRecordHandle(const std::string & name) const
    {
        try
        {
            Statement stmt(_db, "SELECT payload_blob FROM connectors WHERE name = ?1 LIMIT 1;");
            sqlite3_bind_text(stmt.get(), 1, name.c_str(), static_cast<int>(name.size()), SQLITE_TRANSIENT);
            if(stmt.step() != SQLITE_ROW)
            {
                return std::nullopt;
            }

            auto record_opt = parseRecordBlob<ConnectorRecord>(stmt.get(), 0);
            if(!record_opt.has_value())
            {
                return std::nullopt;
            }

            return std::make_shared<ConnectorRecord>(std::move(*record_opt));
        }
        catch(const std::exception & e)
        {
            spdlog::error("SQLite getConnectorRecordHandle query failed for name={}: {}", name, e.what());
            return std::nullopt;
        }
    }

    std::optional<evmc::bytes32> SQLiteRegistryStore::getConnectorFormatHash(const std::string & name) const
    {
        try
        {
            Statement stmt(_db, "SELECT format_hash FROM connectors WHERE name = ?1 LIMIT 1;");
            sqlite3_bind_text(stmt.get(), 1, name.c_str(), static_cast<int>(name.size()), SQLITE_TRANSIENT);
            if(stmt.step() != SQLITE_ROW)
            {
                return std::nullopt;
            }
            return columnBytes32(stmt.get(), 0);
        }
        catch(const std::exception & e)
        {
            spdlog::error("SQLite getConnectorFormatHash query failed for name={}: {}", name, e.what());
            return std::nullopt;
        }
    }

    bool SQLiteRegistryStore::addConnector(
        const chain::Address & address,
        const ConnectorRecord & record,
        const evmc::bytes32 & format_hash,
        const std::vector<ScalarLabel> & canonical_scalar_labels)
    {
        const std::string & connector_name = record.connector().name();
        const auto owner_opt = evmc::from_hex<chain::Address>(record.owner());
        if(!owner_opt)
        {
            spdlog::error("Failed to parse connector owner for `{}`", connector_name);
            return false;
        }

        std::string payload_blob;
        if(!record.SerializeToString(&payload_blob))
        {
            spdlog::error("Failed to serialize connector record for `{}`", connector_name);
            return false;
        }

        spdlog::debug(
            "DB add connector name={} runtime_address={} format_hash={} scalar_labels={}",
            connector_name,
            evmc::hex(address),
            evmc::hex(format_hash),
            canonical_scalar_labels.size());

        if(!_beginTransaction())
        {
            return false;
        }

        auto rollback_with_log = [&](const char * reason)
        {
            spdlog::debug(
                "DB remove connector pending changes name={} runtime_address={} reason={}",
                connector_name,
                evmc::hex(address),
                reason);
            _rollbackTransaction();
        };

        try
        {
            Statement insert_connector(
                _db,
                "INSERT INTO connectors(name, owner, format_hash, payload_blob) VALUES(?1, ?2, ?3, ?4);");
            sqlite3_bind_text(insert_connector.get(), 1, connector_name.c_str(), static_cast<int>(connector_name.size()), SQLITE_TRANSIENT);
            bindAddress(insert_connector.get(), 2, *owner_opt);
            bindBytes32(insert_connector.get(), 3, format_hash);
            sqlite3_bind_blob(insert_connector.get(), 4, payload_blob.data(), static_cast<int>(payload_blob.size()), SQLITE_TRANSIENT);
            if(insert_connector.step() != SQLITE_DONE)
            {
                rollback_with_log("insert connectors");
                return false;
            }

            Statement insert_format_member(
                _db,
                "INSERT OR IGNORE INTO format_members(format_hash, name) VALUES(?1, ?2);");
            bindBytes32(insert_format_member.get(), 1, format_hash);
            sqlite3_bind_text(insert_format_member.get(), 2, connector_name.c_str(), static_cast<int>(connector_name.size()), SQLITE_TRANSIENT);
            if(insert_format_member.step() != SQLITE_DONE)
            {
                rollback_with_log("insert format_members");
                return false;
            }

            Statement insert_scalar_label(
                _db,
                "INSERT OR IGNORE INTO scalar_labels_by_format(format_hash, scalar, path_hash, tail_id) "
                "VALUES(?1, ?2, ?3, ?4);");
            for(const ScalarLabel & label : canonical_scalar_labels)
            {
                insert_scalar_label.reset();
                bindBytes32(insert_scalar_label.get(), 1, format_hash);
                sqlite3_bind_text(insert_scalar_label.get(), 2, label.scalar.c_str(), static_cast<int>(label.scalar.size()), SQLITE_TRANSIENT);
                bindBytes32(insert_scalar_label.get(), 3, label.path_hash);
                sqlite3_bind_int64(insert_scalar_label.get(), 4, static_cast<sqlite3_int64>(label.tail_id));
                if(insert_scalar_label.step() != SQLITE_DONE)
                {
                    rollback_with_log("insert scalar_labels_by_format");
                    return false;
                }
            }

            Statement insert_owned(
                _db,
                "INSERT OR REPLACE INTO owned_connectors(owner, name) VALUES(?1, ?2);");
            bindAddress(insert_owned.get(), 1, *owner_opt);
            sqlite3_bind_text(insert_owned.get(), 2, connector_name.c_str(), static_cast<int>(connector_name.size()), SQLITE_TRANSIENT);
            if(insert_owned.step() != SQLITE_DONE)
            {
                rollback_with_log("insert owned_connectors");
                return false;
            }
        }
        catch(const std::exception & e)
        {
            spdlog::error("SQLite addConnector failed: {}", e.what());
            rollback_with_log("exception");
            return false;
        }

        if(!_commitTransaction())
        {
            rollback_with_log("commit failed");
            return false;
        }

        spdlog::debug("DB add connector committed name={} runtime_address={}", connector_name, evmc::hex(address));
        return true;
    }

    bool SQLiteRegistryStore::addConnectorsBatch(const std::vector<ConnectorBatchItem> & items, bool all_or_nothing)
    {
        if(items.empty())
        {
            return true;
        }

        if(!_beginTransaction())
        {
            return false;
        }

        bool all_ok = true;
        std::size_t inserted_count = 0;
        std::size_t failed_count = 0;
        auto rollback_batch_with_log = [&](const char * reason)
        {
            spdlog::debug("DB remove connector batch pending changes count={} reason={}", items.size(), reason);
            _rollbackTransaction();
        };

        try
        {
            Statement insert_connector(_db, "INSERT INTO connectors(name, owner, format_hash, payload_blob) VALUES(?1, ?2, ?3, ?4);");
            Statement insert_format_member(_db, "INSERT OR IGNORE INTO format_members(format_hash, name) VALUES(?1, ?2);");
            Statement insert_scalar_label(_db, "INSERT OR IGNORE INTO scalar_labels_by_format(format_hash, scalar, path_hash, tail_id) VALUES(?1, ?2, ?3, ?4);");
            Statement insert_owned(_db, "INSERT OR REPLACE INTO owned_connectors(owner, name) VALUES(?1, ?2);");

            for(const ConnectorBatchItem & item : items)
            {
                const bool use_savepoint = !all_or_nothing;
                bool savepoint_active = false;
                if(use_savepoint)
                {
                    if(!_exec("SAVEPOINT connector_batch_item;"))
                    {
                        all_ok = false;
                        ++failed_count;
                        continue;
                    }
                    savepoint_active = true;
                }

                auto fail_item = [&](const char * reason)
                {
                    spdlog::debug("DB remove connector pending changes (batch item) name={} runtime_address={} reason={}", item.record.connector().name(), evmc::hex(item.address), reason);
                    if(savepoint_active)
                    {
                        (void)_exec("ROLLBACK TO SAVEPOINT connector_batch_item;");
                        (void)_exec("RELEASE SAVEPOINT connector_batch_item;");
                        savepoint_active = false;
                    }
                    return false;
                };

                const auto owner_opt = evmc::from_hex<chain::Address>(item.record.owner());
                if(!owner_opt)
                {
                    all_ok = false;
                    if(all_or_nothing)
                    {
                        rollback_batch_with_log("invalid owner");
                        return false;
                    }
                    (void)fail_item("invalid owner");
                    ++failed_count;
                    continue;
                }

                std::string payload_blob;
                if(!item.record.SerializeToString(&payload_blob))
                {
                    all_ok = false;
                    if(all_or_nothing)
                    {
                        rollback_batch_with_log("serialize connector");
                        return false;
                    }
                    (void)fail_item("serialize connector");
                    ++failed_count;
                    continue;
                }

                bool item_ok = true;

                insert_connector.reset();
                sqlite3_bind_text(insert_connector.get(), 1, item.record.connector().name().c_str(), static_cast<int>(item.record.connector().name().size()), SQLITE_TRANSIENT);
                bindAddress(insert_connector.get(), 2, *owner_opt);
                bindBytes32(insert_connector.get(), 3, item.format_hash);
                sqlite3_bind_blob(insert_connector.get(), 4, payload_blob.data(), static_cast<int>(payload_blob.size()), SQLITE_TRANSIENT);
                if(insert_connector.step() != SQLITE_DONE)
                {
                    item_ok = fail_item("insert connectors");
                }

                if(item_ok)
                {
                    insert_format_member.reset();
                    bindBytes32(insert_format_member.get(), 1, item.format_hash);
                    sqlite3_bind_text(insert_format_member.get(), 2, item.record.connector().name().c_str(), static_cast<int>(item.record.connector().name().size()), SQLITE_TRANSIENT);
                    if(insert_format_member.step() != SQLITE_DONE)
                    {
                        item_ok = fail_item("insert format_members");
                    }
                }

                if(item_ok)
                {
                    for(const ScalarLabel & label : item.canonical_scalar_labels)
                    {
                        insert_scalar_label.reset();
                        bindBytes32(insert_scalar_label.get(), 1, item.format_hash);
                        sqlite3_bind_text(insert_scalar_label.get(), 2, label.scalar.c_str(), static_cast<int>(label.scalar.size()), SQLITE_TRANSIENT);
                        bindBytes32(insert_scalar_label.get(), 3, label.path_hash);
                        sqlite3_bind_int64(insert_scalar_label.get(), 4, static_cast<sqlite3_int64>(label.tail_id));
                        if(insert_scalar_label.step() != SQLITE_DONE)
                        {
                            item_ok = fail_item("insert scalar_labels_by_format");
                            break;
                        }
                    }
                }

                if(item_ok)
                {
                    insert_owned.reset();
                    bindAddress(insert_owned.get(), 1, *owner_opt);
                    sqlite3_bind_text(insert_owned.get(), 2, item.record.connector().name().c_str(), static_cast<int>(item.record.connector().name().size()), SQLITE_TRANSIENT);
                    if(insert_owned.step() != SQLITE_DONE)
                    {
                        item_ok = fail_item("insert owned_connectors");
                    }
                }

                if(item_ok && savepoint_active)
                {
                    if(!_exec("RELEASE SAVEPOINT connector_batch_item;"))
                    {
                        item_ok = fail_item("release savepoint");
                    }
                    else
                    {
                        savepoint_active = false;
                    }
                }

                if(!item_ok)
                {
                    all_ok = false;
                    ++failed_count;
                    if(all_or_nothing)
                    {
                        rollback_batch_with_log("batch item failure");
                        return false;
                    }
                }
                else
                {
                    ++inserted_count;
                }
            }
        }
        catch(const std::exception & e)
        {
            spdlog::error("SQLite addConnectorsBatch failed: {}", e.what());
            rollback_batch_with_log("exception");
            return false;
        }

        if(!_commitTransaction())
        {
            rollback_batch_with_log("commit failed");
            return false;
        }

        spdlog::debug("DB add connector batch committed inserted={} failed={}", inserted_count, failed_count);
        return all_ok;
    }

    std::size_t SQLiteRegistryStore::getFormatConnectorNamesCount(const evmc::bytes32 & format_hash) const
    {
        try
        {
            Statement stmt(_db, "SELECT COUNT(*) FROM format_members WHERE format_hash = ?1;");
            bindBytes32(stmt.get(), 1, format_hash);
            if(stmt.step() != SQLITE_ROW)
            {
                return 0;
            }
            return static_cast<std::size_t>(sqlite3_column_int64(stmt.get(), 0));
        }
        catch(const std::exception & e)
        {
            spdlog::error(
                "SQLite getFormatConnectorNamesCount query failed for format_hash={}: {}",
                evmc::hex(format_hash),
                e.what());
            return 0;
        }
    }

    NameCursorPage SQLiteRegistryStore::getFormatConnectorNamesCursor(
        const evmc::bytes32 & format_hash,
        const std::optional<NameCursor> & after,
        std::size_t limit) const
    {
        NameCursorPage page;
        if(limit == 0)
        {
            return page;
        }

        try
        {
            const std::size_t query_limit = limit + 1;
            if(after.has_value())
            {
                Statement stmt(_db, "SELECT name FROM format_members WHERE format_hash = ?1 AND name > ?2 ORDER BY name ASC LIMIT ?3;");
                bindBytes32(stmt.get(), 1, format_hash);
                sqlite3_bind_text(stmt.get(), 2, after->c_str(), static_cast<int>(after->size()), SQLITE_TRANSIENT);
                sqlite3_bind_int(stmt.get(), 3, toSqliteInt(query_limit));
                for(int rc = stmt.step(); rc == SQLITE_ROW; rc = stmt.step())
                {
                    const unsigned char * name_text = sqlite3_column_text(stmt.get(), 0);
                    if(name_text != nullptr)
                    {
                        page.entries.emplace_back(reinterpret_cast<const char *>(name_text));
                    }
                }
            }
            else
            {
                Statement stmt(_db, "SELECT name FROM format_members WHERE format_hash = ?1 ORDER BY name ASC LIMIT ?2;");
                bindBytes32(stmt.get(), 1, format_hash);
                sqlite3_bind_int(stmt.get(), 2, toSqliteInt(query_limit));
                for(int rc = stmt.step(); rc == SQLITE_ROW; rc = stmt.step())
                {
                    const unsigned char * name_text = sqlite3_column_text(stmt.get(), 0);
                    if(name_text != nullptr)
                    {
                        page.entries.emplace_back(reinterpret_cast<const char *>(name_text));
                    }
                }
            }

            if(page.entries.size() > limit)
            {
                page.has_more = true;
                page.entries.resize(limit);
            }
            if(page.has_more && !page.entries.empty())
            {
                page.next_after = page.entries.back();
            }
        }
        catch(const std::exception & e)
        {
            spdlog::error(
                "SQLite getFormatConnectorNamesCursor query failed for format_hash={} after={} limit={}: {}",
                evmc::hex(format_hash),
                after.value_or("<none>"),
                limit,
                e.what());
            return NameCursorPage{};
        }

        return page;
    }

    std::optional<std::vector<ScalarLabel>> SQLiteRegistryStore::getScalarLabelsByFormatHash(const evmc::bytes32 & format_hash) const
    {
        try
        {
            Statement stmt(
                _db,
                "SELECT scalar, path_hash, tail_id FROM scalar_labels_by_format WHERE format_hash = ?1 ORDER BY path_hash ASC, scalar ASC, tail_id ASC;");
            bindBytes32(stmt.get(), 1, format_hash);

            std::vector<ScalarLabel> labels;
            for(int rc = stmt.step(); rc == SQLITE_ROW; rc = stmt.step())
            {
                const unsigned char * scalar_text = sqlite3_column_text(stmt.get(), 0);
                const auto path_hash = columnBytes32(stmt.get(), 1);
                const sqlite3_int64 tail_id_raw = sqlite3_column_int64(stmt.get(), 2);
                if(scalar_text == nullptr || !path_hash.has_value())
                {
                    continue;
                }
                if(tail_id_raw < 0 || tail_id_raw > static_cast<sqlite3_int64>(std::numeric_limits<std::uint32_t>::max()))
                {
                    continue;
                }
                labels.push_back(ScalarLabel{.scalar = std::string(reinterpret_cast<const char *>(scalar_text)), .path_hash = *path_hash, .tail_id = static_cast<std::uint32_t>(tail_id_raw)});
            }

            if(labels.empty())
            {
                return std::nullopt;
            }
            return labels;
        }
        catch(const std::exception & e)
        {
            spdlog::error(
                "SQLite getScalarLabelsByFormatHash query failed for format_hash={}: {}",
                evmc::hex(format_hash),
                e.what());
            return std::nullopt;
        }
    }

    bool SQLiteRegistryStore::hasTransformation(const std::string & name) const
    {
        try
        {
            Statement stmt(_db, "SELECT 1 FROM transformations WHERE name = ?1 LIMIT 1;");
            sqlite3_bind_text(stmt.get(), 1, name.c_str(), static_cast<int>(name.size()), SQLITE_TRANSIENT);
            return stmt.step() == SQLITE_ROW;
        }
        catch(const std::exception & e)
        {
            spdlog::error("SQLite hasTransformation query failed for name={}: {}", name, e.what());
            return false;
        }
    }

    std::optional<TransformationRecordHandle> SQLiteRegistryStore::getTransformationRecordHandle(const std::string & name) const
    {
        try
        {
            Statement stmt(_db, "SELECT payload_blob FROM transformations WHERE name = ?1 LIMIT 1;");
            sqlite3_bind_text(stmt.get(), 1, name.c_str(), static_cast<int>(name.size()), SQLITE_TRANSIENT);
            if(stmt.step() != SQLITE_ROW)
            {
                return std::nullopt;
            }

            auto record_opt = parseRecordBlob<TransformationRecord>(stmt.get(), 0);
            if(!record_opt.has_value())
            {
                return std::nullopt;
            }

            return std::make_shared<TransformationRecord>(std::move(*record_opt));
        }
        catch(const std::exception & e)
        {
            spdlog::error("SQLite getTransformationRecordHandle query failed for name={}: {}", name, e.what());
            return std::nullopt;
        }
    }

    bool SQLiteRegistryStore::addTransformation(const chain::Address & address, const TransformationRecord & record)
    {
        const std::string & transformation_name = record.transformation().name();
        const auto owner_opt = evmc::from_hex<chain::Address>(record.owner());
        if(!owner_opt)
        {
            spdlog::error("Failed to parse transformation owner for `{}`", transformation_name);
            return false;
        }

        std::string payload_blob;
        if(!record.SerializeToString(&payload_blob))
        {
            spdlog::error("Failed to serialize transformation `{}`", transformation_name);
            return false;
        }

        if(!_beginTransaction())
        {
            return false;
        }

        auto rollback_with_log = [&](const char * reason)
        {
            spdlog::debug("DB remove transformation pending changes name={} runtime_address={} reason={}", transformation_name, evmc::hex(address), reason);
            _rollbackTransaction();
        };

        try
        {
            Statement insert_entity(_db, "INSERT INTO transformations(name, owner, payload_blob) VALUES(?1, ?2, ?3);");
            sqlite3_bind_text(insert_entity.get(), 1, transformation_name.c_str(), static_cast<int>(transformation_name.size()), SQLITE_TRANSIENT);
            bindAddress(insert_entity.get(), 2, *owner_opt);
            sqlite3_bind_blob(insert_entity.get(), 3, payload_blob.data(), static_cast<int>(payload_blob.size()), SQLITE_TRANSIENT);
            if(insert_entity.step() != SQLITE_DONE)
            {
                rollback_with_log("insert transformations");
                return false;
            }

            Statement insert_owned(_db, "INSERT OR REPLACE INTO owned_transformations(owner, name) VALUES(?1, ?2);");
            bindAddress(insert_owned.get(), 1, *owner_opt);
            sqlite3_bind_text(insert_owned.get(), 2, transformation_name.c_str(), static_cast<int>(transformation_name.size()), SQLITE_TRANSIENT);
            if(insert_owned.step() != SQLITE_DONE)
            {
                rollback_with_log("insert owned_transformations");
                return false;
            }
        }
        catch(const std::exception & e)
        {
            spdlog::error("SQLite addTransformation failed: {}", e.what());
            rollback_with_log("exception");
            return false;
        }

        if(!_commitTransaction())
        {
            rollback_with_log("commit failed");
            return false;
        }

        return true;
    }

    bool SQLiteRegistryStore::addTransformationsBatch(const std::vector<TransformationBatchItem> & items, bool all_or_nothing)
    {
        if(items.empty())
        {
            return true;
        }

        if(!_beginTransaction())
        {
            return false;
        }

        bool all_ok = true;
        std::size_t inserted_count = 0;
        std::size_t failed_count = 0;
        auto rollback_batch_with_log = [&](const char * reason)
        {
            spdlog::debug("DB remove transformation batch pending changes count={} reason={}", items.size(), reason);
            _rollbackTransaction();
        };

        try
        {
            Statement insert_entity(_db, "INSERT INTO transformations(name, owner, payload_blob) VALUES(?1, ?2, ?3);");
            Statement insert_owned(_db, "INSERT OR REPLACE INTO owned_transformations(owner, name) VALUES(?1, ?2);");
            for(const TransformationBatchItem & item : items)
            {
                const bool use_savepoint = !all_or_nothing;
                bool savepoint_active = false;
                if(use_savepoint)
                {
                    if(!_exec("SAVEPOINT transformation_batch_item;"))
                    {
                        all_ok = false;
                        ++failed_count;
                        continue;
                    }
                    savepoint_active = true;
                }

                auto fail_item = [&](const char * reason)
                {
                    spdlog::debug(
                        "DB remove transformation pending changes (batch item) name={} runtime_address={} reason={}",
                        item.record.transformation().name(),
                        evmc::hex(item.address),
                        reason);
                    if(savepoint_active)
                    {
                        (void)_exec("ROLLBACK TO SAVEPOINT transformation_batch_item;");
                        (void)_exec("RELEASE SAVEPOINT transformation_batch_item;");
                        savepoint_active = false;
                    }
                    return false;
                };

                const auto owner_opt = evmc::from_hex<chain::Address>(item.record.owner());
                if(!owner_opt)
                {
                    all_ok = false;
                    if(all_or_nothing)
                    {
                        rollback_batch_with_log("invalid owner");
                        return false;
                    }
                    (void)fail_item("invalid owner");
                    ++failed_count;
                    continue;
                }

                std::string payload_blob;
                if(!item.record.SerializeToString(&payload_blob))
                {
                    all_ok = false;
                    if(all_or_nothing)
                    {
                        rollback_batch_with_log("serialize transformation");
                        return false;
                    }
                    (void)fail_item("serialize transformation");
                    ++failed_count;
                    continue;
                }

                bool item_ok = true;

                insert_entity.reset();
                sqlite3_bind_text(insert_entity.get(), 1, item.record.transformation().name().c_str(), static_cast<int>(item.record.transformation().name().size()), SQLITE_TRANSIENT);
                bindAddress(insert_entity.get(), 2, *owner_opt);
                sqlite3_bind_blob(insert_entity.get(), 3, payload_blob.data(), static_cast<int>(payload_blob.size()), SQLITE_TRANSIENT);
                if(insert_entity.step() != SQLITE_DONE)
                {
                    item_ok = fail_item("insert transformations");
                }

                if(item_ok)
                {
                    insert_owned.reset();
                    bindAddress(insert_owned.get(), 1, *owner_opt);
                    sqlite3_bind_text(insert_owned.get(), 2, item.record.transformation().name().c_str(), static_cast<int>(item.record.transformation().name().size()), SQLITE_TRANSIENT);
                    if(insert_owned.step() != SQLITE_DONE)
                    {
                        item_ok = fail_item("insert owned_transformations");
                    }
                }

                if(item_ok && savepoint_active)
                {
                    if(!_exec("RELEASE SAVEPOINT transformation_batch_item;"))
                    {
                        item_ok = fail_item("release savepoint");
                    }
                    else
                    {
                        savepoint_active = false;
                    }
                }

                if(!item_ok)
                {
                    all_ok = false;
                    ++failed_count;
                    if(all_or_nothing)
                    {
                        rollback_batch_with_log("batch item failure");
                        return false;
                    }
                }
                else
                {
                    ++inserted_count;
                }
            }
        }
        catch(const std::exception & e)
        {
            spdlog::error("SQLite addTransformationsBatch failed: {}", e.what());
            rollback_batch_with_log("exception");
            return false;
        }

        if(!_commitTransaction())
        {
            rollback_batch_with_log("commit failed");
            return false;
        }

        spdlog::debug("DB add transformation batch committed inserted={} failed={}", inserted_count, failed_count);
        return all_ok;
    }

    bool SQLiteRegistryStore::hasCondition(const std::string & name) const
    {
        try
        {
            Statement stmt(_db, "SELECT 1 FROM conditions WHERE name = ?1 LIMIT 1;");
            sqlite3_bind_text(stmt.get(), 1, name.c_str(), static_cast<int>(name.size()), SQLITE_TRANSIENT);
            return stmt.step() == SQLITE_ROW;
        }
        catch(const std::exception & e)
        {
            spdlog::error("SQLite hasCondition query failed for name={}: {}", name, e.what());
            return false;
        }
    }

    std::optional<ConditionRecordHandle> SQLiteRegistryStore::getConditionRecordHandle(const std::string & name) const
    {
        try
        {
            Statement stmt(_db, "SELECT payload_blob FROM conditions WHERE name = ?1 LIMIT 1;");
            sqlite3_bind_text(stmt.get(), 1, name.c_str(), static_cast<int>(name.size()), SQLITE_TRANSIENT);
            if(stmt.step() != SQLITE_ROW)
            {
                return std::nullopt;
            }

            auto record_opt = parseRecordBlob<ConditionRecord>(stmt.get(), 0);
            if(!record_opt.has_value())
            {
                return std::nullopt;
            }

            return std::make_shared<ConditionRecord>(std::move(*record_opt));
        }
        catch(const std::exception & e)
        {
            spdlog::error("SQLite getConditionRecordHandle query failed for name={}: {}", name, e.what());
            return std::nullopt;
        }
    }

    bool SQLiteRegistryStore::addCondition(const chain::Address & address, const ConditionRecord & record)
    {
        const std::string & condition_name = record.condition().name();
        const auto owner_opt = evmc::from_hex<chain::Address>(record.owner());
        if(!owner_opt)
        {
            spdlog::error("Failed to parse condition owner for `{}`", condition_name);
            return false;
        }

        std::string payload_blob;
        if(!record.SerializeToString(&payload_blob))
        {
            spdlog::error("Failed to serialize condition `{}`", condition_name);
            return false;
        }

        if(!_beginTransaction())
        {
            return false;
        }

        auto rollback_with_log = [&](const char * reason)
        {
            spdlog::debug("DB remove condition pending changes name={} runtime_address={} reason={}", condition_name, evmc::hex(address), reason);
            _rollbackTransaction();
        };

        try
        {
            Statement insert_entity(_db, "INSERT INTO conditions(name, owner, payload_blob) VALUES(?1, ?2, ?3);");
            sqlite3_bind_text(insert_entity.get(), 1, condition_name.c_str(), static_cast<int>(condition_name.size()), SQLITE_TRANSIENT);
            bindAddress(insert_entity.get(), 2, *owner_opt);
            sqlite3_bind_blob(insert_entity.get(), 3, payload_blob.data(), static_cast<int>(payload_blob.size()), SQLITE_TRANSIENT);
            if(insert_entity.step() != SQLITE_DONE)
            {
                rollback_with_log("insert conditions");
                return false;
            }

            Statement insert_owned(_db, "INSERT OR REPLACE INTO owned_conditions(owner, name) VALUES(?1, ?2);");
            bindAddress(insert_owned.get(), 1, *owner_opt);
            sqlite3_bind_text(insert_owned.get(), 2, condition_name.c_str(), static_cast<int>(condition_name.size()), SQLITE_TRANSIENT);
            if(insert_owned.step() != SQLITE_DONE)
            {
                rollback_with_log("insert owned_conditions");
                return false;
            }
        }
        catch(const std::exception & e)
        {
            spdlog::error("SQLite addCondition failed: {}", e.what());
            rollback_with_log("exception");
            return false;
        }

        if(!_commitTransaction())
        {
            rollback_with_log("commit failed");
            return false;
        }

        return true;
    }

    bool SQLiteRegistryStore::addConditionsBatch(const std::vector<ConditionBatchItem> & items, bool all_or_nothing)
    {
        if(items.empty())
        {
            return true;
        }

        if(!_beginTransaction())
        {
            return false;
        }

        bool all_ok = true;
        std::size_t inserted_count = 0;
        std::size_t failed_count = 0;
        auto rollback_batch_with_log = [&](const char * reason)
        {
            spdlog::debug("DB remove condition batch pending changes count={} reason={}", items.size(), reason);
            _rollbackTransaction();
        };

        try
        {
            Statement insert_entity(_db, "INSERT INTO conditions(name, owner, payload_blob) VALUES(?1, ?2, ?3);");
            Statement insert_owned(_db, "INSERT OR REPLACE INTO owned_conditions(owner, name) VALUES(?1, ?2);");
            for(const ConditionBatchItem & item : items)
            {
                const bool use_savepoint = !all_or_nothing;
                bool savepoint_active = false;
                if(use_savepoint)
                {
                    if(!_exec("SAVEPOINT condition_batch_item;"))
                    {
                        all_ok = false;
                        ++failed_count;
                        continue;
                    }
                    savepoint_active = true;
                }

                auto fail_item = [&](const char * reason)
                {
                    spdlog::debug(
                        "DB remove condition pending changes (batch item) name={} runtime_address={} reason={}",
                        item.record.condition().name(),
                        evmc::hex(item.address),
                        reason);
                    if(savepoint_active)
                    {
                        (void)_exec("ROLLBACK TO SAVEPOINT condition_batch_item;");
                        (void)_exec("RELEASE SAVEPOINT condition_batch_item;");
                        savepoint_active = false;
                    }
                    return false;
                };

                const auto owner_opt = evmc::from_hex<chain::Address>(item.record.owner());
                if(!owner_opt)
                {
                    all_ok = false;
                    if(all_or_nothing)
                    {
                        rollback_batch_with_log("invalid owner");
                        return false;
                    }
                    (void)fail_item("invalid owner");
                    ++failed_count;
                    continue;
                }

                std::string payload_blob;
                if(!item.record.SerializeToString(&payload_blob))
                {
                    all_ok = false;
                    if(all_or_nothing)
                    {
                        rollback_batch_with_log("serialize condition");
                        return false;
                    }
                    (void)fail_item("serialize condition");
                    ++failed_count;
                    continue;
                }

                bool item_ok = true;

                insert_entity.reset();
                sqlite3_bind_text(insert_entity.get(), 1, item.record.condition().name().c_str(), static_cast<int>(item.record.condition().name().size()), SQLITE_TRANSIENT);
                bindAddress(insert_entity.get(), 2, *owner_opt);
                sqlite3_bind_blob(insert_entity.get(), 3, payload_blob.data(), static_cast<int>(payload_blob.size()), SQLITE_TRANSIENT);
                if(insert_entity.step() != SQLITE_DONE)
                {
                    item_ok = fail_item("insert conditions");
                }

                if(item_ok)
                {
                    insert_owned.reset();
                    bindAddress(insert_owned.get(), 1, *owner_opt);
                    sqlite3_bind_text(insert_owned.get(), 2, item.record.condition().name().c_str(), static_cast<int>(item.record.condition().name().size()), SQLITE_TRANSIENT);
                    if(insert_owned.step() != SQLITE_DONE)
                    {
                        item_ok = fail_item("insert owned_conditions");
                    }
                }

                if(item_ok && savepoint_active)
                {
                    if(!_exec("RELEASE SAVEPOINT condition_batch_item;"))
                    {
                        item_ok = fail_item("release savepoint");
                    }
                    else
                    {
                        savepoint_active = false;
                    }
                }

                if(!item_ok)
                {
                    all_ok = false;
                    ++failed_count;
                    if(all_or_nothing)
                    {
                        rollback_batch_with_log("batch item failure");
                        return false;
                    }
                }
                else
                {
                    ++inserted_count;
                }
            }
        }
        catch(const std::exception & e)
        {
            spdlog::error("SQLite addConditionsBatch failed: {}", e.what());
            rollback_batch_with_log("exception");
            return false;
        }

        if(!_commitTransaction())
        {
            rollback_batch_with_log("commit failed");
            return false;
        }

        spdlog::debug("DB add condition batch committed inserted={} failed={}", inserted_count, failed_count);
        return all_ok;
    }

    NameCursorPage SQLiteRegistryStore::_getOwnedCursorFromTable(
        const char * table_name,
        const chain::Address & owner,
        const std::optional<NameCursor> & after,
        std::size_t limit) const
    {
        NameCursorPage page;
        if(limit == 0)
        {
            return page;
        }

        try
        {
            const std::size_t query_limit = limit + 1;
            if(after.has_value())
            {
                const std::string sql =
                    std::string("SELECT name FROM ") + table_name +
                    " WHERE owner = ?1 AND name > ?2 ORDER BY name ASC LIMIT ?3;";
                Statement stmt(_db, sql.c_str());
                bindAddress(stmt.get(), 1, owner);
                sqlite3_bind_text(stmt.get(), 2, after->c_str(), static_cast<int>(after->size()), SQLITE_TRANSIENT);
                sqlite3_bind_int(stmt.get(), 3, toSqliteInt(query_limit));

                for(int rc = stmt.step(); rc == SQLITE_ROW; rc = stmt.step())
                {
                    const unsigned char * name_text = sqlite3_column_text(stmt.get(), 0);
                    if(name_text != nullptr)
                    {
                        page.entries.push_back(std::string(reinterpret_cast<const char *>(name_text)));
                    }
                }
            }
            else
            {
                const std::string sql =
                    std::string("SELECT name FROM ") + table_name +
                    " WHERE owner = ?1 ORDER BY name ASC LIMIT ?2;";
                Statement stmt(_db, sql.c_str());
                bindAddress(stmt.get(), 1, owner);
                sqlite3_bind_int(stmt.get(), 2, toSqliteInt(query_limit));
                for(int rc = stmt.step(); rc == SQLITE_ROW; rc = stmt.step())
                {
                    const unsigned char * name_text = sqlite3_column_text(stmt.get(), 0);
                    if(name_text != nullptr)
                    {
                        page.entries.push_back(std::string(reinterpret_cast<const char *>(name_text)));
                    }
                }
            }

            if(page.entries.size() > limit)
            {
                page.has_more = true;
                page.entries.resize(limit);
            }

            if(page.has_more && !page.entries.empty())
            {
                page.next_after = page.entries.back();
            }
        }
        catch(const std::exception & e)
        {
            spdlog::error(
                "SQLite owned cursor query failed for table={} owner={} after={} limit={}: {}",
                table_name,
                evmc::hex(owner),
                after.value_or("<none>"),
                limit,
                e.what());
            return NameCursorPage{};
        }

        return page;
    }

    NameCursorPage SQLiteRegistryStore::getOwnedConnectorsCursor(const chain::Address & owner, const std::optional<NameCursor> & after, std::size_t limit) const
    {
        return _getOwnedCursorFromTable("owned_connectors", owner, after, limit);
    }

    NameCursorPage SQLiteRegistryStore::getOwnedTransformationsCursor(const chain::Address & owner, const std::optional<NameCursor> & after, std::size_t limit) const
    {
        return _getOwnedCursorFromTable("owned_transformations", owner, after, limit);
    }

    NameCursorPage SQLiteRegistryStore::getOwnedConditionsCursor(const chain::Address & owner, const std::optional<NameCursor> & after, std::size_t limit) const
    {
        return _getOwnedCursorFromTable("owned_conditions", owner, after, limit);
    }

    bool SQLiteRegistryStore::checkpointWal(const WalCheckpointMode mode) const
    {
        if(_db == nullptr)
        {
            return false;
        }

        const int set_short_timeout_res = sqlite3_busy_timeout(_db, SQLITE_CHECKPOINT_BUSY_TIMEOUT_MS);
        if(set_short_timeout_res != SQLITE_OK)
        {
            spdlog::warn(
                "Failed to set SQLite checkpoint busy timeout mode={} rc={} err={}",
                checkpointModeToString(mode),
                set_short_timeout_res,
                sqlite3_errmsg(_db));
        }

        int wal_frames_total = 0;
        int wal_frames_checkpointed = 0;
        const int res = sqlite3_wal_checkpoint_v2(
            _db,
            nullptr,
            toSqliteCheckpointMode(mode),
            &wal_frames_total,
            &wal_frames_checkpointed);
        (void)sqlite3_busy_timeout(_db, SQLITE_DEFAULT_BUSY_TIMEOUT_MS);

        if(res != SQLITE_OK)
        {
            spdlog::warn(
                "SQLite WAL checkpoint failed mode={} rc={} err={}",
                checkpointModeToString(mode),
                res,
                sqlite3_errmsg(_db));
            return false;
        }

        spdlog::debug(
            "SQLite WAL checkpoint mode={} frames_total={} frames_checkpointed={}",
            checkpointModeToString(mode),
            wal_frames_total,
            wal_frames_checkpointed);
        return true;
    }
}


namespace dcn::parse
{
    Result<storage::NameCursor> parseNameCursor(const std::string & name_token)
    {
        if(name_token.empty())
        {
            return std::unexpected(ParseError{ParseError::Kind::INVALID_VALUE, "Name cannot be empty"});
        }

        const auto first_non_space = std::find_if_not(
            name_token.begin(),
            name_token.end(),
            [](unsigned char ch)
            {
                return std::isspace(ch) != 0;
            });
        
        if(first_non_space == name_token.end())
        {
            return std::unexpected(ParseError{ParseError::Kind::INVALID_VALUE, "Name cannot be empty"});
        }

        const auto last_non_space = std::find_if_not(
            name_token.rbegin(),
            name_token.rend(),
            [](unsigned char ch)
            {
                return std::isspace(ch) != 0;
            }).base();
        
        return std::string(first_non_space, last_non_space);
    }
}
