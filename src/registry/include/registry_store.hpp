#pragma once

#include <cstddef>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "address.hpp"
#include "pt.hpp"
#include "format_hash.hpp"
#include "parser.hpp"

namespace dcn::storage
{
    using ScalarLabel = dcn::chain::ScalarLabel;
    using ConnectorRecordHandle = std::shared_ptr<const ConnectorRecord>;
    using TransformationRecordHandle = std::shared_ptr<const TransformationRecord>;
    using ConditionRecordHandle = std::shared_ptr<const ConditionRecord>;

    struct ConnectorBatchItem
    {
        chain::Address address{};
        ConnectorRecord record;
        evmc::bytes32 format_hash{};
        std::vector<ScalarLabel> canonical_scalar_labels;
    };

    struct TransformationBatchItem
    {
        chain::Address address{};
        TransformationRecord record;
    };

    struct ConditionBatchItem
    {
        chain::Address address{};
        ConditionRecord record;
    };

    using NameCursor = std::string;

    inline std::string serializeNameCursor(const NameCursor & cursor)
    {
        return cursor;
    }

    struct NameCursorPage
    {
        std::vector<std::string> entries;
        std::optional<std::string> next_after;
        bool has_more = false;
    };

    enum class WalCheckpointMode : std::uint8_t
    {
        PASSIVE,
        FULL,
        TRUNCATE
    };

    class IRegistryStore
    {
        public:
            virtual ~IRegistryStore() = default;

            virtual bool hasConnector(const std::string & name) const = 0;
            virtual std::optional<ConnectorRecordHandle> getConnectorRecordHandle(const std::string & name) const = 0;
            virtual std::optional<evmc::bytes32> getConnectorFormatHash(const std::string & name) const = 0;
            virtual bool addConnector(
                const chain::Address & address,
                const ConnectorRecord & record,
                const evmc::bytes32 & format_hash,
                const std::vector<ScalarLabel> & canonical_scalar_labels) = 0;
            virtual bool addConnectorsBatch(
                const std::vector<ConnectorBatchItem> & items,
                bool all_or_nothing = true) = 0;
            virtual std::size_t getFormatConnectorNamesCount(const evmc::bytes32 & format_hash) const = 0;
            virtual NameCursorPage getFormatConnectorNamesCursor(
                const evmc::bytes32 & format_hash,
                const std::optional<NameCursor> & after,
                std::size_t limit) const = 0;
            virtual std::size_t getFormatsCount() const = 0;
            virtual NameCursorPage getFormatsCursor(
                const std::optional<evmc::bytes32> & after,
                std::size_t limit) const = 0;
            virtual std::optional<std::vector<ScalarLabel>> getScalarLabelsByFormatHash(
                const evmc::bytes32 & format_hash) const = 0;

            virtual bool hasTransformation(const std::string & name) const = 0;
            virtual std::optional<TransformationRecordHandle> getTransformationRecordHandle(
                const std::string & name) const = 0;
            virtual bool addTransformation(
                const chain::Address & address,
                const TransformationRecord & record) = 0;
            virtual bool addTransformationsBatch(
                const std::vector<TransformationBatchItem> & items,
                bool all_or_nothing = true) = 0;

            virtual bool hasCondition(const std::string & name) const = 0;
            virtual std::optional<ConditionRecordHandle> getConditionRecordHandle(
                const std::string & name) const = 0;
            virtual bool addCondition(
                const chain::Address & address,
                const ConditionRecord & record) = 0;
            virtual bool addConditionsBatch(
                const std::vector<ConditionBatchItem> & items,
                bool all_or_nothing = true) = 0;

            virtual NameCursorPage getOwnedConnectorsCursor(
                const chain::Address & owner,
                const std::optional<NameCursor> & after,
                std::size_t limit) const = 0;
            virtual NameCursorPage getOwnedTransformationsCursor(
                const chain::Address & owner,
                const std::optional<NameCursor> & after,
                std::size_t limit) const = 0;
            virtual NameCursorPage getOwnedConditionsCursor(
                const chain::Address & owner,
                const std::optional<NameCursor> & after,
                std::size_t limit) const = 0;
            virtual std::size_t getAccountsCount() const = 0;
            virtual NameCursorPage getAccountsCursor(
                const std::optional<chain::Address> & after,
                std::size_t limit) const = 0;

            virtual bool checkpointWal(WalCheckpointMode mode) const = 0;
    };
}

namespace dcn::parse
{
    Result<storage::NameCursor> parseNameCursor(const std::string & name_token);
    Result<evmc::bytes32> parseFormatCursorHex(const std::string & cursor_token);
}
