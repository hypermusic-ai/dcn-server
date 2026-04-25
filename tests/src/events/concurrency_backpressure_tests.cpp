#include "unit-tests.hpp"

#include "events_sql_assertions.hpp"
#include "events_test_harness.hpp"

#include <atomic>
#include <thread>
#include <vector>

using namespace dcn;
using namespace dcn::tests;
using namespace dcn::tests::events_harness;

TEST_F(UnitTest, Events_Concurrency_ConcurrentIngestCalls_PreserveWriteCorrectness)
{
    const auto paths = makeTempEventsPaths("concurrency_ingest");
    events::SQLiteHotStore store(paths.hot_db, paths.archive_root, 60 * 60 * 1000, CHAIN_ID);

    constexpr int THREADS = 4;
    constexpr int EVENTS_PER_THREAD = 12;
    std::atomic<int> successful_ingests{0};

    std::vector<std::thread> workers;
    workers.reserve(THREADS);
    for(int t = 0; t < THREADS; ++t)
    {
        workers.emplace_back([&, t]
        {
            for(int i = 0; i < EVENTS_PER_THREAD; ++i)
            {
                const std::int64_t block = 1'000 + (t * EVENTS_PER_THREAD) + i;
                const std::uint8_t hash_byte = static_cast<std::uint8_t>(1 + (t * EVENTS_PER_THREAD) + i);
                events::DecodedEvent event = makeDecodedEvent(
                    block,
                    i,
                    1,
                    hash_byte,
                    static_cast<std::uint8_t>(0x80 + i),
                    events::EventType::CONNECTOR_ADDED,
                    events::EventState::OBSERVED,
                    1'700'003'000 + block,
                    1'700'003'000'000 + block);

                const events::ChainBlockInfo block_info = makeBlockInfo(
                    block,
                    event.raw.block_hash,
                    hexBytes(static_cast<std::uint8_t>(hash_byte == 0 ? 0 : hash_byte - 1), 32),
                    1'700'003'000 + block,
                    1'700'003'000'100 + block);

                if(store.ingestBatch(CHAIN_ID, {event}, {block_info}, block + 1, 1'700'003'000'200 + block))
                {
                    ++successful_ingests;
                }
            }
        });
    }

    for(auto & worker : workers)
    {
        worker.join();
    }

    EXPECT_EQ(successful_ingests.load(), THREADS * EVENTS_PER_THREAD);
    events_sql::expectRowCount(paths.hot_db, "raw_events_hot", THREADS * EVENTS_PER_THREAD);
    events_sql::expectRowCount(paths.hot_db, "normalized_events_hot", THREADS * EVENTS_PER_THREAD);
}

TEST_F(UnitTest, Events_Concurrency_ReadersDuringWriter_DoNotCorruptState)
{
    const auto paths = makeTempEventsPaths("concurrency_read_write");
    events::SQLiteHotStore store(paths.hot_db, paths.archive_root, 60 * 60 * 1000, CHAIN_ID);

    std::atomic<bool> done{false};
    std::atomic<int> read_iterations{0};

    std::thread reader([&]
    {
        while(!done.load(std::memory_order_acquire))
        {
            (void)store.getFeedPage(events::FeedQuery{
                .limit = 16,
                .include_unfinalized = true
            });
            (void)store.getStreamPage(events::StreamQuery{
                .since_seq = 0,
                .limit = 16
            });
            ++read_iterations;
        }
    });

    for(int i = 0; i < 24; ++i)
    {
        const std::int64_t block = 2'000 + i;
        events::DecodedEvent event = makeDecodedEvent(
            block,
            0,
            1,
            static_cast<std::uint8_t>(0x55 + i),
            static_cast<std::uint8_t>(0xA0 + i),
            events::EventType::TRANSFORMATION_ADDED,
            events::EventState::OBSERVED,
            1'700'003'500 + i,
            1'700'003'500'000 + i);
        const events::ChainBlockInfo block_info = makeBlockInfo(
            block,
            event.raw.block_hash,
            hexBytes(0x44, 32),
            1'700'003'500 + i,
            1'700'003'500'100 + i);

        ASSERT_TRUE(store.ingestBatch(CHAIN_ID, {event}, {block_info}, block + 1, 1'700'003'500'200 + i));
    }

    EXPECT_EQ(projectAll(store, 1'700'003'501'000), 24u);

    done.store(true, std::memory_order_release);
    reader.join();

    EXPECT_GT(read_iterations.load(), 0);
    events_sql::expectRowCount(paths.hot_db, "raw_events_hot", 24);
    events_sql::expectRowCount(paths.hot_db, "feed_items_hot", 24);
    {
        SqliteReadonly db(paths.hot_db);
        EXPECT_EQ(db.scalarInt64("SELECT COUNT(DISTINCT feed_id) FROM feed_items_hot;"), 24);
    }
}
