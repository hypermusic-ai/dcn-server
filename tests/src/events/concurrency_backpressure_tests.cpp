#include "unit-tests.hpp"

#include "events_sql_assertions.hpp"
#include "events_test_harness.hpp"

#include <atomic>
#include <chrono>
#include <exception>
#include <future>
#include <memory>
#include <thread>
#include <type_traits>
#include <vector>

using namespace dcn;
using namespace dcn::tests;
using namespace dcn::tests::events_harness;

namespace
{
    template<class Fn>
    auto runOnStrand(
        asio::io_context & io_context,
        asio::strand<asio::io_context::executor_type> & strand,
        Fn fn)
    {
        using ReturnT = std::invoke_result_t<Fn>;

        auto promise = std::make_shared<std::promise<ReturnT>>();
        auto future = promise->get_future();

        asio::post(strand, [promise, fn = std::move(fn)]() mutable
        {
            try
            {
                if constexpr(std::is_void_v<ReturnT>)
                {
                    fn();
                    promise->set_value();
                }
                else
                {
                    promise->set_value(fn());
                }
            }
            catch(...)
            {
                promise->set_exception(std::current_exception());
            }
        });

        io_context.restart();
        io_context.run();
        if constexpr(std::is_void_v<ReturnT>)
        {
            future.get();
        }
        else
        {
            return future.get();
        }
    }
}

TEST_F(UnitTest, Events_Concurrency_StrandSerializedIngestAndProject_AreDeterministic)
{
    const auto paths = makeTempEventsPaths("concurrency_bound_strand_marshal");
    asio::io_context io_context;
    auto hot_write_strand = asio::make_strand(io_context);
    events::SQLiteHotStore store(paths.hot_db, paths.archive_root, 60 * 60 * 1000, CHAIN_ID);

    const events::DecodedEvent event = makeDecodedEvent(
        1'000,
        0,
        1,
        0x11,
        0x81,
        events::EventType::CONNECTOR_ADDED,
        events::EventState::OBSERVED,
        1'700'003'000,
        1'700'003'000'000);
    const events::ChainBlockInfo block = makeBlockInfo(
        1'000,
        event.raw.block_hash,
        hexBytes(0x10, 32),
        1'700'003'000,
        1'700'003'000'100);

    EXPECT_TRUE(awaitIngestBatch(io_context, store, CHAIN_ID, {event}, {block}, 1'001, 1'700'003'000'200));
    events_sql::expectRowCount(paths.hot_db, "raw_events_hot", 1);

    const bool accepted = runOnStrand(
        io_context,
        hot_write_strand,
        [&]() -> bool
        {
            return store.ingestBatch(CHAIN_ID, {event}, {block}, 1'001, 1'700'003'000'200);
        });
    EXPECT_TRUE(accepted);
    events_sql::expectRowCount(paths.hot_db, "raw_events_hot", 1);

    const std::size_t projected = runOnStrand(
        io_context,
        hot_write_strand,
        [&]() -> std::size_t
        {
            return store.projectBatch(8, 1'700'003'000'300);
        });
    EXPECT_EQ(projected, 1u);
    events_sql::expectRowCount(paths.hot_db, "feed_items_hot", 1);
}

TEST_F(UnitTest, Events_Concurrency_IngestAndProjectSerializeOnSameHotWriteStrand)
{
    const auto paths = makeTempEventsPaths("concurrency_ingest_project_same_strand");
    asio::io_context io_context;
    auto hot_write_strand = asio::make_strand(io_context);
    events::SQLiteHotStore store(paths.hot_db, paths.archive_root, 60 * 60 * 1000, CHAIN_ID);

    auto work_guard = asio::make_work_guard(io_context);
    std::vector<std::thread> io_workers;
    for(int i = 0; i < 4; ++i)
    {
        io_workers.emplace_back([&]
        {
            io_context.run();
        });
    }

    std::vector<std::future<bool>> ingest_futures;
    std::vector<std::future<std::size_t>> project_futures;
    constexpr int EVENT_COUNT = 32;

    for(int i = 0; i < EVENT_COUNT; ++i)
    {
        auto ingest_promise = std::make_shared<std::promise<bool>>();
        ingest_futures.push_back(ingest_promise->get_future());
        asio::post(hot_write_strand, [&, i, ingest_promise]()
        {
            try
            {
                const std::int64_t block = 3'000 + i;
                const auto event = makeDecodedEvent(
                    block,
                    0,
                    1,
                    static_cast<std::uint8_t>(0x30 + i),
                    static_cast<std::uint8_t>(0x70 + i),
                    events::EventType::TRANSFORMATION_ADDED,
                    events::EventState::OBSERVED,
                    1'700'003'300 + i,
                    1'700'003'300'000 + i);
                const auto block_info = makeBlockInfo(
                    block,
                    event.raw.block_hash,
                    hexBytes(static_cast<std::uint8_t>(0x20 + i), 32),
                    1'700'003'300 + i,
                    1'700'003'300'100 + i);
                const bool ok = store.ingestBatch(
                    CHAIN_ID,
                    std::vector<events::DecodedEvent>{event},
                    std::vector<events::ChainBlockInfo>{block_info},
                    block + 1,
                    1'700'003'300'200 + i);
                ingest_promise->set_value(ok);
            }
            catch(...)
            {
                ingest_promise->set_exception(std::current_exception());
            }
        });

        if((i % 3) == 2)
        {
            auto project_promise = std::make_shared<std::promise<std::size_t>>();
            project_futures.push_back(project_promise->get_future());
            asio::post(hot_write_strand, [&, i, project_promise]()
            {
                try
                {
                    project_promise->set_value(store.projectBatch(8, 1'700'003'400'000 + i));
                }
                catch(...)
                {
                    project_promise->set_exception(std::current_exception());
                }
            });
        }
    }

    for(auto & future : ingest_futures)
    {
        EXPECT_TRUE(future.get());
    }

    std::size_t projected_partial = 0;
    for(auto & future : project_futures)
    {
        projected_partial += future.get();
    }

    auto flush_promise = std::make_shared<std::promise<std::size_t>>();
    auto flush_future = flush_promise->get_future();
    asio::post(hot_write_strand, [&, flush_promise]()
    {
        try
        {
            std::size_t projected = 0;
            for(std::size_t i = 0; i < 128; ++i)
            {
                const std::size_t step = store.projectBatch(32, 1'700'003'500'000 + static_cast<std::int64_t>(i));
                projected += step;
                if(step == 0)
                {
                    break;
                }
            }
            flush_promise->set_value(projected);
        }
        catch(...)
        {
            flush_promise->set_exception(std::current_exception());
        }
    });
    const std::size_t projected_flush = flush_future.get();

    work_guard.reset();
    io_context.stop();
    for(auto & worker : io_workers)
    {
        worker.join();
    }

    events_sql::expectRowCount(paths.hot_db, "raw_events_hot", EVENT_COUNT);
    events_sql::expectRowCount(paths.hot_db, "normalized_events_hot", EVENT_COUNT);
    events_sql::expectRowCount(paths.hot_db, "feed_items_hot", EVENT_COUNT);
    EXPECT_EQ(projected_partial + projected_flush, static_cast<std::size_t>(EVENT_COUNT));
}

TEST_F(UnitTest, Events_Runtime_TransportPath_NotExecutedOnHotWriteStrand)
{
    const auto paths = makeTempEventsPaths("concurrency_transport_not_hot_strand");

    asio::io_context io_context;
    events::EventRuntime runtime(
        io_context,
        events::EventRuntimeConfig{
            .hot_db_path = paths.hot_db,
            .archive_root = paths.archive_root,
            .chain_id = CHAIN_ID,
            .ingestion_enabled = true,
            .rpc_url = "",
            .registry_address = hexAddress(0xAB),
            .rpc_timeout_ms = 100,
            .poll_interval_ms = 20,
            .projector_interval_ms = 20,
            .archive_interval_ms = 5'000,
            .wal_checkpoint_interval_ms = 5'000
        });

    runtime.start();
    std::thread io_worker([&]
    {
        io_context.run();
    });

    std::this_thread::sleep_for(std::chrono::milliseconds(120));
    auto stop_future = asio::co_spawn(io_context, runtime.stop(), asio::use_future);
    stop_future.get();
    io_worker.join();

    EXPECT_GT(runtime.rpcTransportCallCount(), 0u);
    EXPECT_FALSE(runtime.blockingTransportObservedOnHotWriteStrand());
}

TEST_F(UnitTest, Events_Concurrency_ReadersDuringWriter_DoNotCorruptState)
{
    const auto paths = makeTempEventsPaths("concurrency_read_write");
    asio::io_context store_io_context;
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

        ASSERT_TRUE(awaitIngestBatch(
            store_io_context,
            store,
            CHAIN_ID,
            {event},
            {block_info},
            block + 1,
            1'700'003'500'200 + i));
    }

    EXPECT_EQ(projectAll(store_io_context, store, 1'700'003'501'000), 24u);

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
