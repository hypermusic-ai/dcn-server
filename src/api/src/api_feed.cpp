#include "api.hpp"

#include <algorithm>
#include <format>
#include <limits>

namespace dcn
{
    namespace
    {
        constexpr std::size_t MAX_FEED_LIMIT = 256;
        constexpr std::size_t MAX_STREAM_LIMIT = 2048;
    }

    asio::awaitable<http::Response> OPTIONS_feed(const http::Request &, std::vector<server::RouteArg>, server::QueryArgsList)
    {
        http::Response response;
        response.setCode(http::Code::NoContent)
            .setVersion("HTTP/1.1")
            .setHeader(http::Header::AccessControlAllowOrigin, "*")
            .setHeader(http::Header::AccessControlAllowMethods, "GET, OPTIONS")
            .setHeader(http::Header::AccessControlAllowHeaders, "Content-Type")
            .setHeader(http::Header::AccessControlMaxAge, "600")
            .setHeader(http::Header::Connection, "close");
        co_return response;
    }

    asio::awaitable<http::Response> GET_feed(
        const http::Request &,
        std::vector<server::RouteArg> route_args,
        server::QueryArgsList query_args,
        events::EventRuntime & events_runtime)
    {
        http::Response response;
        response.setCode(http::Code::Unknown)
            .setVersion("HTTP/1.1")
            .setHeader(http::Header::AccessControlAllowOrigin, "*")
            .setHeader(http::Header::Connection, "close")
            .setHeader(http::Header::ContentType, "application/json");

        if(!route_args.empty())
        {
            response.setCode(http::Code::BadRequest)
                .setBodyWithContentLength(json{
                    {"message", "Invalid route arguments"}
                }.dump());
            co_return response;
        }

        if(!query_args.contains("limit"))
        {
            response.setCode(http::Code::BadRequest)
                .setBodyWithContentLength(json{
                    {"message", "Missing argument limit"}
                }.dump());
            co_return response;
        }

        auto limit_res = parse::parseRouteArgAs<std::size_t>(query_args.at("limit"));
        if(limit_res && limit_res.value() > MAX_FEED_LIMIT)
        {
            limit_res = std::unexpected(parse::ParseError{parse::ParseError::Kind::OUT_OF_RANGE});
        }
        if(!limit_res)
        {
            response.setCode(http::Code::BadRequest)
                .setBodyWithContentLength(json{
                    {"message", std::format("Invalid argument limit. limit error: {}.", limit_res.error().kind)}
                }.dump());
            co_return response;
        }

        events::FeedQuery feed_query;
        feed_query.limit = *limit_res;

        if(query_args.contains("before"))
        {
            auto before_res = parse::parseRouteArgAs<std::string>(query_args.at("before"));
            if(!before_res)
            {
                response.setCode(http::Code::BadRequest)
                    .setBodyWithContentLength(json{
                        {"message", "Invalid before cursor"}
                    }.dump());
                co_return response;
            }
            feed_query.before_cursor = *before_res;
        }

        if(query_args.contains("type"))
        {
            auto type_res = parse::parseRouteArgAs<std::string>(query_args.at("type"));
            if(!type_res)
            {
                response.setCode(http::Code::BadRequest)
                    .setBodyWithContentLength(json{
                        {"message", "Invalid event type"}
                    }.dump());
                co_return response;
            }

            if(!parse::parseEventType(*type_res).has_value())
            {
                response.setCode(http::Code::BadRequest)
                    .setBodyWithContentLength(json{
                        {"message", "Unknown event type"}
                    }.dump());
                co_return response;
            }

            feed_query.event_type = *type_res;
        }

        if(query_args.contains("include_unfinalized"))
        {
            auto include_res = parse::parseRouteArgAs<std::size_t>(query_args.at("include_unfinalized"));
            if(!include_res || (*include_res != 0u && *include_res != 1u))
            {
                response.setCode(http::Code::BadRequest)
                    .setBodyWithContentLength(json{
                        {"message", "Invalid include_unfinalized flag; expected 0 or 1"}
                    }.dump());
                co_return response;
            }
            feed_query.include_unfinalized = (*include_res == 1u);
        }

        const events::FeedPage page = events_runtime.getFeedPage(feed_query);

        json output;
        output["limit"] = feed_query.limit;
        output["cursor"] = json::object();
        output["cursor"]["has_more"] = page.has_more;
        output["cursor"]["next_before"] = page.next_before_cursor.has_value()
            ? json(*page.next_before_cursor)
            : json(nullptr);
        output["items"] = json::array();
        for(const events::FeedItem & item : page.items)
        {
            output["items"].push_back(json{
                {"feed_id", item.feed_id},
                {"event_type", item.event_type},
                {"status", item.status},
                {"visible", item.visible},
                {"tx_hash", item.tx_hash},
                {"block_number", item.block_number},
                {"tx_index", item.tx_index},
                {"log_index", item.log_index},
                {"history_cursor", item.history_cursor},
                {"created_at_ms", item.created_at_ms},
                {"updated_at_ms", item.updated_at_ms},
                {"projector_version", item.projector_version},
                {"payload", item.payload}
            });
        }

        response.setCode(http::Code::OK)
            .setBodyWithContentLength(output.dump());
        co_return response;
    }

    asio::awaitable<http::Response> OPTIONS_feedStream(const http::Request &, std::vector<server::RouteArg>, server::QueryArgsList)
    {
        http::Response response;
        response.setCode(http::Code::NoContent)
            .setVersion("HTTP/1.1")
            .setHeader(http::Header::AccessControlAllowOrigin, "*")
            .setHeader(http::Header::AccessControlAllowMethods, "GET, OPTIONS")
            .setHeader(http::Header::AccessControlAllowHeaders, "Content-Type")
            .setHeader(http::Header::AccessControlMaxAge, "600")
            .setHeader(http::Header::Connection, "close");
        co_return response;
    }

    asio::awaitable<http::Response> GET_feedStream(
        const http::Request &,
        std::vector<server::RouteArg> route_args,
        server::QueryArgsList query_args,
        events::EventRuntime & events_runtime)
    {
        http::Response response;
        response.setCode(http::Code::Unknown)
            .setVersion("HTTP/1.1")
            .setHeader(http::Header::AccessControlAllowOrigin, "*")
            .setHeader(http::Header::CacheControl, "no-cache")
            .setHeader(http::Header::Connection, "close")
            .setHeader(http::Header::ContentType, "text/event-stream; charset=utf-8");

        if(!route_args.empty())
        {
            response.setCode(http::Code::BadRequest)
                .setHeader(http::Header::ContentType, "application/json")
                .setBodyWithContentLength(json{
                    {"message", "Invalid route arguments"}
                }.dump());
            co_return response;
        }

        events::StreamQuery stream_query;
        stream_query.since_seq = 0;
        stream_query.limit = 200;

        if(query_args.contains("since_seq"))
        {
            auto since_res = parse::parseRouteArgAs<std::size_t>(query_args.at("since_seq"));
            if(!since_res || *since_res > static_cast<std::uint64_t>(std::numeric_limits<std::int64_t>::max()))
            {
                response.setCode(http::Code::BadRequest)
                    .setHeader(http::Header::ContentType, "application/json")
                    .setBodyWithContentLength(json{
                        {"message", "Invalid since_seq cursor"}
                    }.dump());
                co_return response;
            }
            stream_query.since_seq = static_cast<std::int64_t>(*since_res);
        }

        if(query_args.contains("limit"))
        {
            auto limit_res = parse::parseRouteArgAs<std::size_t>(query_args.at("limit"));
            if(!limit_res || *limit_res == 0 || *limit_res > MAX_STREAM_LIMIT)
            {
                response.setCode(http::Code::BadRequest)
                    .setHeader(http::Header::ContentType, "application/json")
                    .setBodyWithContentLength(json{
                        {"message", "Invalid stream limit"}
                    }.dump());
                co_return response;
            }
            stream_query.limit = *limit_res;
        }

        const events::StreamPage stream_page = events_runtime.getStreamPage(stream_query);

        std::string sse_body;
        sse_body.reserve(stream_page.deltas.size() * 256);
        sse_body += std::format(": min_available_seq={}\n\n", stream_page.min_available_seq);

        for(const events::StreamDelta & delta : stream_page.deltas)
        {
            json data{
                {"stream_seq", delta.stream_seq},
                {"op", delta.op},
                {"status", delta.status},
                {"feed_id", delta.feed_id},
                {"history_cursor", delta.history_cursor},
                {"created_at_ms", delta.created_at_ms},
                {"payload", delta.payload}
            };
            sse_body += std::format("id: {}\n", delta.stream_seq);
            sse_body += "event: feed_delta\n";
            sse_body += std::format("data: {}\n\n", data.dump());
        }

        json meta{
            {"has_more", stream_page.has_more},
            {"last_seq", stream_page.last_seq.has_value() ? json(*stream_page.last_seq) : json(nullptr)}
        };
        sse_body += "event: stream_meta\n";
        sse_body += std::format("data: {}\n\n", meta.dump());

        response.setCode(http::Code::OK)
            .setBodyWithContentLength(sse_body);
        co_return response;
    }
}
