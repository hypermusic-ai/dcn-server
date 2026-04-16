#include "unit-tests.hpp"

using namespace dcn;
using namespace dcn::tests;

namespace
{
    asio::awaitable<http::Response> noopHandler(
        const http::Request &,
        std::vector<server::RouteArg>,
        server::QueryArgsList)
    {
        co_return http::Response{};
    }
}

TEST_F(UnitTest, URL_GetPathModule_StripsQueryForSingleSegmentPath)
{
    const http::URL url("/accounts?limit=2&after=abc");
    EXPECT_EQ(url.getPathModule(), "/accounts");
}

TEST_F(UnitTest, URL_GetPathModule_StripsQueryForTemplateRoutePath)
{
    const http::URL url("/formats?limit=<uint>&after=<~string>");
    EXPECT_EQ(url.getPathModule(), "/formats");
}

TEST_F(UnitTest, Router_MatchesAccountsRouteOnSingleSegmentPathWithQuery)
{
    server::Router router;
    router.addRoute(
        {http::Method::GET, "/accounts?limit=<uint>&after=<~string>"},
        server::RouteHandlerFunc(server::HandlerDefinition(noopHandler)));

    http::Request request;
    request.setMethod(http::Method::GET)
           .setPath(http::URL("/accounts?limit=2"))
           .setVersion("HTTP/1.1");

    const auto [handler, args, query_args] = router.findRoute(request);
    ASSERT_NE(handler, nullptr);
    EXPECT_TRUE(args.empty());
    ASSERT_TRUE(query_args.contains("limit"));
    EXPECT_EQ(query_args.at("limit").getData(), "2");
    EXPECT_FALSE(query_args.contains("after"));
}

TEST_F(UnitTest, Router_MatchesFormatsRouteOnSingleSegmentPathWithQuery)
{
    server::Router router;
    router.addRoute(
        {http::Method::GET, "/formats?limit=<uint>&after=<~string>"},
        server::RouteHandlerFunc(server::HandlerDefinition(noopHandler)));

    http::Request request;
    request.setMethod(http::Method::GET)
           .setPath(http::URL("/formats?limit=3&after=abcd"))
           .setVersion("HTTP/1.1");

    const auto [handler, args, query_args] = router.findRoute(request);
    ASSERT_NE(handler, nullptr);
    EXPECT_TRUE(args.empty());
    ASSERT_TRUE(query_args.contains("limit"));
    ASSERT_TRUE(query_args.contains("after"));
    EXPECT_EQ(query_args.at("limit").getData(), "3");
    EXPECT_EQ(query_args.at("after").getData(), "abcd");
}
