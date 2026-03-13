#include "api.hpp"

// TODO
// ABI offset encoding (correctness)
// Add size limits (DoS protection)
// Define a real execution budget / gas policy

namespace dcn
{
    asio::awaitable<http::Response> HEAD_connector(const http::Request & request, std::vector<server::RouteArg> args, server::QueryArgsList, registry::Registry & registry)
    {
        http::Response response;
        response.setCode(http::Code::Unknown)
                .setVersion("HTTP/1.1")
                .setHeader(http::Header::AccessControlAllowOrigin, "*")
                .setHeader(http::Header::CacheControl, "no-store")
                .setHeader(http::Header::ContentLength, "0")
                .setHeader(http::Header::Connection, "close");

        // Validate path: /connector/<name> or /connector/<name>/<address>
        if (args.size() > 2 || args.size() == 0) {
            response.setCode(http::Code::BadRequest);
            co_return response;
        }

        const auto connector_name_result = parse::parseRouteArgAs<std::string>(args.at(0));
        if (!connector_name_result) {
            response.setCode(http::Code::BadRequest);
            co_return response;
        }
        const auto & connector_name = connector_name_result.value();

        std::optional<Connector> connector_res;

        if (args.size() == 2) {
            // /connector/<name>/<address>
            const auto connector_address_arg = parse::parseRouteArgAs<std::string>(args.at(1));
            if (!connector_address_arg) {
                response.setCode(http::Code::BadRequest);
                co_return response;
            }

            const auto connector_address_result = evmc::from_hex<chain::Address>(connector_address_arg.value());

            if (!connector_address_result) {
                response.setCode(http::Code::BadRequest);
                co_return response;
            }

            connector_res = co_await registry.getConnector(connector_name, connector_address_result.value());
        } else {
            // /connector/<name>
            connector_res = co_await registry.getNewestConnector(connector_name);
        }

        if (!connector_res) {
            // connector not found
            response.setCode(http::Code::NotFound);
            co_return response;
        }

        // connector exists
        response.setCode(http::Code::OK);
        co_return response;
    }

    asio::awaitable<http::Response> OPTIONS_connector(const http::Request & request, std::vector<server::RouteArg>, server::QueryArgsList)
    {
        http::Response response;
        response.setCode(http::Code::NoContent)
                .setVersion("HTTP/1.1")
                .setHeader(http::Header::AccessControlAllowOrigin, "*")
                .setHeader(http::Header::AccessControlAllowMethods, "HEAD, GET, POST, OPTIONS")
                .setHeader(http::Header::AccessControlAllowHeaders, "Authorization, Content-Type")
                .setHeader(http::Header::AccessControlMaxAge, "600")
                .setHeader(http::Header::Connection, "close");

        co_return response;
    }

    asio::awaitable<http::Response> GET_connector(const http::Request & request, std::vector<server::RouteArg> args, server::QueryArgsList, registry::Registry & registry, evm::EVM & evm)
    {
        http::Response response;
        response.setCode(http::Code::Unknown)
                .setVersion("HTTP/1.1")
                .setHeader(http::Header::AccessControlAllowOrigin, "*")
                .setHeader(http::Header::ContentType, "application/json")
                .setHeader(http::Header::CacheControl, "no-store")
                .setHeader(http::Header::Connection, "close");

        if(args.size() > 2 || args.size() == 0)
        {
            response.setCode(http::Code::BadRequest)
                .setBodyWithContentLength(json{
                    {"message", "Invalid number of arguments. Expected 1 or 2 arguments."}
                }.dump());

            co_return response;
        }

        auto connector_name_result = parse::parseRouteArgAs<std::string>(args.at(0));

        if(!connector_name_result)
        {
            response.setCode(http::Code::BadRequest)
                .setBodyWithContentLength(json{
                    {"message", "Invalid connector name"}
                }.dump());

            co_return response;
        }
        const auto & connector_name = connector_name_result.value();

        std::optional<Connector> connector_res;

        if(args.size() == 2)
        {
            const auto connector_address_arg = parse::parseRouteArgAs<std::string>(args.at(1));

            if(!connector_address_arg)
            {
                response.setCode(http::Code::BadRequest)
                    .setBodyWithContentLength(json {
                        {"message", "Invalid connector address argument"}
                    }.dump());

                co_return response;
            }

            const auto connector_address_result = evmc::from_hex<chain::Address>(connector_address_arg.value());

            if(!connector_address_result)
            {
                response.setCode(http::Code::BadRequest)
                    .setBodyWithContentLength(json{
                        {"message", "Invalid connector address argument value"}
                    }.dump());

                co_return response;
            }

            connector_res = co_await registry.getConnector(connector_name_result.value(), connector_address_result.value());
        }
        else if(args.size() == 1)
        {
            connector_res = co_await registry.getNewestConnector(connector_name_result.value());
        }

        if(!connector_res) 
        {
            response.setCode(http::Code::NotFound)
                .setBodyWithContentLength(json {
                    {"message", "Connector not found"}
                }.dump());

            co_return response;
        }
        
        auto json_res = parse::parseToJson(*connector_res, parse::use_json);

        if(!json_res)
        {
            response.setCode(http::Code::InternalServerError)
                .setBodyWithContentLength(json {
                    {"message", "Parsing response to json failed"}
                }.dump());

            co_return response;
        }

        std::vector<uint8_t> input_data;
        // function selector
        const auto selector = chain::constructSelector("getConnector(string)");
        input_data.insert(input_data.end(), selector.begin(), selector.end());

        // Step 2: Offset to string data (32 bytes with value 0x20)
        std::vector<uint8_t> offset(32, 0);
        offset[31] = 0x20;
        input_data.insert(input_data.end(), offset.begin(), offset.end());

        // Step 3: String length
        std::vector<uint8_t> str_len(32, 0);
        str_len[31] = static_cast<uint8_t>(connector_name.size());
        input_data.insert(input_data.end(), str_len.begin(), str_len.end());

        // Step 4: String bytes
        input_data.insert(input_data.end(), connector_name.begin(), connector_name.end());

        // Step 5: Padding to 32-byte boundary
        const std::size_t padding = (32 - (connector_name.size() % 32)) % 32;
        input_data.insert(input_data.end(), padding, 0);
        
        co_await evm.setGas(evm.getRegistryAddress(), evm::DEFAULT_GAS_LIMIT);
        const auto exec_result = co_await evm.execute(evm.getRegistryAddress(), evm.getRegistryAddress(), input_data, evm::DEFAULT_GAS_LIMIT, 0);

        // check execution status
        if(!exec_result)
        {
            response.setCode(http::Code::InternalServerError)
                .setBodyWithContentLength(json {
                    {"message", std::format("Failed to fetch connector : {}", exec_result.error().kind)}
                }.dump());
            
            co_return response;
        }

        const auto connector_address_res = chain::readAddressWord(exec_result.value());
        if(!connector_address_res)
        {
            response.setCode(http::Code::InternalServerError)
                .setBodyWithContentLength(json {
                    {"message", "Failed to fetch connector address"}
                }.dump());
            
            co_return response;
        }

        const auto & connector_address = connector_address_res.value();

        const auto owner_result = co_await fetchOwner(evm, connector_address);
        if(!owner_result)
        {
            response.setCode(http::Code::InternalServerError)
                .setBodyWithContentLength(json {
                    {"message", std::format("Failed to fetch owner : {}", owner_result.error().kind)}
                }.dump());
            
            co_return response;
        }

        const auto owner_address_res = chain::readAddressWord(owner_result.value());

        if(!owner_address_res)
        {
            response.setCode(http::Code::InternalServerError)
                .setBodyWithContentLength(json {
                    {"message", "Failed to fetch owner address"}
                }.dump());
            
            co_return response;
        }

        const auto & owner_address = owner_address_res.value();

        (*json_res)["owner"] = evmc::hex(owner_address);
        (*json_res)["local_address"] = evmc::hex(connector_address);
        (*json_res)["address"] = "0x0";

        response.setCode(http::Code::OK)
            .setBodyWithContentLength(json_res->dump());

        co_return response;
    }

    asio::awaitable<http::Response> POST_connector(const http::Request & request, std::vector<server::RouteArg> args, server::QueryArgsList,
        auth::AuthManager & auth_manager, registry::Registry & registry, evm::EVM & evm, const config::Config & config)
    {
        http::Response response;
        response.setCode(http::Code::Unknown)
                .setVersion("HTTP/1.1")
                .setHeader(http::Header::AccessControlAllowOrigin, "*")
                .setHeader(http::Header::ContentType, "application/json")
                .setHeader(http::Header::Connection, "close");

        if(!args.empty())
        {
            response.setCode(http::Code::BadRequest)
                .setBodyWithContentLength(json {
                    {"message", "Unexpected arguments"}
                }.dump());

            co_return response;
        }

        const auto auth_result = co_await authenticate(request, auth_manager);

        if(!auth_result)
        {
            response.setCode(http::Code::Unauthorized)
                .setBodyWithContentLength(json {
                    {"message", std::format("Authentication error: {}", auth_result.error().kind)}
                }.dump());

            co_return response;
        }
        const auto & address = auth_result.value();

        spdlog::debug(std::format("token verified address : {}", address));

        // parse connector from json_string
        const auto connector_res = parse::parseFromJson<Connector>(request.getBody(), parse::use_protobuf);

        if(!connector_res) 
        {
            response.setCode(http::Code::BadRequest)
                .setBodyWithContentLength(json {
                    {"message", "Failed to parse connector"}
                }.dump());

            co_return response;
        }

        const Connector & connector = *connector_res;

        ConnectorRecord connector_record;
        connector_record.set_owner(evmc::hex(address));
        *connector_record.mutable_connector() = std::move(connector);

        const auto deploy_res = co_await loader::deployConnector(evm, registry, connector_record, config.storage_path);
        if(!deploy_res)
        {
            response.setCode(http::Code::BadRequest)
                .setBodyWithContentLength(json {
                    {"message", std::format("Failed to deploy connector. Error: {}", deploy_res.error().kind)}
                }.dump());

            co_return response;
        }

        json json_output;
        json_output["name"] = connector_record.connector().name();
        json_output["owner"] = connector_record.owner();
        json_output["local_address"] = evmc::hex(deploy_res.value());
        json_output["address"] = "0x0";

        response.setCode(http::Code::Created)
            .setBodyWithContentLength(json_output.dump());

        co_return response;
    }
}