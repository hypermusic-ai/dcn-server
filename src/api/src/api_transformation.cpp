#include "api.hpp"

// TODO
// ABI offset encoding (correctness)
// Add size limits (DoS protection)
// Define a real execution budget / gas policy

namespace dcn
{

    asio::awaitable<http::Response> HEAD_transformation(const http::Request & request, std::vector<server::RouteArg> args, server::QueryArgsList, registry::Registry & registry)
    {
        http::Response response;
        response.setCode(http::Code::Unknown)
                .setVersion("HTTP/1.1")
                .setHeader(http::Header::AccessControlAllowOrigin, "*")
                .setHeader(http::Header::CacheControl, "no-store")
                .setHeader(http::Header::ContentLength, "0")
                .setHeader(http::Header::Connection, "close");

        // Expect /transformation/<name> or /transformation/<name>/<address>
        if (args.size() > 2 || args.size() == 0) {
            response.setCode(http::Code::BadRequest);
            co_return response;
        }

        const auto transformation_name_result = parse::parseRouteArgAs<std::string>(args.at(0));

        if (!transformation_name_result) {
            response.setCode(http::Code::BadRequest);
            co_return response;
        }

        const auto & transformation_name = transformation_name_result.value();

        std::optional<Transformation> transformation_res;

        if (args.size() == 2) {
            // /transformation/<name>/<address>
            const auto transformation_address_arg = parse::parseRouteArgAs<std::string>(args.at(1));

            if (!transformation_address_arg) {
                response.setCode(http::Code::BadRequest);
                co_return response;
            }

            const auto transformation_address_result = evmc::from_hex<evm::Address>(*transformation_address_arg);

            if (!transformation_address_result) {
                response.setCode(http::Code::BadRequest);
                co_return response;
            }

            transformation_res = co_await registry.getTransformation(
                transformation_name,
                transformation_address_result.value()
            );
        } else {
            // /transformation/<name>
            transformation_res = co_await registry.getNewestTransformation(
                transformation_name
            );
        }

        if (!transformation_res) {
            // Transformation not found
            response.setCode(http::Code::NotFound);
            co_return response;
        }

        // Transformation exists
        response.setCode(http::Code::OK);
        co_return response;
    }

    asio::awaitable<http::Response> OPTIONS_transformation(const http::Request & request, std::vector<server::RouteArg>, server::QueryArgsList)
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

    asio::awaitable<http::Response> GET_transformation(const http::Request & request, std::vector<server::RouteArg> args, server::QueryArgsList, registry::Registry & registry, evm::EVM & evm)
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
                .setBodyWithContentLength(json {
                    {"message", "Invalid number of arguments. Expected 1 or 2 arguments."}
                }.dump());

            co_return response;
        }

        const auto transformation_name_result = parse::parseRouteArgAs<std::string>(args.at(0));

        if(!transformation_name_result)
        {
            response.setCode(http::Code::BadRequest)
                .setBodyWithContentLength(json {
                    {"message", "Invalid transformation name"}
                }.dump());

            co_return response;
        }
        const auto & transformation_name = transformation_name_result.value();

        std::optional<Transformation> transformation_res;

        if(args.size() == 2)
        {
            const auto transformation_address_arg = parse::parseRouteArgAs<std::string>(args.at(1));

            if(!transformation_address_arg)
            {
                response.setCode(http::Code::BadRequest)
                    .setBodyWithContentLength(json {
                        {"message", "Invalid transformation address"}
                    }.dump());

                co_return response;
            }

            const auto transformation_address_result = evmc::from_hex<evm::Address>(*transformation_address_arg);

            if(!transformation_address_result)
            {
                response.setCode(http::Code::BadRequest)
                    .setBodyWithContentLength(json {
                        {"message", "Invalid transformation address value"}
                    }.dump());

                co_return response;
            }

            transformation_res = co_await registry.getTransformation(transformation_name_result.value(), transformation_address_result.value());
        }
        else if(args.size() == 1)
        {
            transformation_res = co_await registry.getNewestTransformation(transformation_name_result.value());
        }

        if(!transformation_res) 
        {
            response.setCode(http::Code::NotFound)
                .setBodyWithContentLength(json {
                    {"message", "Transformation not found"}
                }.dump());

            co_return response;
        }
        
        auto json_res = parse::parseToJson(*transformation_res, parse::use_json);

        if(!json_res)
        {
            response.setCode(http::Code::InternalServerError)
                .setBodyWithContentLength(json {
                    {"message", "Cannot parse transformation to JSON"}
                }.dump());

            co_return response;
        }

        std::vector<uint8_t> input_data;
        // function selector
        const auto selector = evm::constructSelector("getTransformation(string)");
        input_data.insert(input_data.end(), selector.begin(), selector.end());

        // Step 2: Offset to string data (32 bytes with value 0x20)
        std::vector<uint8_t> offset(32, 0);
        offset[31] = 0x20;
        input_data.insert(input_data.end(), offset.begin(), offset.end());

        // Step 3: String length
        std::vector<uint8_t> str_len(32, 0);
        str_len[31] = static_cast<uint8_t>(transformation_name.size());
        input_data.insert(input_data.end(), str_len.begin(), str_len.end());

        // Step 4: String bytes
        input_data.insert(input_data.end(), transformation_name.begin(), transformation_name.end());

        // Step 5: Padding to 32-byte boundary
        const std::size_t padding = (32 - (transformation_name.size() % 32)) % 32;
        input_data.insert(input_data.end(), padding, 0);

        co_await evm.setGas(evm.getRegistryAddress(), evm::DEFAULT_GAS_LIMIT);
        const auto exec_result = co_await evm.execute(evm.getRegistryAddress(), evm.getRegistryAddress(), input_data, evm::DEFAULT_GAS_LIMIT, 0);

        // check execution status
        if(!exec_result)
        {
            response.setCode(http::Code::InternalServerError)
                .setBodyWithContentLength(json {
                    {"message", std::format("Failed to fetch transformation : {}", exec_result.error())}
                }.dump());

            co_return response;
        }

        const auto transformation_address = evm::decodeReturnedValue<evm::Address>(exec_result.value());
        const auto owner_result = co_await fetchOwner(evm, transformation_address);
        if(!owner_result)
        {
            response.setCode(http::Code::InternalServerError)
                .setBodyWithContentLength(json {
                    {"message", std::format("Failed to fetch owner: {}", owner_result.error())}
                }.dump());
            
            co_return response;
        }

        const auto owner_address = evm::decodeReturnedValue<evm::Address>(owner_result.value());

        (*json_res)["owner"] = evmc::hex(owner_address);
        (*json_res)["local_address"] = evmc::hex(transformation_address);
        (*json_res)["address"] = "0x0";

        response.setCode(http::Code::OK)
            .setBodyWithContentLength(json_res->dump());
        
        co_return response;
    }

    asio::awaitable<http::Response> POST_transformation(const http::Request & request, std::vector<server::RouteArg> args, server::QueryArgsList, auth::AuthManager & auth_manager, registry::Registry & registry, evm::EVM & evm)
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

        spdlog::debug(std::format("token verified address: {}", address));
        
        const auto transformation_res = parse::parseFromJson<Transformation>(request.getBody(), parse::use_protobuf);

        if(!transformation_res) 
        {
            response.setCode(http::Code::BadRequest)
                .setBodyWithContentLength(json {
                    {"message", std::format("Failed to parse transformation: {}", transformation_res.error().kind)}
                }.dump());

            co_return response;
        }

        const Transformation & transformation = *transformation_res;

        TransformationRecord transformation_record;
        transformation_record.set_owner(evmc::hex(address));
        *transformation_record.mutable_transformation() = std::move(transformation);

        const auto deploy_res = co_await loader::deployTransformation(evm, registry, transformation_record);
        if(!deploy_res)
        {
            response.setCode(http::Code::BadRequest)
                .setBodyWithContentLength(json {
                    {"message", std::format("Failed to deploy transformation. Error: {}", deploy_res.error().kind)}
                }.dump());

            co_return response;
        }

        json json_output;
        json_output["name"] = transformation_record.transformation().name();
        json_output["owner"] = transformation_record.owner();
        json_output["local_address"] = evmc::hex(deploy_res.value());
        json_output["address"] = "0x0";

        response.setCode(http::Code::Created)
            .setBodyWithContentLength(json_output.dump());

        co_return response;
    }
}