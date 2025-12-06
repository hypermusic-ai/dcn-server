#include "api.hpp"

namespace dcn
{

    asio::awaitable<http::Response> HEAD_feature(const http::Request & request, std::vector<server::RouteArg> args, server::QueryArgsList, registry::Registry & registry)
    {
        http::Response response;
        response.setVersion("HTTP/1.1");

        setCORSHeaders(request, response);

        // Validate path: /feature/<name> or /feature/<name>/<address>
        if (args.size() > 2 || args.size() == 0) {
            response.setCode(http::Code::BadRequest);
            response.setHeader(http::Header::ContentType, "application/json");
            // For HEAD: no body, just headers
            response.setHeader(http::Header::ContentLength, "0");
            co_return response;
        }

        auto feature_name_result = parse::parseRouteArgAs<std::string>(args.at(0));
        if (!feature_name_result) {
            response.setCode(http::Code::BadRequest);
            response.setHeader(http::Header::ContentType, "application/json");
            response.setHeader(http::Header::ContentLength, "0");
            co_return response;
        }
        const auto & feature_name = feature_name_result.value();

        std::optional<Feature> feature_res;

        if (args.size() == 2) {
            // /feature/<name>/<address>
            const auto feature_address_arg = parse::parseRouteArgAs<std::string>(args.at(1));
            if (!feature_address_arg) {
                response.setCode(http::Code::BadRequest);
                response.setHeader(http::Header::ContentType, "application/json");
                response.setHeader(http::Header::ContentLength, "0");
                co_return response;
            }

            const auto feature_address_result = evmc::from_hex<evm::Address>(feature_address_arg.value());

            if (!feature_address_result) {
                response.setCode(http::Code::BadRequest);
                response.setHeader(http::Header::ContentType, "application/json");
                response.setHeader(http::Header::ContentLength, "0");
                co_return response;
            }

            feature_res = co_await registry.getFeature(feature_name, feature_address_result.value());
        } else {
            // /feature/<name>
            feature_res = co_await registry.getNewestFeature(feature_name);
        }

        if (!feature_res) {
            // Feature not found
            response.setCode(http::Code::NotFound);
            response.setHeader(http::Header::ContentType, "application/json");
            response.setHeader(http::Header::ContentLength, "0");
            co_return response;
        }

        // Feature exists
        response.setCode(http::Code::OK);
        response.setHeader(http::Header::Connection, "close");
        response.setHeader(http::Header::ContentType, "application/json");
        response.setHeader(http::Header::ContentLength, "0");
        co_return response;
    }

    asio::awaitable<http::Response> OPTIONS_feature(const http::Request & request, std::vector<server::RouteArg>, server::QueryArgsList)
    {
        http::Response response;
        response.setVersion("HTTP/1.1");

        setCORSHeaders(request, response);

        response.setHeader(http::Header::AccessControlAllowMethods, "HEAD, GET, POST, OPTIONS");
        response.setHeader(http::Header::AccessControlAllowHeaders, "authorization, content-type");
        response.setHeader(http::Header::Connection, "close");
        response.setHeader(http::Header::ContentType, "text/plain");
        response.setHeader(http::Header::AccessControlAllowCredentials, "true");
        response.setCode(http::Code::OK);
        response.setBodyWithContentLength("OK");
        co_return response;
    }

    asio::awaitable<http::Response> GET_feature(const http::Request & request, std::vector<server::RouteArg> args, server::QueryArgsList, registry::Registry & registry, evm::EVM & evm)
    {
        http::Response response;
        response.setVersion("HTTP/1.1");
        response.setHeader(http::Header::Connection, "close");

        setCORSHeaders(request, response);

        if(args.size() > 2 || args.size() == 0)
        {
            response.setCode(http::Code::BadRequest);
            response.setHeader(http::Header::ContentType, "application/json");

            json error_output;
            error_output["error"] = std::format("{}", response.getCode());
            error_output["message"] = "Invalid url";
            response.setBodyWithContentLength(error_output.dump());

            co_return response;
        }

        auto feature_name_result = parse::parseRouteArgAs<std::string>(args.at(0));

        if(!feature_name_result)
        {
            response.setCode(http::Code::BadRequest);
            response.setHeader(http::Header::ContentType, "application/json");

            json error_output;
            error_output["error"] = std::format("{}", response.getCode());
            error_output["message"] = "Invalid url";
            response.setBodyWithContentLength(error_output.dump());

            co_return response;
        }
        const auto & feature_name = feature_name_result.value();

        std::optional<Feature> feature_res;

        if(args.size() == 2)
        {
            const auto feature_address_arg = parse::parseRouteArgAs<std::string>(args.at(1));

            if(!feature_address_arg)
            {
                response.setCode(http::Code::BadRequest);
                response.setHeader(http::Header::ContentType, "application/json");

                json error_output;
                error_output["error"] = std::format("{}", response.getCode());
                error_output["message"] = "Invalid feature address";
                response.setBodyWithContentLength(error_output.dump());

                co_return response;
            }

            const auto feature_address_result = evmc::from_hex<evm::Address>(feature_address_arg.value());

            if(!feature_address_result)
            {
                response.setCode(http::Code::BadRequest);
                response.setHeader(http::Header::ContentType, "application/json");

                json error_output;
                error_output["error"] = std::format("{}", response.getCode());
                error_output["message"] = "Invalid feature address";
                response.setBodyWithContentLength(error_output.dump());

                co_return response;
            }

            feature_res = co_await registry.getFeature(feature_name_result.value(), feature_address_result.value());
        }
        else if(args.size() == 1)
        {
            feature_res = co_await registry.getNewestFeature(feature_name_result.value());
        }

        if(!feature_res) 
        {
            response.setCode(http::Code::NotFound);
            response.setHeader(http::Header::ContentType, "application/json");

            json error_output;
            error_output["error"] = std::format("{}", response.getCode());
            error_output["message"] = "Feature not found";
            response.setBodyWithContentLength(error_output.dump());

            co_return response;
        }
        
        auto json_res = parse::parseToJson(*feature_res, parse::use_json);

        if(!json_res)
        {
            response.setCode(http::Code::InternalServerError);
            response.setHeader(http::Header::ContentType, "application/json");

            json error_output;
            error_output["error"] = std::format("{}", response.getCode());
            error_output["message"] = "Parsing response to json failed";
            response.setBodyWithContentLength(error_output.dump());

            co_return response;
        }

        std::vector<uint8_t> input_data;
        // function selector
        const auto selector = evm::constructSelector("getFeature(string)");
        input_data.insert(input_data.end(), selector.begin(), selector.end());

        // Step 2: Offset to string data (32 bytes with value 0x20)
        std::vector<uint8_t> offset(32, 0);
        offset[31] = 0x20;
        input_data.insert(input_data.end(), offset.begin(), offset.end());

        // Step 3: String length
        std::vector<uint8_t> str_len(32, 0);
        str_len[31] = static_cast<uint8_t>(feature_name.size());
        input_data.insert(input_data.end(), str_len.begin(), str_len.end());

        // Step 4: String bytes
        input_data.insert(input_data.end(), feature_name.begin(), feature_name.end());

        // Step 5: Padding to 32-byte boundary
        size_t padding = (32 - (feature_name.size() % 32)) % 32;
        input_data.insert(input_data.end(), padding, 0);
        
        co_await evm.setGas(evm.getRegistryAddress(), evm::DEFAULT_GAS_LIMIT);
        const auto exec_result = co_await evm.execute(evm.getRegistryAddress(), evm.getRegistryAddress(), input_data, evm::DEFAULT_GAS_LIMIT, 0);

        // check execution status
        if(!exec_result)
        {
            spdlog::error("Failed to fetch feature {}", exec_result.error());
            response.setHeader(http::Header::Connection, "close");
            response.setCode(http::Code::InternalServerError);
            response.setHeader(http::Header::ContentType, "application/json");

            json error_output;
            error_output["error"] = std::format("{}", response.getCode());
            error_output["message"] = std::format("Failed to fetch feature : {}", exec_result.error());
            response.setBodyWithContentLength(error_output.dump());
            
            co_return std::move(response);
        }

        const auto feature_address = evm::decodeReturnedValue<evm::Address>(exec_result.value());
        const auto owner_result = co_await fetchOwner(evm, feature_address);
        if(!owner_result)
        {
            spdlog::error("Failed to fetch owner {}", owner_result.error());
            response.setHeader(http::Header::Connection, "close");
            response.setCode(http::Code::InternalServerError);
            response.setHeader(http::Header::ContentType, "application/json");

            json error_output;
            error_output["error"] = std::format("{}", response.getCode());
            error_output["message"] = std::format("Failed to fetch owner : {}", owner_result.error());
            response.setBodyWithContentLength(error_output.dump());
            
            co_return std::move(response);
        }

        const auto owner_address = evm::decodeReturnedValue<evm::Address>(owner_result.value());

        (*json_res)["owner"] = evmc::hex(owner_address);
        (*json_res)["local_address"] = evmc::hex(feature_address);
        (*json_res)["address"] = "0x0";

        response.setHeader(http::Header::ContentType, "application/json");
        response.setCode(http::Code::OK);
        response.setBodyWithContentLength(json_res->dump());
        co_return std::move(response);
    }

    asio::awaitable<http::Response> POST_feature(const http::Request & request, std::vector<server::RouteArg> args, server::QueryArgsList, AuthManager & auth_manager, registry::Registry & registry, evm::EVM & evm)
    {
        http::Response response;
        response.setVersion("HTTP/1.1");

        setCORSHeaders(request, response);

        const auto auth_result = co_await authenticate(request, auth_manager);

        if(!auth_result)
        {
            response.setHeader(http::Header::Connection, "close");
            response.setCode(http::Code::Unauthorized);
            response.setHeader(http::Header::ContentType, "application/json");

            json error_output;
            error_output["error"] = std::format("{}", response.getCode());
            error_output["message"] = std::format("Error: {}", auth_result.error().kind);
            response.setBodyWithContentLength(error_output.dump());

            co_return std::move(response);
        }
        const auto & address = auth_result.value();

        spdlog::debug(std::format("token verified address : {}", address));

        // parse feature from json_string
        auto feature_res = parse::parseFromJson<Feature>(request.getBody(), parse::use_protobuf);

        if(!feature_res) 
        {
            response.setHeader(http::Header::Connection, "close");
            response.setCode(http::Code::BadRequest);
            response.setHeader(http::Header::ContentType, "application/json");

            json error_output;
            error_output["error"] = std::format("{}", response.getCode());
            error_output["message"] = "Failed to parse feature";
            response.setBodyWithContentLength(error_output.dump());

            co_return std::move(response);
        }

        const Feature & feature = *feature_res;

        FeatureRecord feature_record;
        feature_record.set_owner(evmc::hex(address));
        *feature_record.mutable_feature() = std::move(feature);

        const auto deploy_res = co_await deployFeature(evm, registry, feature_record);
        if(!deploy_res)
        {
            response.setHeader(http::Header::Connection, "close");
            response.setCode(http::Code::BadRequest);
            response.setHeader(http::Header::ContentType, "application/json");

            json error_output;
            error_output["error"] = std::format("{}", response.getCode());
            error_output["message"] = std::format("Failed to deploy feature. Error: {}", deploy_res.error().kind);
            response.setBodyWithContentLength(error_output.dump());

            co_return std::move(response);
        }

        json json_output;
        json_output["name"] = feature_record.feature().name();
        json_output["owner"] = feature_record.owner();
        json_output["local_address"] = evmc::hex(deploy_res.value());
        json_output["address"] = "0x0";

        response.setHeader(http::Header::Connection, "close");
        response.setHeader(http::Header::ContentType, "application/json");
        response.setCode(http::Code::Created);
        response.setBodyWithContentLength(json_output.dump());
        co_return std::move(response);
    }
}