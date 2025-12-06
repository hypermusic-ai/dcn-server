#include "api.hpp"

namespace dcn
{
    asio::awaitable<http::Response> OPTIONS_execute(const http::Request & request, std::vector<server::RouteArg>, server::QueryArgsList)
    {
        http::Response response;
        response.setVersion("HTTP/1.1");

        setCORSHeaders(request, response);

        response.setHeader(http::Header::AccessControlAllowMethods, "GET, POST, OPTIONS");
        response.setHeader(http::Header::AccessControlAllowHeaders, "authorization, content-type");
        response.setHeader(http::Header::Connection, "close");
        response.setHeader(http::Header::ContentType, "text/plain");
        response.setHeader(http::Header::AccessControlAllowCredentials, "true");
        response.setCode(http::Code::OK);
        response.setBodyWithContentLength("OK");
        co_return response;
    }

    asio::awaitable<http::Response> GET_execute(const http::Request & request, std::vector<server::RouteArg> args, server::QueryArgsList, const AuthManager & auth_manager, const registry::Registry & registry, evm::EVM & evm)
    {
        http::Response response;
        response.setVersion("HTTP/1.1");
        response.setHeader(http::Header::Connection, "close");
        
        setCORSHeaders(request, response);

        if(args.size() != 2 && args.size() != 3)
        {
            response.setCode(http::Code::BadRequest);
            response.setHeader(http::Header::ContentType, "application/json");

            json error_output;
            error_output["error"] = std::format("{}", response.getCode());
            error_output["message"] = "invalid url";
            response.setBodyWithContentLength(error_output.dump());

            co_return response;
        }

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

        // input arguments - from url

        // name
        auto feature_name_result = parse::parseRouteArgAs<std::string>(args.at(0));
        if(!feature_name_result)
        {
            spdlog::error("invalid feature name");
            response.setCode(http::Code::BadRequest);
            response.setHeader(http::Header::ContentType, "application/json");

            json error_output;
            error_output["error"] = std::format("{}", response.getCode());
            error_output["message"] = "invalid feature name";
            response.setBodyWithContentLength(error_output.dump());

            co_return response;
        }
        const auto & feature_name = feature_name_result.value();

        // N
        const auto N_result = parse::parseRouteArgAs<std::uint32_t>(args.at(1));
        if(!N_result)
        {
            spdlog::error("invalid number of samples");
            response.setCode(http::Code::BadRequest);
            response.setHeader(http::Header::ContentType, "application/json");

            json error_output;
            error_output["error"] = std::format("{}", response.getCode());
            error_output["message"] = "invalid number of samples";
            response.setBodyWithContentLength(error_output.dump());

            co_return response;
        }
        const std::uint32_t & N = N_result.value();

        // running instaces
        std::vector<std::tuple<std::uint32_t, std::uint32_t>> running_instances;
        if(args.size() == 3)
        {
            auto running_instances_result = parse::parseRouteArgAs<std::vector<std::tuple<std::uint32_t, std::uint32_t>>>(args.at(2));
            if(!running_instances_result)
            {
                spdlog::error("invalid running instaces");
                response.setCode(http::Code::BadRequest);
                response.setHeader(http::Header::ContentType, "application/json");

                json error_output;
                error_output["error"] = std::format("{}", response.getCode());
                error_output["message"] = "invalid running instaces";
                response.setBodyWithContentLength(error_output.dump());

                co_return response;
            }
            running_instances = running_instances_result.value();
        }

        std::vector<uint8_t> input_data;
        // runner function selector
        const auto selector = evm::constructSelector("gen(string,uint32,(uint32,uint32)[])");
        input_data.insert(input_data.end(), selector.begin(), selector.end());

        // 1. Offset to start of string data
        std::vector<uint8_t> offset_to_string(32, 0);
        offset_to_string[31] = 0x60;
        input_data.insert(input_data.end(), offset_to_string.begin(), offset_to_string.end());

        // 2. uint32 argument, properly encoded as a 32-byte word
        std::vector<std::uint8_t> N_bytes = evm::encodeAsArg(N);
        input_data.insert(input_data.end(), N_bytes.begin(), N_bytes.end());

        // (String encoding)
        std::vector<std::uint8_t> name_bytes = evm::encodeAsArg(feature_name);

        // 3. Offset to vector<tuple>, will be right after string
        std::vector<uint8_t> offset_tuple_vec(32, 0);
        size_t offset_to_tuple_vec = 0x60 + name_bytes.size();  // string offset + string dynamic payload
        offset_tuple_vec[28] = (offset_to_tuple_vec >> 24) & 0xFF;
        offset_tuple_vec[29] = (offset_to_tuple_vec >> 16) & 0xFF;
        offset_tuple_vec[30] = (offset_to_tuple_vec >> 8) & 0xFF;
        offset_tuple_vec[31] = (offset_to_tuple_vec) & 0xFF;
        input_data.insert(input_data.end(), offset_tuple_vec.begin(), offset_tuple_vec.end());

        // 4. Append the string bytes (dynamic)
        input_data.insert(input_data.end(), name_bytes.begin(), name_bytes.end());
        
        // 5. Append vector<tuple<uint32_t, uint32_t>> bytes (dynamic)
        std::vector<uint8_t> tuple_vec_bytes = evm::encodeAsArg(running_instances);
        input_data.insert(input_data.end(), tuple_vec_bytes.begin(), tuple_vec_bytes.end());

        // execute call to runner
        co_await evm.setGas(address, evm::DEFAULT_GAS_LIMIT);
        co_await evm.setGas(evm.getRunnerAddress(), evm::DEFAULT_GAS_LIMIT);
        const auto exec_result = co_await evm.execute(address, evm.getRunnerAddress(), input_data, evm::DEFAULT_GAS_LIMIT, 0);

        // check execution status
        if(!exec_result)
        {
            spdlog::error("Failed to execute code {}", exec_result.error());
            response.setHeader(http::Header::Connection, "close");
            response.setCode(http::Code::InternalServerError);
            response.setHeader(http::Header::ContentType, "application/json");

            json error_output;
            error_output["error"] = std::format("{}", response.getCode());
            error_output["message"] = std::format("Failed to execute code : {}", exec_result.error());
            response.setBodyWithContentLength(error_output.dump());

            co_return std::move(response);
        }

        auto samples = evm::decodeReturnedValue<std::vector<Samples>>(exec_result.value());

        auto json_output = parse::parseToJson(samples, parse::use_json);
        if(!json_output)
        {
            spdlog::error("Failed to parse json output");
            response.setHeader(http::Header::Connection, "close");
            response.setCode(http::Code::InternalServerError);
            response.setHeader(http::Header::ContentType, "application/json");

            json error_output;
            error_output["error"] = std::format("{}", response.getCode());
            error_output["message"] = "Failed to parse json output";
            response.setBodyWithContentLength(error_output.dump());

            co_return std::move(response);
        }

        response.setHeader(http::Header::Connection, "close");
        response.setHeader(http::Header::ContentType, "application/json");
        response.setCode(http::Code::OK);
        response.setBodyWithContentLength(json_output->dump());
        co_return std::move(response);
    }
}