#include "api.hpp"

// TODO
// ABI offset encoding (correctness)
// Add size limits (DoS protection)
// Define a real execution budget / gas policy

namespace dcn
{
    asio::awaitable<http::Response> OPTIONS_execute(const http::Request & request, std::vector<server::RouteArg>, server::QueryArgsList)
    {
        http::Response response;
        response.setCode(http::Code::NoContent)
                .setVersion("HTTP/1.1")
                .setHeader(http::Header::AccessControlAllowOrigin, "*")
                .setHeader(http::Header::AccessControlAllowMethods, "POST, OPTIONS")
                .setHeader(http::Header::AccessControlAllowHeaders, "Authorization, Content-Type")
                .setHeader(http::Header::AccessControlMaxAge, "600")
                .setHeader(http::Header::Connection, "close");
        
        co_return response;
    }

    asio::awaitable<http::Response> POST_execute(const http::Request & request, std::vector<server::RouteArg> args, server::QueryArgsList, const auth::AuthManager & auth_manager, evm::EVM & evm)
    {
        http::Response response;
        response.setCode(http::Code::Unknown)
                .setVersion("HTTP/1.1")
                .setHeader(http::Header::AccessControlAllowOrigin, "*")
                .setHeader(http::Header::ContentType, "application/json")
                .setHeader(http::Header::CacheControl, "no-store")
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

        // parse execution request from json_string
        const auto execute_request_res = parse::parseFromJson<ExecuteRequest>(request.getBody(), parse::use_protobuf);

        if(!execute_request_res) 
        {
            response.setCode(http::Code::BadRequest)
                .setBodyWithContentLength(json {
                    {"message", "Failed to parse execute request"}
                }.dump());

            co_return response;
        }
        const ExecuteRequest & execute_request = *execute_request_res;

        static constexpr uint32_t MAX_SAMPLES_COUNT = 65536;

        if(execute_request.samples_count() > MAX_SAMPLES_COUNT)
        {
            response.setCode(http::Code::BadRequest)
                .setBodyWithContentLength(json {
                    {"message", "samples_count is too large"}
                }.dump());

            co_return response;
        }

        std::vector<uint8_t> input_data;
        // runner function selector
        const auto selector = crypto::constructSelector("gen(string,uint32,(uint32,uint32)[])");
        input_data.insert(input_data.end(), selector.begin(), selector.end());

        // 1. Offset to start of string data
        std::vector<uint8_t> offset_to_string(32, 0);
        offset_to_string[31] = 0x60;
        input_data.insert(input_data.end(), offset_to_string.begin(), offset_to_string.end());

        // 2. uint32 argument, properly encoded as a 32-byte word
        const std::vector<std::uint8_t> samples_count_bytes = evm::encodeAsArg(execute_request.samples_count());
        input_data.insert(input_data.end(), samples_count_bytes.begin(), samples_count_bytes.end());

        // (String encoding)
        const std::vector<std::uint8_t> name_bytes = evm::encodeAsArg(execute_request.particle_name());

        // 3. Offset to vector<tuple>, will be right after string
        std::vector<uint8_t> offset_tuple_vec(32, 0);
        const size_t offset_to_tuple_vec = 0x60 + name_bytes.size();  // string offset + string dynamic payload
        offset_tuple_vec[28] = (offset_to_tuple_vec >> 24) & 0xFF;
        offset_tuple_vec[29] = (offset_to_tuple_vec >> 16) & 0xFF;
        offset_tuple_vec[30] = (offset_to_tuple_vec >> 8) & 0xFF;
        offset_tuple_vec[31] = (offset_to_tuple_vec) & 0xFF;
        input_data.insert(input_data.end(), offset_tuple_vec.begin(), offset_tuple_vec.end());

        // 4. Append the string bytes (dynamic)
        input_data.insert(input_data.end(), name_bytes.begin(), name_bytes.end());
        
        // convert google::protobuf::RepeatedPtrField<dcn::RunningInstance> into vector<tuple<uint32_t, uint32_t>>
        std::vector<std::tuple<std::uint32_t, std::uint32_t>> running_instance;
        for (const auto& running_instance_item : execute_request.running_instances())
        {
            running_instance.push_back(std::make_tuple(running_instance_item.start_point(), running_instance_item.transformation_shift()));
        }

        // 5. Append vector<tuple<uint32_t, uint32_t>> bytes (dynamic)
        const std::vector<uint8_t> tuple_vec_bytes = evm::encodeAsArg(running_instance);
        input_data.insert(input_data.end(), tuple_vec_bytes.begin(), tuple_vec_bytes.end());

        // execute call to runner
        co_await evm.setGas(address, evm::DEFAULT_GAS_LIMIT);
        co_await evm.setGas(evm.getRunnerAddress(), evm::DEFAULT_GAS_LIMIT);
        const auto exec_result = co_await evm.execute(address, evm.getRunnerAddress(), input_data, evm::DEFAULT_GAS_LIMIT, 0);

        // check execution status
        if(!exec_result)
        {
            response.setCode(http::Code::InternalServerError)          
                .setBodyWithContentLength(json {
                    {"message", std::format("Failed to execute code : {}", exec_result.error().kind)}
                }.dump());

            co_return response;
        }

        const auto samples_res = parse::decodeBytes<std::vector<Samples>>(exec_result.value());

        if(!samples_res)
        {
            response.setCode(http::Code::InternalServerError)
                .setBodyWithContentLength(json {
                    {"message", "Failed to decode samples"}
                }.dump());

            co_return response;
        }

        const auto & samples = samples_res.value();
        const auto json_output = parse::parseToJson(samples, parse::use_json);

        if(!json_output)
        {
            response.setCode(http::Code::InternalServerError)
                .setBodyWithContentLength(json {
                    {"message", "Failed to parse json output"}
                }.dump());

            co_return response;
        }

        response.setCode(http::Code::OK)
            .setBodyWithContentLength(json_output->dump());
        
        co_return response;
    }
}