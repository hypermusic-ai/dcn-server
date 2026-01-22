#include "api.hpp"

// TODO
// ABI offset encoding (correctness)
// Add size limits (DoS protection)
// Define a real execution budget / gas policy

namespace dcn
{
    asio::awaitable<http::Response> HEAD_particle(const http::Request & request, std::vector<server::RouteArg> args, server::QueryArgsList, registry::Registry & registry)
    {
        http::Response response;
        response.setCode(http::Code::Unknown)
                .setVersion("HTTP/1.1")
                .setHeader(http::Header::AccessControlAllowOrigin, "*")
                .setHeader(http::Header::CacheControl, "no-store")
                .setHeader(http::Header::ContentLength, "0")
                .setHeader(http::Header::Connection, "close");

        // Validate path: /particle/<name> or /particle/<name>/<address>
        if (args.size() > 2 || args.size() == 0) {
            response.setCode(http::Code::BadRequest);
            co_return response;
        }

        const auto particle_name_result = parse::parseRouteArgAs<std::string>(args.at(0));
        if (!particle_name_result) {
            response.setCode(http::Code::BadRequest);
            co_return response;
        }
        const auto & particle_name = particle_name_result.value();

        std::optional<Particle> particle_res;

        if (args.size() == 2) {
            // /particle/<name>/<address>
            const auto particle_address_arg = parse::parseRouteArgAs<std::string>(args.at(1));
            if (!particle_address_arg) {
                response.setCode(http::Code::BadRequest);
                co_return response;
            }

            const auto particle_address_result = evmc::from_hex<evm::Address>(particle_address_arg.value());

            if (!particle_address_result) {
                response.setCode(http::Code::BadRequest);
                co_return response;
            }

            particle_res = co_await registry.getParticle(particle_name, particle_address_result.value());
        } else {
            // /particle/<name>
            particle_res = co_await registry.getNewestParticle(particle_name);
        }

        if (!particle_res) {
            // particle not found
            response.setCode(http::Code::NotFound);
            co_return response;
        }

        // particle exists
        response.setCode(http::Code::OK);
        co_return response;
    }

    asio::awaitable<http::Response> OPTIONS_particle(const http::Request & request, std::vector<server::RouteArg>, server::QueryArgsList)
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

    asio::awaitable<http::Response> GET_particle(const http::Request & request, std::vector<server::RouteArg> args, server::QueryArgsList, registry::Registry & registry, evm::EVM & evm)
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

        auto particle_name_result = parse::parseRouteArgAs<std::string>(args.at(0));

        if(!particle_name_result)
        {
            response.setCode(http::Code::BadRequest)
                .setBodyWithContentLength(json{
                    {"message", "Invalid particle name"}
                }.dump());

            co_return response;
        }
        const auto & particle_name = particle_name_result.value();

        std::optional<Particle> particle_res;

        if(args.size() == 2)
        {
            const auto particle_address_arg = parse::parseRouteArgAs<std::string>(args.at(1));

            if(!particle_address_arg)
            {
                response.setCode(http::Code::BadRequest)
                    .setBodyWithContentLength(json {
                        {"message", "Invalid particle address argument"}
                    }.dump());

                co_return response;
            }

            const auto particle_address_result = evmc::from_hex<evm::Address>(particle_address_arg.value());

            if(!particle_address_result)
            {
                response.setCode(http::Code::BadRequest)
                    .setBodyWithContentLength(json{
                        {"message", "Invalid particle address argument value"}
                    }.dump());

                co_return response;
            }

            particle_res = co_await registry.getParticle(particle_name_result.value(), particle_address_result.value());
        }
        else if(args.size() == 1)
        {
            particle_res = co_await registry.getNewestParticle(particle_name_result.value());
        }

        if(!particle_res) 
        {
            response.setCode(http::Code::NotFound)
                .setBodyWithContentLength(json {
                    {"message", "Particle not found"}
                }.dump());

            co_return response;
        }
        
        auto json_res = parse::parseToJson(*particle_res, parse::use_json);

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
        const auto selector = evm::constructSelector("getParticle(string)");
        input_data.insert(input_data.end(), selector.begin(), selector.end());

        // Step 2: Offset to string data (32 bytes with value 0x20)
        std::vector<uint8_t> offset(32, 0);
        offset[31] = 0x20;
        input_data.insert(input_data.end(), offset.begin(), offset.end());

        // Step 3: String length
        std::vector<uint8_t> str_len(32, 0);
        str_len[31] = static_cast<uint8_t>(particle_name.size());
        input_data.insert(input_data.end(), str_len.begin(), str_len.end());

        // Step 4: String bytes
        input_data.insert(input_data.end(), particle_name.begin(), particle_name.end());

        // Step 5: Padding to 32-byte boundary
        const std::size_t padding = (32 - (particle_name.size() % 32)) % 32;
        input_data.insert(input_data.end(), padding, 0);
        
        co_await evm.setGas(evm.getRegistryAddress(), evm::DEFAULT_GAS_LIMIT);
        const auto exec_result = co_await evm.execute(evm.getRegistryAddress(), evm.getRegistryAddress(), input_data, evm::DEFAULT_GAS_LIMIT, 0);

        // check execution status
        if(!exec_result)
        {
            response.setCode(http::Code::InternalServerError)
                .setBodyWithContentLength(json {
                    {"message", std::format("Failed to fetch particle : {}", exec_result.error().kind)}
                }.dump());
            
            co_return response;
        }

        const auto particle_address = evm::decodeReturnedValue<evm::Address>(exec_result.value());
        const auto owner_result = co_await fetchOwner(evm, particle_address);
        if(!owner_result)
        {
            response.setCode(http::Code::InternalServerError)
                .setBodyWithContentLength(json {
                    {"message", std::format("Failed to fetch owner : {}", owner_result.error().kind)}
                }.dump());
            
            co_return response;
        }

        const auto owner_address = evm::decodeReturnedValue<evm::Address>(owner_result.value());

        (*json_res)["owner"] = evmc::hex(owner_address);
        (*json_res)["local_address"] = evmc::hex(particle_address);
        (*json_res)["address"] = "0x0";

        response.setCode(http::Code::OK)
            .setBodyWithContentLength(json_res->dump());

        co_return response;
    }

    asio::awaitable<http::Response> POST_particle(const http::Request & request, std::vector<server::RouteArg> args, server::QueryArgsList,
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

        // parse particle from json_string
        const auto particle_res = parse::parseFromJson<Particle>(request.getBody(), parse::use_protobuf);

        if(!particle_res) 
        {
            response.setCode(http::Code::BadRequest)
                .setBodyWithContentLength(json {
                    {"message", "Failed to parse particle"}
                }.dump());

            co_return response;
        }

        const Particle & particle = *particle_res;

        ParticleRecord particle_record;
        particle_record.set_owner(evmc::hex(address));
        *particle_record.mutable_particle() = std::move(particle);

        const auto deploy_res = co_await loader::deployParticle(evm, registry, particle_record, config.storage_path);
        if(!deploy_res)
        {
            response.setCode(http::Code::BadRequest)
                .setBodyWithContentLength(json {
                    {"message", std::format("Failed to deploy particle. Error: {}", deploy_res.error().kind)}
                }.dump());

            co_return response;
        }

        json json_output;
        json_output["name"] = particle_record.particle().name();
        json_output["owner"] = particle_record.owner();
        json_output["local_address"] = evmc::hex(deploy_res.value());
        json_output["address"] = "0x0";

        response.setCode(http::Code::Created)
            .setBodyWithContentLength(json_output.dump());

        co_return response;
    }
}