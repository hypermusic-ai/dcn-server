#include "loader.hpp"

#include <array>
#include <type_traits>

namespace dcn::loader
{
    namespace
    {
        constexpr const char* PT_BUILD_VERSION = "uups-v1";

        const std::array<std::string, 4> PT_STORAGE_ENTITY_DIRS{
            "particles",
            "features",
            "transformations",
            "conditions"
        };
    }

    template<class T>
    static absl::flat_hash_map<std::string, T> _loadJSONRecords(std::filesystem::path dir)
    {
        absl::flat_hash_map<std::string, T> loaded_data;
        parse::Result<T> loaded_result;
        
        bool success = true;

        try {
            for (const auto& entry : std::filesystem::directory_iterator(dir)) 
            {
                if (!entry.is_regular_file() || entry.path().extension() != ".json") continue;
                
                spdlog::debug(std::format("Found JSON file: {}", entry.path().string()));

                std::ifstream file(entry.path());

                if (!file.is_open()) {
                    spdlog::error(std::format("Failed to open file: {}", entry.path().string()));
                    continue;
                }

                std::string json((std::istreambuf_iterator<char>(file)), std::istreambuf_iterator<char>());
                file.close();

                loaded_result = parse::parseFromJson<T>(json, parse::use_protobuf);
                if(!loaded_result)
                {
                    spdlog::error(std::format("Failed to parse JSON: {}", json));
                    continue;
                }

                loaded_data.try_emplace(entry.path().stem().string(), std::move(*loaded_result));
            }
        } 
        catch (const std::filesystem::filesystem_error& e) 
        {
            spdlog::error(std::format("Filesystem error: {}", e.what()));
            success = false;
        } 
        catch (const std::exception& e) 
        {
            spdlog::error(std::format("Exception: {}", e.what()));
            success = false;
        }

        if(!success)
        {
            return {};
        }

        return loaded_data;
    }

    static void _removeFileNoThrow(const std::filesystem::path & file_path)
    {
        std::error_code ec;
        std::filesystem::remove(file_path, ec);
        if(ec)
        {
            spdlog::debug("Failed to remove '{}': {}", file_path.string(), ec.message());
        }
    }

    static void _loadCleanup(const std::filesystem::path & out_dir, const std::string & name)
    {
        // remove binary file
        _removeFileNoThrow(out_dir / (name + ".bin"));
        _removeFileNoThrow(out_dir / (name + ".abi"));
    }

    bool ensurePTBuildVersion(const std::filesystem::path & storage_path)
    {
        const std::filesystem::path marker_path = storage_path / "pt_solidity_build_version";

        std::string current_version;
        if(std::filesystem::exists(marker_path))
        {
            std::ifstream marker_in(marker_path);
            if(marker_in.is_open())
            {
                std::getline(marker_in, current_version);
            }
        }

        if(current_version == PT_BUILD_VERSION)
        {
            spdlog::debug("PT Solidity build cache version is up-to-date: {}", PT_BUILD_VERSION);
            return true;
        }

        spdlog::info(
            "PT Solidity build cache version changed from '{}' to '{}'. Cleaning stale build artifacts.",
            current_version.empty() ? "<none>" : current_version,
            PT_BUILD_VERSION
        );

        try
        {
            for(const std::string & entity_dir : PT_STORAGE_ENTITY_DIRS)
            {
                const std::filesystem::path build_dir = storage_path / entity_dir / "build";
                std::size_t removed_count = 0;

                if(std::filesystem::exists(build_dir))
                {
                    for(const auto & entry : std::filesystem::directory_iterator(build_dir))
                    {
                        if(!entry.is_regular_file())
                        {
                            continue;
                        }

                        const std::filesystem::path file_path = entry.path();
                        const std::string ext = file_path.extension().string();
                        if(ext == ".bin" || ext == ".abi")
                        {
                            std::filesystem::remove(file_path);
                            ++removed_count;
                        }
                    }
                }

                spdlog::info("PT build cleanup '{}': removed {} cached artifacts", entity_dir, removed_count);
            }
        }
        catch(const std::filesystem::filesystem_error & e)
        {
            spdlog::error("Failed to cleanup PT build artifacts: {}", e.what());
            return false;
        }

        std::ofstream marker_out(marker_path, std::ios::out | std::ios::trunc);
        if(!marker_out.is_open())
        {
            spdlog::error("Failed to write PT build version marker '{}'", marker_path.string());
            return false;
        }

        marker_out << PT_BUILD_VERSION;
        marker_out.close();

        spdlog::info("PT Solidity build cache marker updated: {}", PT_BUILD_VERSION);
        return true;
    }

    static asio::awaitable<bool> _ensurePTContractProxyBin(evm::EVM & evm)
    {
        const std::filesystem::path proxy_out_dir = evm.getPTPath() / "out" / "proxy";
        const std::filesystem::path proxy_bin_path = proxy_out_dir / "PTContractProxy.bin";

        if(std::filesystem::exists(proxy_bin_path))
        {
            co_return true;
        }

        if(!co_await evm.compile(
            evm.getPTPath() / "contracts" / "proxy" / "PTContractProxy.sol",
            proxy_out_dir,
            evm.getPTPath() / "contracts",
            evm.getPTPath() / "node_modules"))
        {
            spdlog::error("Failed to compile PT contract proxy");
            co_return false;
        }

        co_return true;
    }

    template<class RecordType>
    static asio::awaitable<bool> _saveJsonRecord(const std::string & name, RecordType record, const std::filesystem::path out_dir)
    {
        if(std::filesystem::exists(out_dir) == false)
        {
            spdlog::error("Directory {} does not exists", out_dir.string());
            co_return false;
        }

        const auto parsing_result = parse::parseToJson(record, parse::use_protobuf);
        if(!parsing_result)
        {
            spdlog::error("Failed to parse record `{}`", name);
            co_return false;
        }

        std::ofstream output_file(out_dir / (name + ".json"), std::ios::out | std::ios::trunc);

        output_file << *parsing_result;
        output_file.close();

        co_return true;
    }

    // template<class RecordType, class CodeGeneratorFunc>
    // static asio::awaitable<bool> _compileRecord(const std::string & name, RecordType record, 
    //     CodeGeneratorFunc && constructSolidityCode,
    //     const std::filesystem::path bin_dir)
    // {
    //     // if binary file does not exist
    //     if(std::filesystem::exists(bin_dir / (name + ".bin")) == false)
    //     {
    //         // there is a need for compile code
    //         const std::filesystem::path code_path = bin_dir / (name + ".sol");

    //         // create code file
    //         std::ofstream out_file(code_path); 
    //         if(!out_file.is_open())
    //         {
    //             spdlog::error("Failed to create file");
    //             co_return std::unexpected(evm::DeployError{});
    //         }

    //         out_file << constructSolidityCode(record.particle());
    //         out_file.close();

    //         // compile code
    //         if(!co_await evm.compile(code_path, bin_dir, file::getPTPath()/ "contracts"))
    //         {
    //             spdlog::error("Failed to compile code");
    //             // remove code file
    //             std::filesystem::remove(code_path);
    //             co_return std::unexpected(evm::DeployError{evm::DeployError::Kind::COMPILATION_ERROR});
    //         }

    //         // remove code file
    //         std::filesystem::remove(code_path);
    //     }
    // }



    template<class T, class InternalGetter, class SolidityCodeCtor>
    static asio::awaitable<std::expected<chain::Address, pt::PTDeployError>> _deployObjectLocally(evm::EVM & evm, 
        registry::Registry & registry, T object, InternalGetter getter,
        const std::filesystem::path out_dir, SolidityCodeCtor solidity_code_ctor, pt::PTDeployError::Kind expected_conflict_error
    )
    {
        using Internal_t = std::invoke_result_t<InternalGetter, T>;
        const Internal_t & internal = std::invoke(getter, object);

        if(internal.name().empty())
        {
            spdlog::error("Object name is empty");
            co_return std::unexpected(pt::PTDeployError{
                .kind = pt::PTDeployError::Kind::INVALID_INPUT});
        }

        const std::string name = internal.name();

        const auto address_result = evmc::from_hex<chain::Address>(object.owner());
        if(!address_result)
        {
            spdlog::error("Failed to parse address");
            co_return std::unexpected(pt::PTDeployError{pt::PTDeployError::Kind::INVALID_INPUT});
        }
        const auto & address = *address_result;

        const std::filesystem::path bin_dir = out_dir / "build";

        if(std::filesystem::exists(bin_dir) == false)
        {
            spdlog::error("Directory {} does not exists", bin_dir.string());
            co_return std::unexpected(pt::PTDeployError{});
        }

        // if binary file does not exist
        if(std::filesystem::exists(bin_dir / (name + ".bin")) == false)
        {
            // there is a need for compile code
            const std::filesystem::path code_path = bin_dir / (name + ".sol");

            // create code file
            std::ofstream out_file(code_path); 
            if(!out_file.is_open())
            {
                spdlog::error("Failed to create file");
                co_return std::unexpected(pt::PTDeployError{});
            }
            
            out_file << std::invoke(std::forward<SolidityCodeCtor>(solidity_code_ctor), internal);
            out_file.close();

            // compile code
            if(!co_await evm.compile(code_path, bin_dir, evm.getPTPath() / "contracts", evm.getPTPath() / "node_modules"))
            {
                spdlog::error("Failed to compile code");
                // remove code file
                _removeFileNoThrow(code_path);
                co_return std::unexpected(pt::PTDeployError{ pt::PTDeployError::Kind::INVALID_INPUT});
            }

            // remove code file
            _removeFileNoThrow(code_path);
        }

        co_await evm.addAccount(address, evm::DEFAULT_GAS_LIMIT);
        co_await evm.setGas(address, evm::DEFAULT_GAS_LIMIT);
        
        auto implementation_deploy_res = co_await evm.deploy(
            bin_dir / (name + ".bin"),
            address, 
            {},
            evm::DEFAULT_GAS_LIMIT, 
            0);


        if(!implementation_deploy_res)
        {
            spdlog::error(std::format("Failed to deploy object implementation: {}", implementation_deploy_res.error().kind));
            _loadCleanup(bin_dir, name);

            const auto & error = implementation_deploy_res.error();
            const auto pt_error = parse::decodeBytes<pt::PTDeployError>(error.result_bytes);

            if(!pt_error)
            {
                spdlog::error(std::format("Failed to parse PTDeployError: {}", pt_error.error().kind));
                co_return std::unexpected(pt::PTDeployError{});
            }

            co_return std::unexpected(pt_error.value());
        }

        const auto implementation_address = implementation_deploy_res.value();
        spdlog::info("Object implementation address '{}': {}", name, evmc::hex(implementation_address));

        if(!co_await _ensurePTContractProxyBin(evm))
        {
            spdlog::error("Failed to ensure PTContractProxy.bin");
            _loadCleanup(bin_dir, name);
            co_return std::unexpected(pt::PTDeployError{pt::PTDeployError::Kind::INVALID_INPUT});
        }

        std::vector<std::uint8_t> proxy_ctor_args = evm::encodeAsArg(implementation_address);
        const auto registry_arg = evm::encodeAsArg(evm.getRegistryAddress());
        proxy_ctor_args.insert(proxy_ctor_args.end(), registry_arg.begin(), registry_arg.end());

        auto proxy_deploy_res = co_await evm.deploy(
            evm.getPTPath() / "out" / "proxy" / "PTContractProxy.bin",
            address,
            std::move(proxy_ctor_args),
            evm::DEFAULT_GAS_LIMIT,
            0);

        if(!proxy_deploy_res)
        {
            spdlog::error(std::format("Failed to deploy object proxy : {}", proxy_deploy_res.error().kind));

            const auto & error = proxy_deploy_res.error();

            const auto pt_error = parse::decodeBytes<pt::PTDeployError>(error.result_bytes);

            if(!pt_error)
            {
                spdlog::error(std::format("Failed to parse PTDeployError: {}", pt_error.error().kind));
                co_return std::unexpected(pt::PTDeployError{});
            }

            // deploy can fail in case when this name is already taken - then we don't want to remove binary file
            // if it fails for another reason - we want to remove binary file
            if(pt_error->kind != expected_conflict_error)
            {
                // remove binary file
                _loadCleanup(bin_dir, name);
            }
            co_return std::unexpected(pt_error.value());
        }

        const auto object_proxy_address = proxy_deploy_res.value();
        spdlog::info("Object proxy address '{}': {}", name, evmc::hex(object_proxy_address));

        const auto owner_result = co_await fetchOwner(evm, object_proxy_address);

        // check execution status
        if(!owner_result)
        {
            spdlog::error(std::format("Failed to fetch owner {}", owner_result.error().kind));

            // remove binary file
            _loadCleanup(bin_dir, name);

            co_return std::unexpected(pt::PTDeployError{});
        }

        const auto owner_address_res = chain::readAddressWord(owner_result.value());

        if(!owner_address_res)
        {
            spdlog::error("Failed to parse owner address");

            // remove binary file
            _loadCleanup(bin_dir, name);

            co_return std::unexpected(pt::PTDeployError{pt::PTDeployError::Kind::INVALID_INPUT});
        }

        const auto & owner_address = owner_address_res.value();

        if(owner_address != address)
        {
            spdlog::error("Owner address mismatch");

            // remove binary file
            _loadCleanup(bin_dir, name);

            co_return std::unexpected(pt::PTDeployError{pt::PTDeployError::Kind::INVALID_INPUT});
        }

        if(!co_await registry.add(object_proxy_address, object))
        {
            spdlog::error("Failed to add object '{}'", name);

            // remove binary file
            _loadCleanup(bin_dir, name);

            co_return std::unexpected(pt::PTDeployError{});
        }
        
        if(!co_await _saveJsonRecord(name, std::move(object), out_dir))
        {
            spdlog::error("Failed to save object json '{}'", name);

            // remove binary file
            _loadCleanup(bin_dir, name);

            co_return std::unexpected(pt::PTDeployError{});
        }

        spdlog::debug("Object '{}' added", name);
        co_return object_proxy_address;
    }


    asio::awaitable<std::expected<chain::Address, pt::PTDeployError>> deployParticle(evm::EVM & evm, registry::Registry & registry, ParticleRecord particle_record, const std::filesystem::path & storage_path)
    {
        return _deployObjectLocally(evm,
            registry, 
            std::move(particle_record), 
            &ParticleRecord::particle, 
            storage_path / "particles",
            &constructParticleSolidityCode,
            pt::PTDeployError::Kind::PARTICLE_ALREADY_REGISTERED);
    }

    asio::awaitable<std::expected<chain::Address, pt::PTDeployError>> deployFeature(evm::EVM & evm, registry::Registry & registry, FeatureRecord feature_record, const std::filesystem::path & storage_path)
    {
        return _deployObjectLocally(evm,
            registry, 
            std::move(feature_record), 
            &FeatureRecord::feature, 
            storage_path / "features",
            &constructFeatureSolidityCode,
            pt::PTDeployError::Kind::FEATURE_ALREADY_REGISTERED);
    }

    asio::awaitable<std::expected<chain::Address, pt::PTDeployError>> deployTransformation(evm::EVM & evm, registry::Registry & registry, TransformationRecord transformation_record, const std::filesystem::path & storage_path)
    {
        return _deployObjectLocally(evm,
            registry, 
            std::move(transformation_record), 
            &TransformationRecord::transformation, 
            storage_path / "transformations",
            &constructTransformationSolidityCode,
            pt::PTDeployError::Kind::TRANSFORMATION_ALREADY_REGISTERED);
    }


    asio::awaitable<std::expected<chain::Address, pt::PTDeployError>> deployCondition(evm::EVM & evm, registry::Registry & registry, ConditionRecord condition_record, const std::filesystem::path & storage_path)
    {
        return _deployObjectLocally(evm,
            registry, 
            std::move(condition_record), 
            &ConditionRecord::condition, 
            storage_path / "conditions",
            &constructConditionSolidityCode,
            pt::PTDeployError::Kind::CONDITION_ALREADY_REGISTERED);
    }


    asio::awaitable<bool> loadStoredParticles(evm::EVM & evm, registry::Registry & registry, const std::filesystem::path & storage_path)
    {
        spdlog::info("Loading stored particles...");

        auto loaded_particles = _loadJSONRecords<ParticleRecord>(storage_path / "particles");
        if(loaded_particles.empty())
        {
            co_return false;
        }

        const auto sorted_particles = utils::topologicalSort<ParticleRecord, std::string, std::vector>(
                loaded_particles,
                [](const ParticleRecord & record)
                {
                    std::vector<std::string> composites;
                    composites.reserve(record.particle().composites().size());
                    for(const auto & [_, composite] : record.particle().composites())
                    {
                        composites.push_back(composite);
                    }
                    return composites;
                },
                [](const std::string & composite) {return composite;}
            );

        bool success = true;
        std::size_t i = 0;
        const std::size_t batch_size = (sorted_particles.size() / 100) + 1;
        assert(batch_size > 0);
        
        for(const auto & name : sorted_particles)
        {
            if(!co_await deployParticle(evm, registry, std::move(loaded_particles.at(name)), storage_path))
            {
                spdlog::error("Failed to deploy particle `{}`", name);
                success = false;
            }

            if(++i % batch_size == 0) {spdlog::debug("{}/{} particles loaded", i, loaded_particles.size());}
        }

        co_return success;
    }


    asio::awaitable<bool> loadStoredFeatures(evm::EVM & evm, registry::Registry & registry, const std::filesystem::path & storage_path)
    {
        spdlog::info("Loading stored features...");

        auto loaded_features = _loadJSONRecords<FeatureRecord>(storage_path / "features");
        if(loaded_features.empty())
        {
            co_return false;
        }

        bool success = true;
        std::size_t i = 0;
        const std::size_t batch_size = (loaded_features.size() / 100) + 1;
        assert(batch_size > 0);
        
        for(const auto & [name, feature] : loaded_features)
        {
            if(!co_await deployFeature(evm, registry, std::move(feature), storage_path))
            {
                spdlog::error("Failed to deploy feature `{}`", name);
                success = false;
            }

            if(++i % batch_size == 0) {spdlog::debug("{}/{} features loaded", i, loaded_features.size());}
        }

        co_return success;
    }

    asio::awaitable<bool> loadStoredTransformations(evm::EVM & evm, registry::Registry & registry, const std::filesystem::path & storage_path)
    {
        spdlog::info("Loading stored transformations...");

        auto loaded_transformations = _loadJSONRecords<TransformationRecord>(storage_path / "transformations");
        if(loaded_transformations.empty())
        {
            co_return false;
        }

        bool success = true;
        std::size_t i = 0;
        const std::size_t batch_size = (loaded_transformations.size() / 100) + 1;
        assert(batch_size > 0);

        for(auto & [name, transformation] : loaded_transformations)
        {
            if(!co_await deployTransformation(evm, registry, std::move(transformation), storage_path))
            {
                spdlog::error("Failed to deploy transformation `{}`", name);
                success = false;
            }

            if(++i % batch_size == 0) {spdlog::debug("{}/{} transformations loaded", i, loaded_transformations.size());}
        }

        co_return success;
    }

    asio::awaitable<bool> loadStoredConditions(evm::EVM & evm, registry::Registry & registry, const std::filesystem::path & storage_path)
    {
        spdlog::info("Loading stored conditions...");

        auto loaded_conditions = _loadJSONRecords<ConditionRecord>(storage_path / "conditions");
        if(loaded_conditions.empty())
        {
            co_return false;
        }

        bool success = true;
        std::size_t i = 0;
        const std::size_t batch_size = (loaded_conditions.size() / 100) + 1;
        assert(batch_size > 0);

        for(auto & [name, condition] : loaded_conditions)
        {
            if(!co_await deployCondition(evm, registry, std::move(condition), storage_path))
            {
                spdlog::error("Failed to deploy condition `{}`", name);
                success = false;
            }

            if(++i % batch_size == 0) {spdlog::debug("{}/{} conditions loaded", i, loaded_conditions.size());}
        }

        co_return success;
    }

}
