#include "loader.hpp"

namespace dcn::loader
{
    template<class T>
    static absl::flat_hash_map<std::string, T> _loadJSONRecords(std::filesystem::path dir)
    {
        std::ifstream file;

        absl::flat_hash_map<std::string, T> loaded_data;
        parse::Result<T> loaded_result;
        
        bool success = true;

        try {
            for (const auto& entry : std::filesystem::directory_iterator(dir)) 
            {
                if (!entry.is_regular_file() || entry.path().extension() != ".json") continue;
                
                spdlog::debug(std::format("Found JSON file: {}", entry.path().string()));
                file.open(entry.path());

                if (!file.is_open()) {
                    spdlog::error(std::format("Failed to open file: {}", entry.path().string()));
                    continue;
                }

                std::string json((std::istreambuf_iterator<char>(file)), std::istreambuf_iterator<char>());

                loaded_result = parse::parseFromJson<T>(json, parse::use_protobuf);
                if(!loaded_result)
                {
                    spdlog::error(std::format("Failed to parse JSON: {}", json));
                    continue;
                }

                loaded_data.try_emplace(entry.path().stem().string(), std::move(*loaded_result));

                file.close();
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

    asio::awaitable<std::expected<evm::Address, evm::DeployError>> deployFeature(evm::EVM & evm, registry::Registry & registry, FeatureRecord feature_record)
    {
        if(feature_record.feature().name().empty())
        {
            spdlog::error("Feature name is empty");
            co_return std::unexpected(evm::DeployError{evm::DeployError::Kind::INVALID_DATA});
        }

        if(co_await registry.checkIfSubFeaturesExist(feature_record.feature()) == false)
        {
            spdlog::error("Cannot find subfeatures for feature");
            co_return std::unexpected(evm::DeployError{evm::DeployError::Kind::FEATURE_MISSING});
        }

        const auto address_result = evmc::from_hex<evm::Address>(feature_record.owner());
        if(!address_result)
        {
            spdlog::error("Failed to parse address");
            co_return std::unexpected(evm::DeployError{evm::DeployError::Kind::INVALID_ADDRESS});
        }
        const auto & address = *address_result;

        static const std::filesystem::path out_dir = file::getStoragePath() / "features" / "build";

        if(std::filesystem::exists(out_dir) == false)
        {
            spdlog::error("Directory {} does not exists", out_dir.string());
            co_return std::unexpected(evm::DeployError{});
        }

        // if binary file does not exist
        if(std::filesystem::exists(out_dir / (feature_record.feature().name() + ".bin")) == false)
        {
            // there is a need for compile code
            const std::filesystem::path code_path = out_dir / (feature_record.feature().name() + ".sol");

            // create code file
            std::ofstream out_file(code_path); 
            if(!out_file.is_open())
            {
                spdlog::error("Failed to create file");
                co_return std::unexpected(evm::DeployError{});
            }

            out_file << constructFeatureSolidityCode(feature_record.feature());
            out_file.close();

            // compile code
            if(!co_await evm.compile(code_path, out_dir, file::getPTPath()/ "contracts"))
            {
                spdlog::error("Failed to compile code");
                // remove code file
                std::filesystem::remove(code_path);
                co_return std::unexpected(evm::DeployError{evm::DeployError::Kind::COMPILATION_ERROR});
            }

            // remove code file
            std::filesystem::remove(code_path);
        }

        co_await evm.addAccount(address, evm::DEFAULT_GAS_LIMIT);
        co_await evm.setGas(address, evm::DEFAULT_GAS_LIMIT);
        
        auto deploy_res = co_await evm.deploy(
            out_dir / (feature_record.feature().name() + ".bin"), 
            address, 
            evm::encodeAsArg(evm.getRegistryAddress()),
            evm::DEFAULT_GAS_LIMIT, 
            0);

        if(!deploy_res)
        {
            spdlog::error(std::format("Failed to deploy code : {}", deploy_res.error().kind));

            // deploy can fail in case when this name is already taken - then we don't want to remove binary file
            // if it fails for another reason - we want to remove binary file
            if(deploy_res.error().kind != evm::DeployError::Kind::FEATURE_ALREADY_REGISTERED)
            {
                // remove binary file
                std::filesystem::remove(out_dir / (feature_record.feature().name() + ".bin"));
                std::filesystem::remove(out_dir / (feature_record.feature().name() + ".abi"));
            }
            co_return std::unexpected(deploy_res.error());
        }

        const auto owner_result = co_await fetchOwner(evm, deploy_res.value());

        // check execution status
        if(!owner_result)
        {
            spdlog::error(std::format("Failed to fetch owner {}", owner_result.error().kind));

            // remove binary file
            std::filesystem::remove(out_dir / (feature_record.feature().name() + ".bin"));
            std::filesystem::remove(out_dir / (feature_record.feature().name() + ".abi"));

            co_return std::unexpected(evm::DeployError{});
        }

        const auto owner_address = evm::decodeReturnedValue<evm::Address>(owner_result.value());

        if(owner_address != address)
        {
            spdlog::error("Owner address mismatch");

            // remove binary file
            std::filesystem::remove(out_dir / (feature_record.feature().name() + ".bin"));
            std::filesystem::remove(out_dir / (feature_record.feature().name() + ".abi"));

            co_return std::unexpected(evm::DeployError{evm::DeployError::Kind::INVALID_DATA});
        }

        const std::string name = feature_record.feature().name();
        if(!co_await registry.addFeature(deploy_res.value(), std::move(feature_record)))
        {
            spdlog::error("Failed to add feature '{}'", name);

            // remove binary file
            std::filesystem::remove(out_dir / (feature_record.feature().name() + ".bin"));
            std::filesystem::remove(out_dir / (feature_record.feature().name() + ".abi"));

            co_return std::unexpected(evm::DeployError{});
        }

        spdlog::debug("feature '{}' added", name);
        co_return deploy_res.value();
    }

    asio::awaitable<std::expected<evm::Address, evm::DeployError>> deployTransformation(evm::EVM & evm, registry::Registry & registry, TransformationRecord transformation_record)
    {
        if(transformation_record.transformation().name().empty() || transformation_record.transformation().sol_src().empty())
        {
            spdlog::error("Transformation name or source is empty");
            co_return std::unexpected(evm::DeployError{evm::DeployError::Kind::INVALID_DATA});
        }

        const auto address_result = evmc::from_hex<evm::Address>(transformation_record.owner());
        if(!address_result)
        {
            spdlog::error("Failed to parse address");
            co_return std::unexpected(evm::DeployError{evm::DeployError::Kind::INVALID_ADDRESS});
        }
        const auto & address = *address_result;

        static const std::filesystem::path out_dir = file::getStoragePath() / "transformations" / "build";

        if(std::filesystem::exists(out_dir) == false)
        {
            spdlog::error("Directory {} does not exists", out_dir.string());
            co_return std::unexpected(evm::DeployError{});
        }

        // if binary file does not exist
        if(std::filesystem::exists(out_dir / (transformation_record.transformation().name() + ".bin")) == false)
        {
            // there is a need for compile code
            const std::filesystem::path code_path = out_dir / (transformation_record.transformation().name() + ".sol");

            // create code file
            std::ofstream out_file(code_path); 
            if(!out_file.is_open())
            {
                spdlog::error("Failed to create file");
                co_return std::unexpected(evm::DeployError{});
            }

            out_file << constructTransformationSolidityCode(transformation_record.transformation());
            out_file.close();

            // compile code
            if(!co_await evm.compile(code_path, out_dir, file::getPTPath()/ "contracts"))
            {
                spdlog::error("Failed to compile code");
                // remove code file
                std::filesystem::remove(code_path);
                co_return std::unexpected(evm::DeployError{evm::DeployError::Kind::COMPILATION_ERROR});
            }

            // remove code file
            std::filesystem::remove(code_path);
        }

        co_await evm.addAccount(address, evm::DEFAULT_GAS_LIMIT);
        co_await evm.setGas(address, evm::DEFAULT_GAS_LIMIT);

        auto deploy_res = co_await evm.deploy(  
            out_dir / (transformation_record.transformation().name() + ".bin"), 
            address, 
            evm::encodeAsArg(evm.getRegistryAddress()), 
            evm::DEFAULT_GAS_LIMIT, 
            0);
        
        if(!deploy_res)
        {
            spdlog::error(std::format("Failed to deploy code {}", deploy_res.error().kind));

            // deploy can fail in case when this name is already taken - then we don't want to remove binary file
            // if it fails for another reason - we want to remove binary file
            if(deploy_res.error().kind != evm::DeployError::Kind::TRANSFORMATION_ALREADY_REGISTERED)
            {
                // remove binary file
                std::filesystem::remove(out_dir / (transformation_record.transformation().name() + ".bin"));
                std::filesystem::remove(out_dir / (transformation_record.transformation().name() + ".abi"));
            }
            co_return std::unexpected(deploy_res.error());
        }

        const auto owner_result = co_await fetchOwner(evm, deploy_res.value());

        // check execution status
        if(!owner_result)
        {
            spdlog::error(std::format("Failed to fetch owner {}", owner_result.error().kind));

            // remove binary file
            std::filesystem::remove(out_dir / (transformation_record.transformation().name() + ".bin"));
            std::filesystem::remove(out_dir / (transformation_record.transformation().name() + ".abi"));

            co_return std::unexpected(evm::DeployError{});
        }
        const auto owner_address = evm::decodeReturnedValue<evm::Address>(owner_result.value());

        if(owner_address != address)
        {
            spdlog::error("Owner address mismatch");

            // remove binary file
            std::filesystem::remove(out_dir / (transformation_record.transformation().name() + ".bin"));
            std::filesystem::remove(out_dir / (transformation_record.transformation().name() + ".abi"));

            co_return std::unexpected(evm::DeployError{evm::DeployError::Kind::INVALID_DATA});
        }

        const std::string name = transformation_record.transformation().name();
        if(!co_await registry.addTransformation(deploy_res.value(), std::move(transformation_record))) 
        {
            spdlog::error("Failed to add transformation '{}'", name);

            // remove binary file
            std::filesystem::remove(out_dir / (transformation_record.transformation().name() + ".bin"));
            std::filesystem::remove(out_dir / (transformation_record.transformation().name() + ".abi"));

            co_return std::unexpected(evm::DeployError{});
        }

        spdlog::debug("transformation '{}' added", name);
        co_return deploy_res.value();
    }


    asio::awaitable<std::expected<evm::Address, evm::DeployError>> deployCondition(evm::EVM & evm, registry::Registry & registry, ConditionRecord condition_record)
    {
        if(condition_record.condition().name().empty() || condition_record.condition().sol_src().empty())
        {
            spdlog::error("Condition name or source is empty");
            co_return std::unexpected(evm::DeployError{evm::DeployError::Kind::INVALID_DATA});
        }

        const auto address_result = evmc::from_hex<evm::Address>(condition_record.owner());
        if(!address_result)
        {
            spdlog::error("Failed to parse address");
            co_return std::unexpected(evm::DeployError{evm::DeployError::Kind::INVALID_ADDRESS});
        }
        const auto & address = *address_result;

        static const std::filesystem::path out_dir = file::getStoragePath() / "conditions" / "build";

        if(std::filesystem::exists(out_dir) == false)
        {
            spdlog::error("Directory {} does not exists", out_dir.string());
            co_return std::unexpected(evm::DeployError{});
        }

        // if binary file does not exist
        if(std::filesystem::exists(out_dir / (condition_record.condition().name() + ".bin")) == false)
        {
            // there is a need for compile code
            const std::filesystem::path code_path = out_dir / (condition_record.condition().name() + ".sol");

            // create code file
            std::ofstream out_file(code_path); 
            if(!out_file.is_open())
            {
                spdlog::error("Failed to create file");
                co_return std::unexpected(evm::DeployError{});
            }

            out_file << constructConditionSolidityCode(condition_record.condition());
            out_file.close();

            // compile code
            if(!co_await evm.compile(code_path, out_dir, file::getPTPath()/ "contracts"))
            {
                spdlog::error("Failed to compile code");
                // remove code file
                std::filesystem::remove(code_path);
                co_return std::unexpected(evm::DeployError{evm::DeployError::Kind::COMPILATION_ERROR});
            }

            // remove code file
            std::filesystem::remove(code_path);
        }

        co_await evm.addAccount(address, evm::DEFAULT_GAS_LIMIT);
        co_await evm.setGas(address, evm::DEFAULT_GAS_LIMIT);

        auto deploy_res = co_await evm.deploy(  
            out_dir / (condition_record.condition().name() + ".bin"), 
            address, 
            evm::encodeAsArg(evm.getRegistryAddress()), 
            evm::DEFAULT_GAS_LIMIT, 
            0);
        
        if(!deploy_res)
        {
            spdlog::error(std::format("Failed to deploy code {}", deploy_res.error().kind));

            // deploy can fail in case when this name is already taken - then we don't want to remove binary file
            // if it fails for another reason - we want to remove binary file
            if(deploy_res.error().kind != evm::DeployError::Kind::CONDITION_ALREADY_REGISTERED)
            {
                // remove binary file
                std::filesystem::remove(out_dir / (condition_record.condition().name() + ".bin"));
                std::filesystem::remove(out_dir / (condition_record.condition().name() + ".abi"));
            }
            co_return std::unexpected(deploy_res.error());
        }

        const auto owner_result = co_await fetchOwner(evm, deploy_res.value());

        // check execution status
        if(!owner_result)
        {
            spdlog::error(std::format("Failed to fetch owner {}", owner_result.error().kind));

            // remove binary file
            std::filesystem::remove(out_dir / (condition_record.condition().name() + ".bin"));
            std::filesystem::remove(out_dir / (condition_record.condition().name() + ".abi"));

            co_return std::unexpected(evm::DeployError{});
        }
        const auto owner_address = evm::decodeReturnedValue<evm::Address>(owner_result.value());

        if(owner_address != address)
        {
            spdlog::error("Owner address mismatch");

            // remove binary file
            std::filesystem::remove(out_dir / (condition_record.condition().name() + ".bin"));
            std::filesystem::remove(out_dir / (condition_record.condition().name() + ".abi"));

            co_return std::unexpected(evm::DeployError{evm::DeployError::Kind::INVALID_DATA});
        }

        const std::string name = condition_record.condition().name();
        if(!co_await registry.addCondition(deploy_res.value(), std::move(condition_record))) 
        {
            spdlog::error("Failed to add condition '{}'", name);

            // remove binary file
            std::filesystem::remove(out_dir / (condition_record.condition().name() + ".bin"));
            std::filesystem::remove(out_dir / (condition_record.condition().name() + ".abi"));

            co_return std::unexpected(evm::DeployError{});
        }

        spdlog::debug("condition '{}' added", name);
        co_return deploy_res.value();
    }



    asio::awaitable<bool> loadStoredFeatures(evm::EVM & evm, registry::Registry & registry)
    {
        spdlog::info("Loading stored features...");

        auto loaded_features = _loadJSONRecords<FeatureRecord>(file::getStoragePath() / "features");
        if(loaded_features.empty())
        {
            co_return false;
        }

        const auto sorted_features = utils::topologicalSort<FeatureRecord, Dimension, google::protobuf::RepeatedPtrField>(
                loaded_features,
                [](const FeatureRecord & record){return record.feature().dimensions();},
                [](const Dimension & dim) {return dim.feature_name();}
            );

        bool success = true;
        std::size_t i = 0;
        const std::size_t batch_size = (sorted_features.size() / 100) + 1;
        assert(batch_size > 0);
        
        for(const auto & name : sorted_features)
        {
            if(!co_await deployFeature(evm, registry, std::move(loaded_features.at(name))))
            {
                spdlog::error("Failed to deploy feature `{}`", name);
                success = false;
            }

            if(++i % batch_size == 0) {spdlog::debug("{}/{} features loaded", i, loaded_features.size());}
        }

        co_return success;
    }

    asio::awaitable<bool> loadStoredTransformations(evm::EVM & evm, registry::Registry & registry)
    {
        spdlog::info("Loading stored transformations...");

        auto loaded_transformations = _loadJSONRecords<TransformationRecord>(file::getStoragePath() / "transformations");
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
            if(!co_await deployTransformation(evm, registry, std::move(transformation)))
            {
                spdlog::error("Failed to deploy transformation `{}`", name);
                success = false;
            }

            if(++i % batch_size == 0) {spdlog::debug("{}/{} transformations loaded", i, loaded_transformations.size());}
        }

        co_return success;
    }

    asio::awaitable<bool> loadStoredConditions(evm::EVM & evm, registry::Registry & registry)
    {
        spdlog::info("Loading stored conditions...");

        auto loaded_conditions = _loadJSONRecords<ConditionRecord>(file::getStoragePath() / "conditions");
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
            if(!co_await deployCondition(evm, registry, std::move(condition)))
            {
                spdlog::error("Failed to deploy condition `{}`", name);
                success = false;
            }

            if(++i % batch_size == 0) {spdlog::debug("{}/{} conditions loaded", i, loaded_conditions.size());}
        }

        co_return success;
    }

}