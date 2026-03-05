#include "error.hpp"

#include <spdlog/spdlog.h>

namespace dcn::parse
{
    template<>
    Result<pt::PTExecuteError> decodeBytes(const std::vector<std::uint8_t> & r)
    {
        if(r.data() == nullptr || r.size() < 4)
        {
            return std::unexpected(ParseError{ParseError::Kind::INVALID_VALUE, "invalid bytes"});
        }

        std::vector<std::uint8_t> selector;

        selector = chain::constructSelector("ConditionNotMet(bytes32)");
        if (std::equal(selector.begin(), selector.end(), r.data()))
        {
            return pt::PTExecuteError{pt::PTExecuteError::Kind::CONDITION_NOT_MET};
        }

        return std::unexpected(ParseError{ParseError::Kind::UNKNOWN, "unknown error"});
    }

    template<>
    Result<pt::PTDeployError> decodeBytes(const std::vector<std::uint8_t> & r)
    {
        if(r.data() == nullptr || r.size() < 4)
        {
            return std::unexpected(ParseError{ParseError::Kind::INVALID_VALUE, "invalid bytes"});
        }

        std::vector<std::uint8_t> selector;

        selector = chain::constructSelector("ParticleAlreadyRegistered(bytes32)");
        if (std::equal(selector.begin(), selector.end(), r.data()))
        {
            return pt::PTDeployError{pt::PTDeployError::Kind::PARTICLE_ALREADY_REGISTERED};
        }

        selector = chain::constructSelector("ParticleMissing(bytes32)");
        if (std::equal(selector.begin(), selector.end(), r.data()))
        {
            return pt::PTDeployError{pt::PTDeployError::Kind::PARTICLE_MISSING};
        }

        selector = chain::constructSelector("ParticleDimensionsMismatch(bytes32)");
        if (std::equal(selector.begin(), selector.end(), r.data()))
        {
            return pt::PTDeployError{pt::PTDeployError::Kind::PARTICLE_DIMENSIONS_MISMATCH};
        }

        selector = chain::constructSelector("FeatureAlreadyRegistered(bytes32)");
        if (std::equal(selector.begin(), selector.end(), r.data()))
        {
            return pt::PTDeployError{pt::PTDeployError::Kind::FEATURE_ALREADY_REGISTERED};
        }

        selector = chain::constructSelector("FeatureMissing(bytes32)");
        if (std::equal(selector.begin(), selector.end(), r.data()))
        {
            return pt::PTDeployError{pt::PTDeployError::Kind::FEATURE_MISSING};
        }

        selector = chain::constructSelector("TransformationAlreadyRegistered(bytes32)");
        if (std::equal(selector.begin(), selector.end(), r.data()))
        {
            return pt::PTDeployError{pt::PTDeployError::Kind::TRANSFORMATION_ALREADY_REGISTERED};
        }

        selector = chain::constructSelector("TransformationArgumentsMismatch(bytes32)");
        if (std::equal(selector.begin(), selector.end(), r.data()))
        {
            return pt::PTDeployError{pt::PTDeployError::Kind::TRANSFORMATION_ARGUMENTS_MISMATCH};
        }

        selector = chain::constructSelector("TransformationMissing(bytes32)");
        if (std::equal(selector.begin(), selector.end(), r.data()))
        {
            return pt::PTDeployError{pt::PTDeployError::Kind::TRANSFORMATION_MISSING};
        }

        selector = chain::constructSelector("ConditionAlreadyRegistered(bytes32)");
        if (std::equal(selector.begin(), selector.end(), r.data()))
        {
            return pt::PTDeployError{pt::PTDeployError::Kind::CONDITION_ALREADY_REGISTERED};
        }

        selector = chain::constructSelector("ConditionArgumentsMismatch(bytes32)");
        if (std::equal(selector.begin(), selector.end(), r.data()))
        {
            return pt::PTDeployError{pt::PTDeployError::Kind::CONDITION_ARGUMENTS_MISMATCH};
        }

        selector = chain::constructSelector("ConditionMissing(bytes32)");
        if (std::equal(selector.begin(), selector.end(), r.data()))
        {
            return pt::PTDeployError{pt::PTDeployError::Kind::CONDITION_MISSING};
        }

        selector = chain::constructSelector("RegistryError(uint32)");
        if (std::equal(selector.begin(), selector.end(), r.data()))
        {
            return pt::PTDeployError{pt::PTDeployError::Kind::REGISTRY_ERROR};
        }

        return std::unexpected{ParseError{ParseError::Kind::UNKNOWN, "unknown error"}};
    }

}