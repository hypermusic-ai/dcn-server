#pragma once

#include <string>
#include <format>
#include <vector>
#include <optional>
#include <filesystem>

#include <absl/container/flat_hash_map.h>
#include <spdlog/spdlog.h>

namespace dcn::cmd
{
    struct CommandLineArgDef
    {
        enum class NArgs
        {
            Zero,
            One,
            Many
        };

        enum class Type
        {
            Unknown = 0,
            Int,
            Uint,
            String,
            Bool,
            Float,
            Path
        };

        const std::string name;
        const std::string desc;

        NArgs nargs;
        Type type;
    };

    class ArgParser
    {

        public:
            std::string constructHelpMessage() const;

            template<class T>
            void addArg(std::string name, std::string desc);

            template<class T>
            void addArg(std::string name, std::string desc, T default_value)
            {
                addArg<T>(std::move(name), std::move(desc));
                _values[name] = std::move(default_value);
            }

            void parse(int argc, char** argv);

            template<class T>
            std::optional<T> getArg(std::string_view name)
            {
                const auto it = _values.find(name);
                if(it == _values.end())
                {
                    return std::nullopt;
                }

                if(std::holds_alternative<T>(it->second) == false)
                {
                    spdlog::error("Type mismatch for argument \"{}\"", name);
                    return std::nullopt;
                }

                return std::get<T>(it->second);
            }


        private:
            void _addArgDef(std::string name, CommandLineArgDef::NArgs nargs, CommandLineArgDef::Type type, std::string desc);



            std::vector<CommandLineArgDef> _args;

            absl::flat_hash_map<std::string, std::variant<
                bool,

                int,
                std::vector<int>,

                unsigned int,
                std::vector<unsigned int>,

                std::string,
                std::vector<std::string>,

                float,
                std::vector<float>,

                std::filesystem::path,
                std::vector<std::filesystem::path>>> _values;
    };

    template<>
    void ArgParser::addArg<bool>(std::string name, std::string desc);

    template<>
    void ArgParser::addArg<int>(std::string name, std::string desc);

    template<>
    void ArgParser::addArg<std::vector<int>>(std::string name, std::string desc);

    template<>
    void ArgParser::addArg<unsigned int>(std::string name, std::string desc);

    template<>
    void ArgParser::addArg<std::vector<unsigned int>>(std::string name, std::string desc);

    template<>
    void ArgParser::addArg<std::string>(std::string name, std::string desc);

    template<>
    void ArgParser::addArg<std::vector<std::string>>(std::string name, std::string desc);

    template<>
    void ArgParser::addArg<float>(std::string name, std::string desc);

    template<>
    void ArgParser::addArg<std::vector<float>>(std::string name, std::string desc);

    template<>
    void ArgParser::addArg<std::filesystem::path>(std::string name, std::string desc);

    template<>
    void ArgParser::addArg<std::vector<std::filesystem::path>>(std::string name, std::string desc);

}