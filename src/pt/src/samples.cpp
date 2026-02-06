#include "samples.hpp"

namespace dcn::parse
{
    template<>
    Result<json> parseToJson(std::vector<Samples> samples, use_json_t) 
    {
        json arr = json::array();

        for (const auto& s : samples) {
            json obj;
            obj["path"] = s.path();
            obj["data"] = s.data();
            arr.push_back(obj);
        }

        return arr;
    }

    template<>
    Result<std::vector<Samples>> parseFromJson(json json_val, use_json_t) 
    {
        if (!json_val.is_array())
            return std::unexpected(ParseError{ParseError::Kind::TYPE_MISMATCH, "Invalid JSON object"});

        std::vector<Samples> result;

        for (const auto& item : json_val) {
            if (!item.contains("path") || !item.contains("data"))
                return std::unexpected(ParseError{ParseError::Kind::INVALID_VALUE, "path or data not found"});

            Samples s;
            s.set_path(item["path"].get<std::string>());

            for (const auto& val : item["data"]) {
                s.add_data(val.get<std::uint32_t>());
            }

            result.push_back(std::move(s));
        }

        return result;
    }

}