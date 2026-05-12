#pragma once

#include <cstdint>
#include <map>
#include <optional>
#include <stdexcept>
#include <string>
#include <type_traits>
#include <variant>

#include <spark/connect/ml_common.pb.h>

#include "param.h"

using ParamValue = std::variant<std::string, int, std::int64_t, double, bool>;

class ParamMap
{
    public:
        ParamMap() = default;

        static ParamMap empty()
        {
            return ParamMap{};
        }

        // ----------------------------------------------------------------------
        // Param<T>-keyed API
        // ----------------------------------------------------------------------

        template <typename T>
        ParamMap& put(const Param<T>& param, const T& value)
        {
            if (!param.is_valid(value))
                throw std::invalid_argument("Invalid value for param " + param.key());

            map_[param.key()] = ParamValue{value};

            return *this;
        }

        template <typename T>
        std::optional<T> get(const Param<T>& param) const
        {
            auto it = map_.find(param.key());

            if (it == map_.end())
                return std::nullopt;

            if (const auto* v = std::get_if<T>(&it->second))
                return *v;

            return std::nullopt;
        }

        template <typename T>
        T getOrElse(const Param<T>& param, std::type_identity_t<T> default_value) const
        {
            return get(param).value_or(std::move(default_value));
        }

        template <typename T>
        bool contains(const Param<T>& param) const
        {
            return map_.find(param.key()) != map_.end();
        }

        template <typename T>
        std::optional<T> remove(const Param<T>& param)
        {
            auto it = map_.find(param.key());

            if (it == map_.end())
                return std::nullopt;

            const auto* v = std::get_if<T>(&it->second);
            std::optional<T> result = v ? std::optional<T>{*v} : std::nullopt;

            map_.erase(it);

            return result;
        }

        // ----------------------------------------------------------------------
        // String-keyed API
        // ----------------------------------------------------------------------

        template <typename T>
        ParamMap& put(const std::string& key, const T& value)
        {
            map_[key] = ParamValue{value};

            return *this;
        }

        ParamMap& put(const std::string& key, const char* value)
        {
            map_[key] = ParamValue{std::string{value}};

            return *this;
        }

        std::optional<ParamValue> get(const std::string& key) const
        {
            auto it = map_.find(key);

            if (it == map_.end())
                return std::nullopt;
                
            return it->second;
        }

        bool contains(const std::string& key) const
        {
            return map_.find(key) != map_.end();
        }

        bool clear(const std::string& key)
        {
            return map_.erase(key) > 0;
        }

        // ----------------------------------------------------------------------
        // Bulk operations
        // ----------------------------------------------------------------------

        void clear()
        {
            map_.clear();
        }

        bool is_empty() const
        {
            return map_.empty();
        }

        int size() const
        {
            return static_cast<int>(map_.size());
        }

        void put_all(const ParamMap& other)
        {
            for (const auto& [k, v] : other.map_)
                map_[k] = v;
        }

        ParamMap copy() const
        {
            return *this;
        }

        std::string toString() const;

        const std::map<std::string, ParamValue>& entries() const
        {
            return map_;
        }

    private:
        std::map<std::string, ParamValue> map_;
};

inline spark::connect::MlParams to_ml_params(const ParamMap& param_map)
{
    spark::connect::MlParams ml_params;
    auto* proto_map = ml_params.mutable_params();

    for (const auto& [key, value] : param_map.entries())
    {
        // ParamMap stores keys as `parent__name` (see Param::key()); the wire
        // format expects the bare param name, since the parent uid is implicit
        // in the estimator the params attach to.
        const auto sep = key.rfind("__");
        const std::string& name = (sep != std::string::npos) ? key.substr(sep + 2) : key;

        std::visit(
            [&](auto&& v)
            {
                using V = std::decay_t<decltype(v)>;

                if constexpr (std::is_same_v<V, std::string>)
                    (*proto_map)[name].set_string(v);

                else if constexpr (std::is_same_v<V, int>)
                    (*proto_map)[name].set_integer(v);

                else if constexpr (std::is_same_v<V, std::int64_t>)
                    (*proto_map)[name].set_long_(v);

                else if constexpr (std::is_same_v<V, double>)
                    (*proto_map)[name].set_double_(v);

                else if constexpr (std::is_same_v<V, bool>)
                    (*proto_map)[name].set_boolean(v);
            },
            value);
    }

    return ml_params;
}
