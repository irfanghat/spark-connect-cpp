#pragma once

#include "param.h"

#include <cstdint>
#include <map>
#include <optional>
#include <spark/connect/ml_common.pb.h>
#include <stdexcept>
#include <string>
#include <type_traits>
#include <variant>
#include <vector>

using ParamValue = std::variant<std::string, int, std::int64_t, double, bool,
                                std::vector<std::string>>;

class ParamMap
{
  public:
    ParamMap() = default;

    static ParamMap
    empty()
    {
        return ParamMap{};
    }

    // Param<T>-keyed API
    template <typename T>
    ParamMap&
    put(Param<T> const& param, T const& value)
    {
        if (!param.is_valid(value))
            throw std::invalid_argument("Invalid value for param " +
                                        param.key());

        map_[param.key()] = ParamValue{value};

        return *this;
    }

    template <typename T>
    std::optional<T>
    get(Param<T> const& param) const
    {
        auto it = map_.find(param.key());

        if (it == map_.end())
            return std::nullopt;

        if (auto const* v = std::get_if<T>(&it->second))
            return *v;

        return std::nullopt;
    }

    template <typename T>
    T
    getOrElse(Param<T> const& param,
              std::type_identity_t<T> default_value) const
    {
        return get(param).value_or(std::move(default_value));
    }

    template <typename T>
    bool
    contains(Param<T> const& param) const
    {
        return map_.find(param.key()) != map_.end();
    }

    template <typename T>
    std::optional<T>
    remove(Param<T> const& param)
    {
        auto it = map_.find(param.key());

        if (it == map_.end())
            return std::nullopt;

        auto const* v = std::get_if<T>(&it->second);
        std::optional<T> result = v ? std::optional<T>{*v} : std::nullopt;

        map_.erase(it);

        return result;
    }

    // String-keyed API
    template <typename T>
    ParamMap&
    put(std::string const& key, T const& value)
    {
        map_[key] = ParamValue{value};

        return *this;
    }

    ParamMap&
    put(std::string const& key, char const* value)
    {
        map_[key] = ParamValue{std::string{value}};

        return *this;
    }

    std::optional<ParamValue>
    get(std::string const& key) const
    {
        auto it = map_.find(key);

        if (it == map_.end())
            return std::nullopt;

        return it->second;
    }

    bool
    contains(std::string const& key) const
    {
        return map_.find(key) != map_.end();
    }

    bool
    clear(std::string const& key)
    {
        return map_.erase(key) > 0;
    }

    void
    clear()
    {
        map_.clear();
    }

    bool
    is_empty() const
    {
        return map_.empty();
    }

    int
    size() const
    {
        return static_cast<int>(map_.size());
    }

    void
    put_all(ParamMap const& other)
    {
        for (auto const& [k, v] : other.map_)
            map_[k] = v;
    }

    ParamMap
    copy() const
    {
        return *this;
    }

    std::string toString() const;

    std::map<std::string, ParamValue> const&
    entries() const
    {
        return map_;
    }

  private:
    std::map<std::string, ParamValue> map_;
};

inline spark::connect::MlParams
to_ml_params(ParamMap const& param_map)
{
    spark::connect::MlParams ml_params;
    auto* proto_map = ml_params.mutable_params();

    for (auto const& [key, value] : param_map.entries())
    {
        // -------------------------------------------------------------------------
        // ParamMap stores keys as `parent__name` (see Param::key())
        // The wire format expects the bare param name, since the parent uid
        // is implicit in the estimator the params attach to.
        // -------------------------------------------------------------------------
        auto const sep = key.rfind("__");
        std::string const& name =
            (sep != std::string::npos) ? key.substr(sep + 2) : key;

        std::visit(
            [&](auto&& v) {
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

                else if constexpr (std::is_same_v<V, std::vector<std::string>>)
                {
                    auto* literal = &(*proto_map)[name];
                    auto* array = literal->mutable_array();

                    // ------------------------------------------------
                    // Set the root literal's type to ARRAY<STRING>.
                    // ------------------------------------------------
                    auto* array_type =
                        literal->mutable_data_type()->mutable_array();
                    array_type->mutable_element_type()->mutable_string();

                    for (auto const& element : v)
                        array->add_elements()->set_string(element);
                }
            },
            value);
    }

    return ml_params;
}
