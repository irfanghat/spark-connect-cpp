#pragma once

#include <stdexcept>
#include <variant>

#include <spark/connect/ml_common.pb.h>

#include "param_map.h"

inline spark::connect::MlParams to_ml_params(const ParamMap& param_map)
{
    spark::connect::MlParams ml_params;
    auto* proto_map = ml_params.mutable_params();

    for (const auto& [key, value] : param_map.entries())
    {
        std::visit(
            [&](auto&& v)
            {
                using T = std::decay_t<decltype(v)>;
                if constexpr (std::is_same_v<T, std::string>)
                    (*proto_map)[key].set_string(v);
                else if constexpr (std::is_same_v<T, int>)
                    (*proto_map)[key].set_integer(v);
                else if constexpr (std::is_same_v<T, int64_t>)
                    (*proto_map)[key].set_long_(v);
                else if constexpr (std::is_same_v<T, double>)
                    (*proto_map)[key].set_double_(v);
                else if constexpr (std::is_same_v<T, bool>)
                    (*proto_map)[key].set_boolean(v);
            },
            value);
    }

    return ml_params;
}
