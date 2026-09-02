#pragma once

#include <cstdint>

namespace spark::sql::types
{
struct DecimalType
{
    int32_t precision = 10;
    int32_t scale = 0;
    DecimalType() = default;
    DecimalType(int32_t p, int32_t s) : precision(p), scale(s) {}
};
} // namespace spark::sql::types

