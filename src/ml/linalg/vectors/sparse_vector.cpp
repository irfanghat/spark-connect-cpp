#include "ml/linalg/vectors/sparse_vector.h"

#include <cmath>
#include <numeric>

double
SparseVector::norm(int param)
{
    switch (param)
    {
    case 1: // City taxi (Multiple destinations sequentially)
        return std::accumulate(
            values_.begin(), values_.end(), 0.0,
            [](double sum, double val) { return sum + std::abs(val); });

    case 2: // As the Crow flies (Single destination)
        return std::sqrt(std::accumulate(
            values_.begin(), values_.end(), 0.0,
            [](double sum, double val) { return sum + val * val; }));

    case 3: // Drone fleet (Multiple destinations simultaneously)
        return *std::max_element(
            values_.begin(), values_.end(),
            [](double a, double b) { return std::abs(a) < std::abs(b); });

    default:
        return -1;
    }
}
