#include <numeric>
#include <cmath>

#include "ml/linalg/vectors/sparse_vector.h"

double SparseVector::norm(int param)
{
    switch (param)
    {
        case 1: // City taxi (multiple destinations sequentially)
            return std::accumulate(values_.begin(), values_.end(), 0.0, [](double sum, double val) { 
                return sum + std::abs(val); 
            });

        case 2: // As the Crow flies (single destination)
            return std::sqrt(std::accumulate(values_.begin(), values_.end(), 0.0, [](double sum, double val) { 
                return sum + val * val; 
            }));

        case 3: // Drone fleet (multiple destinations simultaneously)
            return *std::max_element(values_.begin(), values_.end(), [](double a, double b) { 
                return std::abs(a) < std::abs(b); 
            });

        default:
            return -1;
    }
}
