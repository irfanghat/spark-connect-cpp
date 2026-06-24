#pragma once

#include <vector>

class DenseVector
{
    public:
        DenseVector(const std::vector<double>& values)
        :
            values_(values)
        {}

        int size() const
        {
            return values_.size();
        }

        const std::vector<double>& values() const
        {
            return values_;
        }

    private:
        std::vector<double> values_;
};
