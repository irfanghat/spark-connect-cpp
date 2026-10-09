#pragma once

#include <vector>

class DenseVector
{
  public:
    DenseVector(std::vector<double> const& values) : values_(values)
    {
    }

    int
    size() const
    {
        return values_.size();
    }

    std::vector<double> const&
    values() const
    {
        return values_;
    }

  private:
    std::vector<double> values_;
};
