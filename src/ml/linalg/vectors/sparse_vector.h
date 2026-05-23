#pragma once

#include <vector>
#include <algorithm>

class SparseVector
{
    public:
        SparseVector(int size, const std::vector<int>& indices, const std::vector<double>& values)
        :
            size_(size), 
            indices_(indices),
            values_(values)
        {}

        SparseVector()
        : 
            SparseVector(0, {}, {}) 
        {}

        int size() const
        { 
            return size_; 
        }

        const std::vector<int>& indices() const 
        { 
            return indices_; 
        }

        int numNonzeros() const 
        { 
            return values_.size(); 
        }
        
        int numActives() const 
        { 
            return indices_.size();
        }

        int argmax() const
        {
            if (values_.empty())
                return -1;

            auto max_element = std::max_element(values_.begin(), values_.end());
            int max_index = std::distance(values_.begin(), max_element);

            return indices_[max_index];
        }

        SparseVector copy() const
        {
            return *this;
        }

    private:
        int size_;
        std::vector<int> indices_;
        std::vector<double> values_;
};
