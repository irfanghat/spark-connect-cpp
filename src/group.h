#pragma once

#include <vector>
#include <string>
#include <memory>
#include "dataframe.h"
#include "types.h"

class GroupedData {
public:
    GroupedData(DataFrame& df, std::vector<spark::sql::types::Column> grouping_cols);

    DataFrame count();
    DataFrame sum(const std::vector<std::string>& cols);
    DataFrame avg(const std::vector<std::string>& cols);
    // ...

private:
    DataFrame& df_;
    std::vector<spark::sql::types::Column> grouping_cols_;
};