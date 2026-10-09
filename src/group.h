#pragma once

#include "dataframe.h"
#include "functions.h"
#include "types.h"

#include <memory>
#include <string>
#include <vector>

class GroupedData
{
  public:
    GroupedData(DataFrame& df,
                std::vector<spark::sql::functions::Column> grouping_cols);

    DataFrame count();
    DataFrame sum(std::vector<std::string> const& cols);
    DataFrame avg(std::vector<std::string> const& cols);
    // ...

  private:
    DataFrame& df_;
    std::vector<spark::sql::functions::Column> grouping_cols_;
};