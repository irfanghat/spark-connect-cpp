#include <gtest/gtest.h>

#include "types.h"
#include "ml/linalg/vectors/sparse_vector.h"
#include "spark/spark_fixture.h"

using namespace spark::sql::types;

TEST_F(SparkIntegrationTest, SparkVector)
{
    auto col = std::make_shared<Row>();

    col->column_names = {"type", "size", "indices", "values"};
    col->values = {
        int8_t(0),
        int32_t(4),
        std::make_shared<ArrayData>(ArrayData{{0, 1}}),
        std::make_shared<ArrayData>(ArrayData{{3.0, -4.0}})
    };

    auto row = std::make_shared<Row>();

    row->column_names = {"features"};
    row->values = {col};

    SparseVector vec;

    ASSERT_NO_THROW(vec = row->get<SparseVector>("features"));
    EXPECT_DOUBLE_EQ(vec.norm(1), 7.0);
    EXPECT_DOUBLE_EQ(vec.norm(2), 5.0);
}
