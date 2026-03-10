#include <gtest/gtest.h>
#include <gmock/gmock.h>

#include "session.h"
#include "dataframe.h"
#include "graphframe.h"
#include "functions.h"

using namespace graphframes;
using namespace spark::sql::functions;
using ::testing::Contains;

static std::vector<std::string> gfColumns(DataFrame df)
{
    auto rows = df.collect();
    if (rows.empty())
        return {};
    return rows[0].column_names;
}

static int64_t gfCount(DataFrame df)
{
    return static_cast<int64_t>(df.collect().size());
}

template <typename T>
static std::vector<T> gfColumn(DataFrame df, const std::string &col)
{
    auto rows = df.collect();
    std::vector<T> out;
    out.reserve(rows.size());
    for (auto &r : rows)
        out.push_back(r.get<T>(col));
    return out;
}

/**
 * @note
 *
 * Graph (Directed):
 *
 * Vertices (INT id):  `1 - Alice(34)`  `2 - Bob(36)`  `3 - Charlie(30)`  `4 - Anne(29)`
 *
 * Edges (INT src/dst): 1 -> 2 friend
 *                      2 -> 3 follow
 *                      3 -> 1 friend    <- closes triangle 1 -> 2 -> 3 -> 1
 *                      1 -> 4 colleague <- 4 is a leaf
 *
 * `ids` are plain integers (INT32), not BIGINT. ShortestPaths internally
 * builds MAP<id_type, INT> and requires INT32 ids to avoid a type
 * mismatch in its Pregel implementation.
 *
 * `connectedComponents` uses use_local_checkpoints=true by default
 * therefore, no checkpoint directory needs to be configured on the server.
 * We still need to review this.
 */
class SparkIntegrationTest : public ::testing::Test
{
protected:
    static SparkSession *spark;
    static DataFrame *vertices;
    static DataFrame *edges;

    static void SetUpTestSuite()
    {
        spark = &SparkSession::builder()
                     .master("sc://localhost")
                     .appName("SparkConnectCppGTest")
                     .getOrCreate();

        vertices = new DataFrame(spark->sql(R"(
            SELECT CAST(id AS INT) AS id, name, age FROM VALUES
                (1, 'Alice',   34),
                (2, 'Bob',     36),
                (3, 'Charlie', 30),
                (4, 'Anne',    29)
            AS people(id, name, age)
        )"));

        edges = new DataFrame(spark->sql(R"(
            SELECT CAST(src AS INT) AS src, CAST(dst AS INT) AS dst, relationship FROM VALUES
                (1, 2, 'friend'),
                (2, 3, 'follow'),
                (3, 1, 'friend'),
                (1, 4, 'colleague')
            AS connections(src, dst, relationship)
        )"));
    }

    static void TearDownTestSuite()
    {
        delete vertices;
        delete edges;
        spark->stop();
    }

    GraphFrame gf() const { return GraphFrame(*vertices, *edges); }
};

SparkSession *SparkIntegrationTest::spark = nullptr;
DataFrame *SparkIntegrationTest::vertices = nullptr;
DataFrame *SparkIntegrationTest::edges = nullptr;

// --------------------------------------------------------------------------
// PageRank
// --------------------------------------------------------------------------
TEST_F(SparkIntegrationTest, PageRankHasPageRankColumn)
{
    auto rows = gf().pageRank(0.15, 5).collect();
    ASSERT_FALSE(rows.empty());
    EXPECT_THAT(rows[0].column_names, Contains("pagerank"));
}

TEST_F(SparkIntegrationTest, PageRankReturnsOneRowPerVertex)
{
    EXPECT_EQ(gfCount(gf().pageRank(0.15, 5)), 4);
}

TEST_F(SparkIntegrationTest, PageRankAllValuesPositive)
{
    for (double r : gfColumn<double>(gf().pageRank(0.15, 5), "pagerank"))
        EXPECT_GT(r, 0.0);
}

TEST_F(SparkIntegrationTest, PageRankThrowsWhenBothMaxIterAndTolProvided)
{
    EXPECT_THROW(gf().pageRank(0.15, 5, 0.01), std::invalid_argument);
}

TEST_F(SparkIntegrationTest, PageRankWithTolConverges)
{
    EXPECT_EQ(gfCount(gf().pageRank(0.15, std::nullopt, 0.01)), 4);
}

TEST_F(SparkIntegrationTest, PageRankShow)
{
    EXPECT_NO_THROW(gf().pageRank(0.15, 5).show());
}

// --------------------------------------------------------------------------
// find() - Motif Matching
// --------------------------------------------------------------------------
TEST_F(SparkIntegrationTest, FindSingleHopMatchesAllEdges)
{
    EXPECT_EQ(gfCount(gf().find("(a)-[e]->(b)")), 4);
}

TEST_F(SparkIntegrationTest, FindTriangleReturnsThreeRotations)
{
    // ----------------------------------------------------------------
    // GraphFrames returns one row per rotation of each triangle.
    // The single triangle 1 -> 2 -> 3 -> 1 produces 3 rows:
    //   (a=1,b=2,c=3), (a=2,b=3,c=1), (a=3,b=1,c=2)
    // ----------------------------------------------------------------
    EXPECT_EQ(gfCount(gf().find("(a)-[e1]->(b); (b)-[e2]->(c); (c)-[e3]->(a)")), 3);
}

TEST_F(SparkIntegrationTest, FindTwoHopReturnsResults)
{
    EXPECT_GT(gfCount(gf().find("(a)-[e1]->(b); (b)-[e2]->(c)")), 0);
}

TEST_F(SparkIntegrationTest, FindResultHasCorrectColumns)
{
    auto cols = gfColumns(gf().find("(a)-[e]->(b)"));
    EXPECT_THAT(cols, Contains("a"));
    EXPECT_THAT(cols, Contains("e"));
    EXPECT_THAT(cols, Contains("b"));
}

TEST_F(SparkIntegrationTest, FindShow)
{
    EXPECT_NO_THROW(gf().find("(a)-[e]->(b)").show());
}

// --------------------------------------------------------------------------
// triplets()
// --------------------------------------------------------------------------
TEST_F(SparkIntegrationTest, TripletsCountMatchesEdges)
{
    EXPECT_EQ(gfCount(gf().triplets()), 4);
}

TEST_F(SparkIntegrationTest, TripletsHasCorrectColumns)
{
    auto cols = gfColumns(gf().triplets());
    EXPECT_THAT(cols, Contains("src"));
    EXPECT_THAT(cols, Contains("edge"));
    EXPECT_THAT(cols, Contains("dst"));
}

TEST_F(SparkIntegrationTest, TripletsShow)
{
    EXPECT_NO_THROW(gf().triplets().show());
}

// --------------------------------------------------------------------------
// filterEdges()
// --------------------------------------------------------------------------
TEST_F(SparkIntegrationTest, FilterEdgesSQLKeepsFriendEdges)
{
    EXPECT_EQ(gfCount(gf().filterEdges("relationship = 'friend'")), 2);
}

TEST_F(SparkIntegrationTest, FilterEdgesColumnKeepsFriendEdges)
{
    EXPECT_EQ(gfCount(gf().filterEdges(col("relationship") == lit("friend"))), 2);
}

TEST_F(SparkIntegrationTest, FilterEdgesNoMatchReturnsEmpty)
{
    EXPECT_EQ(gfCount(gf().filterEdges("relationship = 'enemy'")), 0);
}

TEST_F(SparkIntegrationTest, FilterEdgesShow)
{
    EXPECT_NO_THROW(gf().filterEdges("relationship = 'friend'").show());
}

// --------------------------------------------------------------------------
// filterVertices()
// --------------------------------------------------------------------------
TEST_F(SparkIntegrationTest, FilterVerticesSQLKeepsYoungVertices)
{
    // ---------------------------------------------------
    // age < 34: Charlie(30), Anne(29) -> 2 vertices
    // ---------------------------------------------------
    EXPECT_EQ(gfCount(gf().filterVertices("age < 34")), 2);
}

TEST_F(SparkIntegrationTest, FilterVerticesColumnExpr)
{
    EXPECT_EQ(gfCount(gf().filterVertices(col("age") < lit(34))), 2);
}

TEST_F(SparkIntegrationTest, FilterVerticesNoMatchReturnsEmpty)
{
    EXPECT_EQ(gfCount(gf().filterVertices("age > 100")), 0);
}

TEST_F(SparkIntegrationTest, FilterVerticesShow)
{
    EXPECT_NO_THROW(gf().filterVertices("age < 34").show());
}

// --------------------------------------------------------------------------
// dropIsolatedVertices()
// --------------------------------------------------------------------------
TEST_F(SparkIntegrationTest, DropIsolatedVerticesKeepsAllWhenNoneIsolated)
{
    EXPECT_EQ(gfCount(gf().dropIsolatedVertices()), 4);
}

TEST_F(SparkIntegrationTest, DropIsolatedVerticesRemovesIsolatedNode)
{
    auto v_with_isolated = spark->sql(R"(
        SELECT * FROM VALUES
            (1, 'Alice',   34),
            (2, 'Bob',     36),
            (3, 'Charlie', 30),
            (4, 'Anne',    29),
            (99, 'Ghost',  99)
        AS people(id, name, age)
    )");
    EXPECT_EQ(gfCount(GraphFrame(v_with_isolated, *edges).dropIsolatedVertices()), 4);
}

TEST_F(SparkIntegrationTest, DropIsolatedVerticesShow)
{
    EXPECT_NO_THROW(gf().dropIsolatedVertices().show());
}

// --------------------------------------------------------------------------
// BFS
// --------------------------------------------------------------------------
TEST_F(SparkIntegrationTest, BFSFindsPath)
{
    EXPECT_GT(gfCount(gf().bfs("id = 1", "id = 3")), 0);
}

TEST_F(SparkIntegrationTest, BFSNoPathInDirectedGraph)
{
    // -------------------------------------------------------------------
    // Vertex 4 has no outgoing edges so it cannot reach vertex 1.
    // -------------------------------------------------------------------
    EXPECT_EQ(gfCount(gf().bfs("id = 4", "id = 1")), 0);
}

TEST_F(SparkIntegrationTest, BFSWithEdgeFilter)
{
    EXPECT_GT(gfCount(gf().bfs("id = 1", "id = 2", "relationship = 'friend'")), 0);
}

TEST_F(SparkIntegrationTest, BFSWithColumnExpression)
{
    EXPECT_GT(gfCount(gf().bfs(col("id") == lit(1), col("id") == lit(3))), 0);
}

TEST_F(SparkIntegrationTest, BFSShow)
{
    EXPECT_NO_THROW(gf().bfs("id = 1", "id = 3").show());
}

// --------------------------------------------------------------------------
// connectedComponents()
//
// We set use_local_checkpoints=true so no checkpoint directory
// needs to be configured on the Spark server.
// --------------------------------------------------------------------------
TEST_F(SparkIntegrationTest, ConnectedComponentsReturnsOneRowPerVertex)
{
    EXPECT_EQ(gfCount(gf().connectedComponents()), 4);
}

TEST_F(SparkIntegrationTest, ConnectedComponentsHasComponentColumn)
{
    EXPECT_THAT(gfColumns(gf().connectedComponents()), Contains("component"));
}

TEST_F(SparkIntegrationTest, ConnectedComponentsAllInSameComponent)
{
    auto comps = gfColumn<int64_t>(gf().connectedComponents(), "component");
    std::set<int64_t> unique(comps.begin(), comps.end());
    EXPECT_EQ(unique.size(), 1u);
}

TEST_F(SparkIntegrationTest, ConnectedComponentsShow)
{
    EXPECT_NO_THROW(gf().connectedComponents().show());
}

// --------------------------------------------------------------------------
// stronglyConnectedComponents()
// --------------------------------------------------------------------------
TEST_F(SparkIntegrationTest, SCCReturnsOneRowPerVertex)
{
    EXPECT_EQ(gfCount(gf().stronglyConnectedComponents(10)), 4);
}

TEST_F(SparkIntegrationTest, SCCHasComponentColumn)
{
    EXPECT_THAT(gfColumns(gf().stronglyConnectedComponents()), Contains("component"));
}

TEST_F(SparkIntegrationTest, SCCProducesMultipleComponents)
{
    // -------------------------------------------------------------
    // Triangle 1 - 2 - 3 is one SCC
    // vertex 4 (leaf) is its own SCC
    // -------------------------------------------------------------
    auto comps = gfColumn<int64_t>(gf().stronglyConnectedComponents(), "component");
    std::set<int64_t> unique(comps.begin(), comps.end());
    EXPECT_GT(unique.size(), 1u);
}

TEST_F(SparkIntegrationTest, SCCShow)
{
    EXPECT_NO_THROW(gf().stronglyConnectedComponents().show());
}

// --------------------------------------------------------------------------
// shortestPaths()
// --------------------------------------------------------------------------
TEST_F(SparkIntegrationTest, ShortestPathsReturnsOneRowPerVertex)
{
    EXPECT_EQ(gfCount(gf().shortestPaths(std::vector<int32_t>{1, 3})), 4);
}

TEST_F(SparkIntegrationTest, ShortestPathsHasDistancesColumn)
{
    EXPECT_THAT(gfColumns(gf().shortestPaths(std::vector<int32_t>{1})), Contains("distances"));
}

TEST_F(SparkIntegrationTest, ShortestPathsShow)
{
    EXPECT_NO_THROW(gf().shortestPaths(std::vector<int32_t>{1}).show());
}

// --------------------------------------------------------------------------
// triangleCount()
// --------------------------------------------------------------------------
TEST_F(SparkIntegrationTest, TriangleCountReturnsOneRowPerVertex)
{
    EXPECT_EQ(gfCount(gf().triangleCount()), 4);
}

TEST_F(SparkIntegrationTest, TriangleCountHasCountColumn)
{
    EXPECT_THAT(gfColumns(gf().triangleCount()), Contains("count"));
}

TEST_F(SparkIntegrationTest, TriangleCountVerticesInTriangleNonZero)
{
    auto rows = gf().triangleCount().collect();
    std::map<int32_t, int64_t> counts;
    for (auto &row : rows)
        counts[row.get<int32_t>("id")] = row.get<int64_t>("count");

    EXPECT_GT(counts[1], 0);
    EXPECT_GT(counts[2], 0);
    EXPECT_GT(counts[3], 0);
}

TEST_F(SparkIntegrationTest, TriangleCountLeafVertexIsZero)
{
    auto rows = gf().triangleCount().collect();
    std::map<int32_t, int64_t> counts;
    for (auto &row : rows)
        counts[row.get<int32_t>("id")] = row.get<int64_t>("count");

    // --------------------------------------------------------
    // Vertex 4 (Anne) is a leaf — not part of any triangle.
    // --------------------------------------------------------
    EXPECT_EQ(counts[4], 0);
}

TEST_F(SparkIntegrationTest, TriangleCountShow)
{
    EXPECT_NO_THROW(gf().triangleCount().show());
}

// --------------------------------------------------------------------------
// labelPropagation() — omitted
//
// Important:
// Requires increased Spark executor memory in some cases.
// Especially when running alongside other long-running algorithms
// (ConnectedComponents, SCC, TriangleCount)
// --------------------------------------------------------------------------
TEST_F(SparkIntegrationTest, LabelPropagationReturnsOneRowPerVertex)
{
    EXPECT_EQ(gfCount(gf().labelPropagation(5)), 4);
}

TEST_F(SparkIntegrationTest, LabelPropagationHasLabelColumn)
{
    EXPECT_THAT(gfColumns(gf().labelPropagation(5)), Contains("label"));
}

// --------------------------------------------------------------------------
// Chaining - GraphFrames result into plain DataFrame ops
// --------------------------------------------------------------------------
TEST_F(SparkIntegrationTest, FindThenFilter)
{
    auto result = gf()
                      .find("(a)-[e]->(b)")
                      .filter("e.relationship = 'friend'");
    EXPECT_GT(gfCount(result), 0);
}

TEST_F(SparkIntegrationTest, PageRankThenFilter)
{
    auto result = gf().pageRank(0.15, 5).filter("pagerank > 0.0");
    EXPECT_GT(gfCount(result), 0);
}

TEST_F(SparkIntegrationTest, PageRankOnSubgraph)
{
    auto sub_v = vertices->filter("age >= 30");
    auto sub_e = edges->filter("src IN (1,2,3) AND dst IN (1,2,3)");
    EXPECT_EQ(gfCount(GraphFrame(sub_v, sub_e).pageRank(0.15, 3)), 3);
}