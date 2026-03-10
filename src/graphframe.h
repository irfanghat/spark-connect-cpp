#pragma once

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include <google/protobuf/any.pb.h>
#include <spark/connect/base.grpc.pb.h>

#include "dataframe.h"
#include "functions.h"

namespace org::graphframes::connect::proto
{
    class GraphFramesAPI;
    class ColumnOrExpression;
    class StorageLevel;
}

namespace graphframes
{
    /**
     * @class GraphFrame
     * @brief Represents a graph with vertices and edges stored as DataFrames.
     *
     * @param vertices DataFrame holding vertex information.
     * Must contain a column named `id` that stores unique vertex IDs.
     *
     * @param edges DataFrame holding edge information.
     * Must contain two columns `src` and `dst` storing source vertex IDs and destination vertex IDs of edges, respectively.
     *
     */
    class GraphFrame
    {
    public:
        /**
         * @brief Constructs a GraphFrame from vertex and edge DataFrames.
         *
         * @param vertices  DataFrame with at least a column `id`.
         * @param edges     DataFrame with at least columns `src` and `dst`.
         */
        GraphFrame(const DataFrame &vertices, const DataFrame &edges);

        /** @brief Returns the `vertex` DataFrame. */
        const DataFrame &vertices() const;

        /** @brief Returns the `edge` DataFrame. */
        const DataFrame &edges() const;

        /**
         * @brief Finds structural patterns in a graph using a motif DSL.
         *
         * Example patterns:
         *
         *   `(a)-[e]->(b)`               – Directed edge from `a` to `b`
         *
         *   `(a)-[e]->(b); (b)-[e2]->(c)` – Two-hop chain
         *
         *   `(a)-[]->(b)`                – Anonymous edge
         *
         * @param pattern  Motif pattern string.
         * @return A new DataFrame with one column per named vertex/edge in the pattern.
         */
        DataFrame find(const std::string &pattern) const;

        /**
         * @brief The triplets (source vertex)-[edge]->(destination vertex) for all edges in the graph.
         * Returns a DataFrame of all triplets (src vertex, edge, dst vertex).
         * @return DataFrame with columns: `src`, `edge`, `dst`.
         */
        DataFrame triplets() const;

        /**
         * @brief Returns a sub-graph containing only edges satisfying `condition`.
         * @param condition  SQL expression or Column applied to edge columns.
         */
        DataFrame filterEdges(const std::string &condition) const;

        /** @overload Column-expression variant. */
        DataFrame filterEdges(const spark::sql::functions::Column &condition) const;

        /**
         * @brief Filters the vertices based on expression, remove edges containing any dropped vertices.
         * Returns a sub-graph containing only vertices satisfying `condition`.
         * @param condition  String or Column describing the condition expression for filtering.
         */
        DataFrame filterVertices(const std::string &condition) const;

        /** @overload Column-expression variant. */
        DataFrame filterVertices(const spark::sql::functions::Column &condition) const;

        /**
         * @brief Returns a new GraphFrame with isolated vertices removed.
         * A vertex is isolated if it has no in- or out-edges.
         */
        DataFrame dropIsolatedVertices() const;

        // =======================================================================
        // The following section contains definitions for Algorithms that can be
        // applied to valid GraphFrames.
        // Reference: https://graphframes.io/api/python/graphframes.html#graphframes.GraphFrame.k_core
        // =======================================================================

        /**
         * @brief Runs PageRank until convergence or for a fixed number of iterations on the graph.
         *
         * Exactly one of `max_iter` or `tol` must be specified:
         *  - `max_iter` runs a fixed number of iterations.
         *  - `tol`      runs until the L1 norm of the rank change falls below the tolerance.
         *
         * @param reset_probability  Probability of a `teleport` (1 - damping factor). Default 0.15.
         * @param max_iter           Fixed iteration count (mutually exclusive with tol).
         * @param tol                Convergence tolerance (mutually exclusive with max_iter).
         * @param source_id_str      Optional source vertex id (string). Personalises PageRank.
         * @param source_id_long     Optional source vertex id (integer). Personalises PageRank.
         * @return DataFrame of vertices with "pagerank" column and edges with `weight`.
         */
        DataFrame pageRank(double reset_probability = 0.15,
                           std::optional<int> max_iter = std::nullopt,
                           std::optional<double> tol = std::nullopt,
                           std::optional<std::string> source_id_str = std::nullopt,
                           std::optional<int64_t> source_id_long = std::nullopt) const;

        /**
         * @brief Run the personalized PageRank algorithm on the graph, from the provided list of sources in parallel for a fixed number of iterations.
         *
         * @param source_ids_str   String vertex IDs used as personalisation sources.
         * @param reset_probability  Teleport probability.
         * @param max_iter         Number of iterations.
         * @return Vertex DataFrame with one pagerank column per source.
         */
        DataFrame parallelPersonalizedPageRank(
            const std::vector<std::string> &source_ids_str,
            double reset_probability = 0.15,
            int max_iter = 10) const;

        /** @overload Integer source IDs. */
        DataFrame parallelPersonalizedPageRank(
            const std::vector<int64_t> &source_ids_long,
            double reset_probability = 0.15,
            int max_iter = 10) const;

        /**
         * @brief Computes connected components.
         *
         * @param algorithm            `graphframes` (default) or `graphx`.
         * @param checkpoint_interval  How often to checkpoint (default 2).
         * @param broadcast_threshold  Threshold for broadcasting small graphs (default 1 000 000).
         * @return Vertex DataFrame with a "component" column (long).
         */
        DataFrame connectedComponents(
            const std::string &algorithm = "graphframes",
            int checkpoint_interval = 2,
            int broadcast_threshold = 1'000'000) const;

        /**
         * @brief Computes strongly connected components.
         * @param max_iter  Maximum number of iterations (default 10).
         * @return Vertex DataFrame with a `component` column.
         */
        DataFrame stronglyConnectedComponents(int max_iter = 10) const;

        /**
         * @brief Runs the Label Propagation Algorithm (LPA) for community detection.
         * Runs static label propagation for detecting communities in networks.
         * @param max_iter  Number of supersteps (default 5).
         * @return Vertex DataFrame with a "label" column.
         */
        DataFrame labelPropagation(int max_iter = 5) const;

        /**
         * @brief Computes shortest paths from every vertex to each landmark.
         *
         * @param landmarks  String vertex IDs to use as destinations.
         * @return Vertex DataFrame with a "distances" map column.
         */
        DataFrame shortestPaths(const std::vector<int32_t> &landmarks,
                                const std::string &algorithm = "graphframes") const;

        /** @overload Long vertex IDs as landmarks. */
        DataFrame shortestPaths(const std::vector<int64_t> &landmarks,
                                const std::string &algorithm = "graphframes") const;

        /** @overload Strings as landmarks. */
        DataFrame shortestPaths(const std::vector<std::string> &landmarks,
                                const std::string &algorithm = "graphframes") const;

        /**
         * @brief Counts the triangles passing through each vertex.
         * This impementation is based on the computing the intersection of vertices neighborhoods. It requires to collect the whole neighborhood of each vertex.
         * @return Vertex DataFrame with a "count" column.
         */
        DataFrame triangleCount() const;

        /**
         * @brief Breadth-first search between vertex sets.
         *
         * @param from_expr       SQL expression selecting source vertices (e.g. `id = 1`).
         * @param to_expr         SQL expression selecting destination vertices.
         * @param edge_filter     Optional SQL expression to filter traversable edges.
         * @param max_path_length Maximum number of edges in the path (default 10).
         * @return DataFrame containing columns for each vertex and edge along the shortest path.
         */
        DataFrame bfs(const std::string &from_expr,
                      const std::string &to_expr,
                      const std::string &edge_filter = "",
                      int max_path_length = 10) const;

        /** @overload Column-expression variant for from/to/edge_filter. */
        DataFrame bfs(const spark::sql::functions::Column &from_expr,
                      const spark::sql::functions::Column &to_expr,
                      std::optional<spark::sql::functions::Column> edge_filter = std::nullopt,
                      int max_path_length = 10) const;

        /**
         * @brief Finds all cycles in the graph.
         * An implementation of the Rocha–Thatte cycle detection algorithm.
         * Rocha, Rodrigo Caetano, and Bhalchandra D. Thatte.
         * `Distributed cycle detection in large-scale sparse graphs.`
         * Proceedings of Simpósio Brasileiro de Pesquisa Operacional (SBPO’15) (2015): 1-11.
         * @return Vertex DataFrame annotated with cycle membership (A DataFrame with unique cycles).
         */
        DataFrame detectingCycles() const;

        /**
         * @brief Computes the k-core decomposition of the graph.
         * The k-core is the maximal subgraph such that every vertex has at least degree k.
         * The k-core metric is a measure of the centrality of a node in a network,
         * based on its degree and the degrees of its neighbors.
         * Nodes with higher k-core values are considered to be more central and
         * influential within the network.
         * This implementation is based on the algorithm described in:
         * Mandal, Aritra, and Mohammad Al Hasan.
         * `A distributed k-core decomposition algorithm on spark.`
         * 2017 IEEE International Conference on Big Data (Big Data). IEEE, 2017.
         * @return Vertex DataFrame with a "coreness" column.
         */
        DataFrame kCore() const;

        /**
         * @brief Computes a Maximal Independent Set.
         * @param seed  Random seed for determinism.
         * @return Vertex DataFrame with an "inMIS" boolean column.
         */
        DataFrame maximalIndependentSet(int64_t seed = 0) const;

        /**
         * @brief Runs Power Iteration Clustering.
         * @param k           Number of clusters.
         * @param max_iter    Maximum number of iterations (default 20).
         * @param weight_col  Optional name of an edge column to use as weight.
         * @return DataFrame with vertex "id" and "cluster" columns.
         */
        DataFrame powerIterationClustering(int k,
                                           int max_iter = 20,
                                           std::optional<std::string> weight_col = std::nullopt) const;

        /**
         * @brief Runs SVD++ collaborative filtering.
         * @param rank      Number of latent factors.
         * @param max_iter  Number of iterations (default 2).
         * @param min_value Minimum rating value.
         * @param max_value Maximum rating value.
         * @param gamma1    Regularisation for user/item factors.
         * @param gamma2    Regularisation for biases.
         * @param gamma6    Learning rate for factors.
         * @param gamma7    Learning rate for biases.
         * @return DataFrame with user/item columns and predicted ratings.
         */
        DataFrame svdPlusPlus(int rank = 10,
                              int max_iter = 2,
                              double min_value = 0.0,
                              double max_value = 5.0,
                              double gamma1 = 0.007,
                              double gamma2 = 0.007,
                              double gamma6 = 0.005,
                              double gamma7 = 0.015) const;

    private:
        DataFrame vertices_;
        DataFrame edges_;

        /**
         * @brief Stamps vertices/edges bytes onto `api`, packs it into
         *        a `google.protobuf.Any`, and returns a DataFrame whose plan
         *        root is `Relation.extension = <packed Any>`.
         */
        DataFrame execute(org::graphframes::connect::proto::GraphFramesAPI &api) const;

        /**
         * @brief Populates a ColumnOrExpression from a plain SQL string (`expr variant`).
         */
        static void setExprStr(org::graphframes::connect::proto::ColumnOrExpression *coe,
                               const std::string &expr);

        /**
         * @brief Populates a ColumnOrExpression from a Column object
         *        by serialising the Column's inner Expression proto to bytes (`col variant`).
         */
        static void setExprCol(org::graphframes::connect::proto::ColumnOrExpression *coe,
                               const spark::sql::functions::Column &col);
    };

}