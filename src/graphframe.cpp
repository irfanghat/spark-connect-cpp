
#include <stdexcept>

#include <google/protobuf/any.pb.h>
#include <spark/connect/relations.pb.h>
#include <spark/connect/graphframes.pb.h>

#include "graphframe.h"
#include "functions.h"

using namespace spark::sql::functions;
using namespace org::graphframes::connect::proto;

namespace graphframes
{
    GraphFrame::GraphFrame(const DataFrame &vertices, const DataFrame &edges)
        : vertices_(vertices), edges_(edges)
    {
    }

    const DataFrame &GraphFrame::vertices() const { return vertices_; }
    const DataFrame &GraphFrame::edges() const { return edges_; }

    /**
     * @brief Stamps the serialised vertex/edge plans onto `api`, packs it into a
     *        `google::protobuf::Any`, and places the `Any` in the `Spark Connect
     *        Relation.extension` field (`tag 998`).  Returns the resulting DataFrame.
     *
     * The Spark Connect Relation proto has:
     *   `oneof rel_type {
     *     ...
     *     google.protobuf.Any extension = 998;
     *   }`
     *
     * @note
     * The C++ generated accessor is `mutable_extension()` which returns a
     * `google::protobuf::Any*`.  We call `PackFrom()` on it to serialise the
     * GraphFramesAPI payload with the correct type URL so the server-side plugin
     * can `UnpackTo()` / `Is<>()` it correctly.
     *
     * 
     * The vertices / edges DataFrame plans are stored as raw serialised bytes of
     * the underlying `spark::connect::Relation` proto.
     */
    DataFrame GraphFrame::execute(GraphFramesAPI &api) const
    {
        api.set_vertices(vertices_.plan_.SerializeAsString());
        api.set_edges(edges_.plan_.SerializeAsString());

        spark::connect::Plan plan;
        plan.mutable_root()->mutable_extension()->PackFrom(api);

        return DataFrame(vertices_.stub_, plan, vertices_.session_id_, vertices_.user_id_);
    }

    void GraphFrame::setExprStr(ColumnOrExpression *coe, const std::string &expr)
    {
        // -----------------------------------------------
        // ColumnOrExpression.expr (string variant)
        // -----------------------------------------------
        coe->set_expr(expr);
    }

    void GraphFrame::setExprCol(ColumnOrExpression *coe,
                                const spark::sql::functions::Column &col)
    {
        // --------------------------------------------------------------------------
        // ColumnOrExpression.col (bytes variant) - Serialise the inner
        // spark::connect::Expression proto to binary.
        // --------------------------------------------------------------------------
        if (!col.expr)
            throw std::invalid_argument("GraphFrame: Column expression is null.");
        coe->set_col(col.expr->SerializeAsString());
    }

    // =======================================================================
    // @note
    //
    // The following section contains definitions for the Core graph operations
    // Reference: https://graphframes.io/api/python/graphframes.html#graphframes.GraphFrame.k_core
    // =======================================================================

    DataFrame GraphFrame::find(const std::string &pattern) const
    {
        GraphFramesAPI api;
        api.mutable_find()->set_pattern(pattern);
        return execute(api);
    }

    DataFrame GraphFrame::triplets() const
    {
        GraphFramesAPI api;
        // ---------------------------------------------------------------------
        // Triplets message has no fields, just setting the oneof is enough.
        // ---------------------------------------------------------------------
        api.mutable_triplets();
        return execute(api);
    }

    DataFrame GraphFrame::filterEdges(const std::string &condition) const
    {
        GraphFramesAPI api;
        setExprStr(api.mutable_filter_edges()->mutable_condition(), condition);
        return execute(api);
    }

    DataFrame GraphFrame::filterEdges(const spark::sql::functions::Column &condition) const
    {
        GraphFramesAPI api;
        setExprCol(api.mutable_filter_edges()->mutable_condition(), condition);
        return execute(api);
    }

    DataFrame GraphFrame::filterVertices(const std::string &condition) const
    {
        GraphFramesAPI api;
        setExprStr(api.mutable_filter_vertices()->mutable_condition(), condition);
        return execute(api);
    }

    DataFrame GraphFrame::filterVertices(const spark::sql::functions::Column &condition) const
    {
        GraphFramesAPI api;
        setExprCol(api.mutable_filter_vertices()->mutable_condition(), condition);
        return execute(api);
    }

    DataFrame GraphFrame::dropIsolatedVertices() const
    {
        GraphFramesAPI api;

        // -------------------------------------------------------------
        // Empty message, presence signals the operation
        // -------------------------------------------------------------
        api.mutable_drop_isolated_vertices();
        return execute(api);
    }

    // =======================================================================
    // @note
    //
    // The following section contains definitions for Algorithms that can be
    // applied to valid GraphFrames.
    // Reference: https://graphframes.io/api/python/graphframes.html#graphframes.GraphFrame.k_core
    // =======================================================================

    DataFrame GraphFrame::pageRank(double reset_probability,
                                   std::optional<int> max_iter,
                                   std::optional<double> tol,
                                   std::optional<std::string> source_id_str,
                                   std::optional<int64_t> source_id_long) const
    {
        if (max_iter.has_value() && tol.has_value())
            throw std::invalid_argument("pageRank: specify either max_iter or tol, not both.");

        GraphFramesAPI api;
        auto *pr = api.mutable_page_rank();
        pr->set_reset_probability(reset_probability);

        if (max_iter.has_value())
            pr->set_max_iter(*max_iter);
        if (tol.has_value())
            pr->set_tol(*tol);

        // ---------------------------------------------------
        // Optional personalised source vertex
        // ---------------------------------------------------
        if (source_id_str.has_value())
            pr->mutable_source_id()->set_string_id(*source_id_str);
        else if (source_id_long.has_value())
            pr->mutable_source_id()->set_long_id(*source_id_long);

        return execute(api);
    }

    DataFrame GraphFrame::parallelPersonalizedPageRank(
        const std::vector<std::string> &source_ids_str,
        double reset_probability,
        int max_iter) const
    {
        GraphFramesAPI api;
        auto *pppr = api.mutable_parallel_personalized_page_rank();
        pppr->set_reset_probability(reset_probability);
        pppr->set_max_iter(max_iter);
        for (const auto &id : source_ids_str)
            pppr->add_source_ids()->set_string_id(id);
        return execute(api);
    }

    DataFrame GraphFrame::parallelPersonalizedPageRank(
        const std::vector<int64_t> &source_ids_long,
        double reset_probability,
        int max_iter) const
    {
        GraphFramesAPI api;
        auto *pppr = api.mutable_parallel_personalized_page_rank();
        pppr->set_reset_probability(reset_probability);
        pppr->set_max_iter(max_iter);
        for (const auto &id : source_ids_long)
            pppr->add_source_ids()->set_long_id(id);
        return execute(api);
    }

    DataFrame GraphFrame::connectedComponents(const std::string &algorithm,
                                              int checkpoint_interval,
                                              int broadcast_threshold) const
    {
        GraphFramesAPI api;
        auto *cc = api.mutable_connected_components();
        cc->set_algorithm(algorithm);
        cc->set_checkpoint_interval(checkpoint_interval);
        cc->set_broadcast_threshold(broadcast_threshold);

        // ----------------------------------------------------------------
        // @note
        // set_use_local_checkpoints() avoids needing sc.setCheckpointDir()
        // We probably need to support this at the Spark Session
        // ----------------------------------------------------------------
        cc->set_use_local_checkpoints(true);
        return execute(api);
    }

    DataFrame GraphFrame::stronglyConnectedComponents(int max_iter) const
    {
        GraphFramesAPI api;
        api.mutable_strongly_connected_components()->set_max_iter(max_iter);
        return execute(api);
    }

    DataFrame GraphFrame::labelPropagation(int max_iter) const
    {
        GraphFramesAPI api;
        auto *lp = api.mutable_label_propagation();
        lp->set_algorithm("graphframes");
        lp->set_max_iter(max_iter);
        return execute(api);
    }

    DataFrame GraphFrame::shortestPaths(const std::vector<int32_t> &landmarks,
                                        const std::string &algorithm) const
    {
        GraphFramesAPI api;
        auto *sp = api.mutable_shortest_paths();
        sp->set_algorithm(algorithm);
        for (const auto &lm : landmarks)
            sp->add_landmarks()->set_long_id(static_cast<int64_t>(lm));

        auto castedVertices = vertices_.withColumn("id", col("id").cast("bigint"));
        return GraphFrame(castedVertices, edges_).execute(api);
    }

    DataFrame GraphFrame::shortestPaths(const std::vector<int64_t> &landmarks,
                                        const std::string &algorithm) const
    {
        GraphFramesAPI api;
        auto *sp = api.mutable_shortest_paths();
        sp->set_algorithm(algorithm);
        for (const auto &lm : landmarks)
            sp->add_landmarks()->set_long_id(lm);

        auto castedVertices = vertices_.withColumn("id", col("id").cast("bigint"));
        return GraphFrame(castedVertices, edges_).execute(api);
    }

    DataFrame GraphFrame::shortestPaths(const std::vector<std::string> &landmarks,
                                        const std::string &algorithm) const
    {
        GraphFramesAPI api;
        auto *sp = api.mutable_shortest_paths();
        sp->set_algorithm(algorithm);
        for (const auto &lm : landmarks)
            sp->add_landmarks()->set_string_id(lm);

        return execute(api);
    }

    DataFrame GraphFrame::triangleCount() const
    {
        GraphFramesAPI api;

        // -----------------------------------------------
        // Empty, presence signals the operation
        // -----------------------------------------------
        api.mutable_triangle_count();
        return execute(api);
    }

    DataFrame GraphFrame::bfs(const std::string &from_expr,
                              const std::string &to_expr,
                              const std::string &edge_filter,
                              int max_path_length) const
    {
        GraphFramesAPI api;
        auto *b = api.mutable_bfs();
        setExprStr(b->mutable_from_expr(), from_expr);
        setExprStr(b->mutable_to_expr(), to_expr);
        setExprStr(b->mutable_edge_filter(), edge_filter.empty() ? "true" : edge_filter);
        b->set_max_path_length(max_path_length);
        return execute(api);
    }

    DataFrame GraphFrame::bfs(const spark::sql::functions::Column &from_expr,
                              const spark::sql::functions::Column &to_expr,
                              std::optional<spark::sql::functions::Column> edge_filter,
                              int max_path_length) const
    {
        GraphFramesAPI api;
        auto *b = api.mutable_bfs();
        setExprCol(b->mutable_from_expr(), from_expr);
        setExprCol(b->mutable_to_expr(), to_expr);

        if (edge_filter.has_value())
            setExprCol(b->mutable_edge_filter(), *edge_filter);
        else
            setExprStr(b->mutable_edge_filter(), "true");
        b->set_max_path_length(max_path_length);
        return execute(api);
    }

    DataFrame GraphFrame::detectingCycles() const
    {
        GraphFramesAPI api;
        api.mutable_detecting_cycles();
        return execute(api);
    }

    DataFrame GraphFrame::kCore() const
    {
        GraphFramesAPI api;
        api.mutable_kcore();
        return execute(api);
    }

    DataFrame GraphFrame::maximalIndependentSet(int64_t seed) const
    {
        GraphFramesAPI api;
        auto *mis = api.mutable_mis();
        mis->set_seed(seed);
        return execute(api);
    }

    DataFrame GraphFrame::powerIterationClustering(int k,
                                                   int max_iter,
                                                   std::optional<std::string> weight_col) const
    {
        GraphFramesAPI api;
        auto *pic = api.mutable_power_iteration_clustering();
        pic->set_k(k);
        pic->set_max_iter(max_iter);
        if (weight_col.has_value())
            pic->set_weight_col(*weight_col);
        return execute(api);
    }

    DataFrame GraphFrame::svdPlusPlus(int rank,
                                      int max_iter,
                                      double min_value,
                                      double max_value,
                                      double gamma1,
                                      double gamma2,
                                      double gamma6,
                                      double gamma7) const
    {
        GraphFramesAPI api;
        auto *svd = api.mutable_svd_plus_plus();
        svd->set_rank(rank);
        svd->set_max_iter(max_iter);
        svd->set_min_value(min_value);
        svd->set_max_value(max_value);
        svd->set_gamma1(gamma1);
        svd->set_gamma2(gamma2);
        svd->set_gamma6(gamma6);
        svd->set_gamma7(gamma7);
        return execute(api);
    }
}