#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <iostream>
#include <sstream>

#include "config.h"
#include "dataframe.h"
#include "functions.h"
#include "graphframe.h"
#include "session.h"
#include "types.h"

using namespace graphframes;
using namespace spark::sql::functions;
using namespace spark::sql::types;
using ::testing::Contains;
using ::testing::ElementsAre;
#include "spark_fixture.h"

// ---------------------------------------------------------------------------------
// SCENARIO: Fraud Ring Detection
//
// A compliance team suspects a group of accounts are cycling funds in a closed
// loop to simulate legitimate transaction volume ("layering").
//
// Graph: Directed transfer network between accounts.
// Accounts 101 → 102 → 103 → 101 form a closed cycle (The fraud ring).
// Account 104 wires into 102 but receives nothing back (Mule / Entry point).
// Account 105 is completely dormant, no transfers in or out.
//
// ConnectedComponents (Undirected) must group 101, 102, 103, 104 into one
// cluster and leave 105 in its own cluster.
// SCC must isolate 104 (No path back to it from within the ring).
// ---------------------------------------------------------------------------------
TEST_F(SparkIntegrationTest, FraudRing_ConnectedComponentsGroupsRingAndMule)
{
    auto accounts = spark->sql(R"(
        SELECT CAST(id AS INT) AS id, name, CAST(balance AS DOUBLE) AS balance FROM VALUES
            (101, 'Shell Corp A',  500000.0),
            (102, 'Shell Corp B',  320000.0),
            (103, 'Shell Corp C',  410000.0),
            (104, 'Mule Account',   15000.0),
            (105, 'Dormant Acct',       0.0)
        AS t(id, name, balance)
    )");

    auto transfers = spark->sql(R"(
        SELECT CAST(src AS INT) AS src, CAST(dst AS INT) AS dst, amount FROM VALUES
            (101, 102, 200000.0),
            (102, 103, 195000.0),
            (103, 101, 190000.0),
            (104, 102,  15000.0)
        AS t(src, dst, amount)
    )");

    auto components = GraphFrame(accounts, transfers).connectedComponents().collect();

    ASSERT_EQ(components.size(), 5u);

    std::map<int32_t, int64_t> comp_by_id;
    for (auto& row : components)
        comp_by_id[row.get<int32_t>("id")] = row.get<int64_t>("component");

    // --------------------------------------------------------------------------------
    // The fraud ring (101, 102, 103) and the mule (104) are all reachable
    // from each other via undirected edges, so they must share one
    // component id.
    // --------------------------------------------------------------------------------
    EXPECT_EQ(comp_by_id[101], comp_by_id[102]);
    EXPECT_EQ(comp_by_id[102], comp_by_id[103]);
    EXPECT_EQ(comp_by_id[103], comp_by_id[104]);

    // --------------------------------------------------------------------------------
    // The dormant account is its own island.
    // --------------------------------------------------------------------------------
    EXPECT_NE(comp_by_id[105], comp_by_id[101]);
}

TEST_F(SparkIntegrationTest, FraudRing_SCCIsolatesMuleFromRing)
{
    auto accounts = spark->sql(R"(
        SELECT CAST(id AS INT) AS id, name FROM VALUES
            (101, 'Shell Corp A'),
            (102, 'Shell Corp B'),
            (103, 'Shell Corp C'),
            (104, 'Mule Account')
        AS t(id, name)
    )");

    auto transfers = spark->sql(R"(
        SELECT CAST(src AS INT) AS src, CAST(dst AS INT) AS dst FROM VALUES
            (101, 102),
            (102, 103),
            (103, 101),
            (104, 102)
        AS t(src, dst)
    )");

    auto components = GraphFrame(accounts, transfers).stronglyConnectedComponents(10).collect();

    std::map<int32_t, int64_t> scc_by_id;
    for (auto& row : components)
        scc_by_id[row.get<int32_t>("id")] = row.get<int64_t>("component");

    // --------------------------------------------------------------------------------
    // 101 → 102 → 103 → 101 is a directed cycle: One SCC.
    // --------------------------------------------------------------------------------
    EXPECT_EQ(scc_by_id[101], scc_by_id[102]);
    EXPECT_EQ(scc_by_id[102], scc_by_id[103]);

    // --------------------------------------------------------------------------------
    // 104 pushes funds into 102 but nothing flows back: Isolated SCC.
    // --------------------------------------------------------------------------------
    EXPECT_NE(scc_by_id[104], scc_by_id[101]);
}

TEST_F(SparkIntegrationTest, FraudRing_MotifDetectsCyclicTransferPattern)
{
    // --------------------------------------------------------------------------------
    // Compliance rule: flag any trio of accounts where A→B, B→C, C→A
    // (A "round-trip triangle") - the canonical layering signature.
    // --------------------------------------------------------------------------------
    auto accounts = spark->sql(R"(
        SELECT CAST(id AS INT) AS id FROM VALUES (101),(102),(103),(104) AS t(id)
    )");

    auto transfers = spark->sql(R"(
        SELECT CAST(src AS INT) AS src, CAST(dst AS INT) AS dst FROM VALUES
            (101, 102),
            (102, 103),
            (103, 101),
            (104, 102)
        AS t(src, dst)
    )");

    auto rings = GraphFrame(accounts, transfers)
                     .find("(a)-[e1]->(b); (b)-[e2]->(c); (c)-[e3]->(a)")
                     .collect();

    // --------------------------------------------------------------------------------
    // Exactly one directed 3-cycle exists: 101→102→103→101.
    // GraphFrames returns one row per rotation, so 3 rows.
    // --------------------------------------------------------------------------------
    EXPECT_EQ(rings.size(), 3u);
}

// ---------------------------------------------------------------------------------
// SCENARIO: Payment Authorization - Contagion Distance
//
// A sanctions-screening engine must compute how many hops a payment touches
// before reaching a flagged counterparty ("contagion radius"). Payments travel
// through correspondent banks; if any bank within 2 hops of a flagged entity
// is involved, the transaction is blocked.
//
// Flagged landmark: bank 9 (OFAC-listed).
// ShortestPaths from every bank to landmark 9 reveals which banks are within
// the contagion radius and must be blocked.
//
// NOTE: `distances` is a Spark MAP<INT,INT>, surfaced in the Row as
// shared_ptr<MapData>. Keys and values are ColumnValues stored as int32_t.
// ---------------------------------------------------------------------------------
TEST_F(SparkIntegrationTest, Sanctions_BankWithinOneHopOfFlaggedEntity)
{
    auto banks = spark->sql(R"(
        SELECT CAST(id AS INT) AS id, name FROM VALUES
            (1, 'Originating Bank'),
            (2, 'Correspondent A'),
            (9, 'OFAC Listed Bank')
        AS t(id, name)
    )");

    auto wires = spark->sql(R"(
        SELECT CAST(src AS INT) AS src, CAST(dst AS INT) AS dst FROM VALUES
            (1, 2),
            (2, 9)
        AS t(src, dst)
    )");

    auto sp = GraphFrame(banks, wires).shortestPaths(std::vector<int32_t>{9}).collect();

    // --------------------------------------------------------------------------------
    // Scan a MapData for a given landmark, returning the hop count if
    // found.
    //
    // ShortestPaths passes INT32 landmarks but Pregel promotes the
    // distances MAP key type to LongType (int64_t) internally, so the
    // ColumnValue stored in map_ptr->keys holds int64_t at runtime even
    // when ids are INT32. We use std::visit with arithmetic widening — the
    // same strategy Row::get uses — so the comparison and extraction are
    // type-agnostic.
    // --------------------------------------------------------------------------------
    auto hop_count = [](const Row& row, int64_t landmark) -> std::optional<int32_t>
    {
        auto map_ptr = row.get<std::shared_ptr<MapData>>("distances");
        if (!map_ptr)
            return std::nullopt;
        for (size_t i = 0; i < map_ptr->keys.size(); ++i)
        {
            int64_t key = std::visit(
                [](auto&& arg) -> int64_t
                {
                    using T = std::decay_t<decltype(arg)>;
                    if constexpr (std::is_arithmetic_v<T> && !std::is_same_v<T, bool>)
                        return static_cast<int64_t>(arg);
                    throw std::runtime_error("ShortestPaths: unexpected non-numeric "
                                             "map key");
                },
                map_ptr->keys[i]);

            if (key != landmark)
                continue;

            return std::visit(
                [](auto&& arg) -> int32_t
                {
                    using T = std::decay_t<decltype(arg)>;
                    if constexpr (std::is_arithmetic_v<T> && !std::is_same_v<T, bool>)
                        return static_cast<int32_t>(arg);
                    throw std::runtime_error("ShortestPaths: unexpected non-numeric "
                                             "map value");
                },
                map_ptr->values[i]);
        }
        return std::nullopt;
    };

    std::map<int32_t, Row> row_by_id;
    for (auto& row : sp)
        row_by_id.emplace(row.get<int32_t>("id"), row);

    // --------------------------------------------------------------------------------
    // Correspondent A is exactly 1 hop from the flagged bank.
    // --------------------------------------------------------------------------------
    auto dist_2 = hop_count(row_by_id.at(2), 9);
    ASSERT_TRUE(dist_2.has_value());
    EXPECT_EQ(*dist_2, 1);

    // --------------------------------------------------------------------------------
    // Originating bank is 2 hops away — within contagion radius.
    // --------------------------------------------------------------------------------
    auto dist_1 = hop_count(row_by_id.at(1), 9);
    ASSERT_TRUE(dist_1.has_value());
    EXPECT_EQ(*dist_1, 2);

    // --------------------------------------------------------------------------------
    // The flagged bank itself is 0 hops from itself.
    // --------------------------------------------------------------------------------
    auto dist_9 = hop_count(row_by_id.at(9), 9);
    ASSERT_TRUE(dist_9.has_value());
    EXPECT_EQ(*dist_9, 0);
}

TEST_F(SparkIntegrationTest, Sanctions_IsolatedBankHasNoPathToFlaggedEntity)
{
    auto banks = spark->sql(R"(
        SELECT CAST(id AS INT) AS id, name FROM VALUES
            (1, 'Clean Bank A'),
            (2, 'Clean Bank B'),
            (9, 'OFAC Listed Bank')
        AS t(id, name)
    )");

    // --------------------------------------------------------------------------------
    // Wire network that does NOT connect to bank 9.
    // --------------------------------------------------------------------------------
    auto wires = spark->sql(R"(
        SELECT CAST(src AS INT) AS src, CAST(dst AS INT) AS dst FROM VALUES
            (1, 2)
        AS t(src, dst)
    )");

    auto sp = GraphFrame(banks, wires).shortestPaths(std::vector<int32_t>{9}).collect();

    // --------------------------------------------------------------------------------
    // Banks 1 and 2 have no directed path to bank 9.
    // Their MapData must either be null or contain no entry for landmark 9.
    // --------------------------------------------------------------------------------
    for (auto& row : sp)
    {
        int32_t bank_id = row.get<int32_t>("id");
        if (bank_id == 9)
            continue; // skip the landmark itself

        auto map_ptr = row.get<std::shared_ptr<MapData>>("distances");
        bool has_path_to_9 = false;
        if (map_ptr)
        {
            for (size_t i = 0; i < map_ptr->keys.size(); ++i)
            {
                if (std::get<int32_t>(map_ptr->keys[i]) == 9)
                {
                    has_path_to_9 = true;
                    break;
                }
            }
        }
        EXPECT_FALSE(has_path_to_9) << "Bank id=" << bank_id << " should have no path to bank 9";
    }
}

// ---------------------------------------------------------------------------------
// SCENARIO: Know-Your-Customer (KYC) Beneficial Ownership
//
// Regulators require identifying the "ultimate beneficial owner" (UBO) of
// a corporate structure through chains of ownership.  BFS traversal discovers
// whether two entities are connected within a legal ownership depth limit
// (e.g., max 3 hops).
//
// Structure:
//   HoldCo → SubCo A → OpCo      (2 hops, within limit)
//   HoldCo → SubCo A → SubCo B → ShellCo (3 hops, at the limit)
//   TrustA  (no edges — appears unrelated; BFS must confirm)
// ---------------------------------------------------------------------------------
TEST_F(SparkIntegrationTest, KYC_BFSFindsOwnershipChainWithinDepthLimit)
{
    auto entities = spark->sql(R"(
        SELECT CAST(id AS INT) AS id, name FROM VALUES
            (1, 'HoldCo'),
            (2, 'SubCo A'),
            (3, 'OpCo'),
            (4, 'SubCo B'),
            (5, 'ShellCo'),
            (6, 'TrustA')
        AS t(id, name)
    )");

    auto ownership = spark->sql(R"(
        SELECT CAST(src AS INT) AS src, CAST(dst AS INT) AS dst FROM VALUES
            (1, 2),
            (2, 3),
            (2, 4),
            (4, 5)
        AS t(src, dst)
    )");

    // --------------------------------------------------------------------------------
    // HoldCo → SubCo A → OpCo: 2-hop path must exist.
    // --------------------------------------------------------------------------------
    auto path_to_opco = GraphFrame(entities, ownership).bfs("id = 1", "id = 3").collect();
    EXPECT_GT(path_to_opco.size(), 0u);

    // --------------------------------------------------------------------------------
    // HoldCo → SubCo A → SubCo B → ShellCo: 3-hop path must exist.
    // --------------------------------------------------------------------------------
    auto path_to_shell = GraphFrame(entities, ownership).bfs("id = 1", "id = 5").collect();
    EXPECT_GT(path_to_shell.size(), 0u);
}

TEST_F(SparkIntegrationTest, KYC_BFSConfirmsUnrelatedEntityHasNoOwnershipPath)
{
    auto entities = spark->sql(R"(
        SELECT CAST(id AS INT) AS id, name FROM VALUES
            (1, 'HoldCo'),
            (2, 'SubCo A'),
            (6, 'TrustA')
        AS t(id, name)
    )");

    auto ownership = spark->sql(R"(
        SELECT CAST(src AS INT) AS src, CAST(dst AS INT) AS dst FROM VALUES
            (1, 2)
        AS t(src, dst)
    )");

    // --------------------------------------------------------------------------------
    // TrustA (id=6) has no ownership link to HoldCo: BFS must return empty.
    // --------------------------------------------------------------------------------
    auto path = GraphFrame(entities, ownership).bfs("id = 1", "id = 6").collect();
    EXPECT_EQ(path.size(), 0u);
}

TEST_F(SparkIntegrationTest, KYC_DropIsolatedVerticesRemovesOrphanTrust)
{
    auto entities = spark->sql(R"(
        SELECT CAST(id AS INT) AS id, name FROM VALUES
            (1, 'HoldCo'),
            (2, 'SubCo A'),
            (6, 'TrustA')
        AS t(id, name)
    )");

    auto ownership = spark->sql(R"(
        SELECT CAST(src AS INT) AS src, CAST(dst AS INT) AS dst FROM VALUES
            (1, 2)
        AS t(src, dst)
    )");

    // --------------------------------------------------------------------------------
    // TrustA appears in the registry but has no ownership edges.
    // dropIsolatedVertices must remove it, reducing the vertex count from 3
    // to 2.
    // --------------------------------------------------------------------------------
    auto active = GraphFrame(entities, ownership).dropIsolatedVertices().collect();
    EXPECT_EQ(active.size(), 2u);

    std::set<int32_t> active_ids;
    for (auto& row : active)
        active_ids.insert(row.get<int32_t>("id"));

    EXPECT_TRUE(active_ids.count(1));
    EXPECT_TRUE(active_ids.count(2));
    EXPECT_FALSE(active_ids.count(6));
}

// ---------------------------------------------------------------------------------
// SCENARIO: High-Frequency Trading — Venue Influence Scoring
//
// A market microstructure team models order routing as a directed graph:
// edges represent a "fills at venue A triggered a resting order at venue B"
// relationship observed in historical executions.  PageRank measures which
// venues are the most influential price-setters ("price leaders").
//
// Venues: NYSE, NASDAQ, BATS, IEX, CBOE
// Known from academic literature: NYSE and NASDAQ dominate price discovery.
// The test asserts that their combined PageRank exceeds that of the smaller
// venues after convergence.
// ---------------------------------------------------------------------------------
TEST_F(SparkIntegrationTest, HFT_PageRankIdentifiesPriceLeaderVenues)
{
    auto venues = spark->sql(R"(
        SELECT CAST(id AS INT) AS id, name FROM VALUES
            (1, 'NYSE'),
            (2, 'NASDAQ'),
            (3, 'BATS'),
            (4, 'IEX'),
            (5, 'CBOE')
        AS t(id, name)
    )");

    // --------------------------------------------------------------------------------
    // Edge weight encodes observed influence frequency, here we model it
    // structurally: NYSE and NASDAQ receive many in-edges from satellite
    // venues, reflecting that satellite venues follow their price moves.
    // --------------------------------------------------------------------------------
    auto influences = spark->sql(R"(
        SELECT CAST(src AS INT) AS src, CAST(dst AS INT) AS dst FROM VALUES
            (3, 1),
            (4, 1),
            (5, 1),
            (3, 2),
            (4, 2),
            (5, 2),
            (1, 2),
            (2, 1)
        AS t(src, dst)
    )");

    auto pr_rows = GraphFrame(venues, influences).pageRank(0.15, 20).collect();

    std::map<int32_t, double> rank_by_id;
    for (auto& row : pr_rows)
        rank_by_id[row.get<int32_t>("id")] = row.get<double>("pagerank");

    // --------------------------------------------------------------------------------
    // NYSE (1) and NASDAQ (2) each have 3 + 1 in-edges, they must outrank
    // every satellite venue individually.
    // --------------------------------------------------------------------------------
    EXPECT_GT(rank_by_id[1], rank_by_id[3]);
    EXPECT_GT(rank_by_id[1], rank_by_id[4]);
    EXPECT_GT(rank_by_id[1], rank_by_id[5]);
    EXPECT_GT(rank_by_id[2], rank_by_id[3]);
    EXPECT_GT(rank_by_id[2], rank_by_id[4]);
    EXPECT_GT(rank_by_id[2], rank_by_id[5]);
}

// ---------------------------------------------------------------------------------
// SCENARIO: Insider Trading Network — Triangle Detection
//
// A market-surveillance engine flags clusters of traders who mutually share
// non-public information.  A directed triangle A→B, B→C, C→A in a "tipped"
// communication graph indicates a self-reinforcing information-sharing ring —
// the classical signature of an insider ring.
//
// triangleCount() identifies which traders participated in at least one
// closed information loop; those with count > 0 are escalated for review.
// ---------------------------------------------------------------------------------
TEST_F(SparkIntegrationTest, InsiderTrading_TriangleCountFlagsInformationSharingRing)
{
    auto traders = spark->sql(R"(
        SELECT CAST(id AS INT) AS id, name FROM VALUES
            (1, 'Trader Alpha'),
            (2, 'Trader Beta'),
            (3, 'Trader Gamma'),
            (4, 'Analyst A')
        AS t(id, name)
    )");

    // --------------------------------------------------------------------------------
    // Alpha → Beta → Gamma → Alpha: closed ring.
    // Analyst A tips Alpha only (no cycle back).
    // --------------------------------------------------------------------------------
    auto comms = spark->sql(R"(
        SELECT CAST(src AS INT) AS src, CAST(dst AS INT) AS dst FROM VALUES
            (1, 2),
            (2, 3),
            (3, 1),
            (4, 1)
        AS t(src, dst)
    )");

    auto tc_rows = GraphFrame(traders, comms).triangleCount().collect();

    std::map<int32_t, int64_t> count_by_id;
    for (auto& row : tc_rows)
        count_by_id[row.get<int32_t>("id")] = row.get<int64_t>("count");

    // --------------------------------------------------------------------------------
    // Traders 1, 2, 3 are inside the ring - triangle count must be
    // non-zero.
    // --------------------------------------------------------------------------------
    EXPECT_GT(count_by_id[1], 0LL);
    EXPECT_GT(count_by_id[2], 0LL);
    EXPECT_GT(count_by_id[3], 0LL);

    // --------------------------------------------------------------------------------
    // Analyst A is a tip source but not part of any cycle - not flagged.
    // --------------------------------------------------------------------------------
    EXPECT_EQ(count_by_id[4], 0LL);
}

// ---------------------------------------------------------------------------------
// SCENARIO: Credit Risk Contagion - Counterparty Exposure Propagation
//
// A prime broker must model how a single default propagates through its
// counterparty network.  The "contagion graph" is a directed graph where
// edge A→B means "A owes B an exposure that, if unpaid, stresses B."
//
// Strongly-connected components reveal clusters where every member both owes
// and is owed by every other member — a "mutual exposure cluster" where a
// single default can cascade and take down the whole group.
//
// The test models the 2008-era pattern:
//   Bank A ↔ Bank B ↔ Bank C (circular guarantees) → one SCC, systemic risk.
//   Hedge Fund X → Bank A only (no return leg) → isolated SCC, contained risk.
// ---------------------------------------------------------------------------------
TEST_F(SparkIntegrationTest, CreditRisk_SCCIdentifiesMutualExposureCluster)
{
    auto counterparties = spark->sql(R"(
        SELECT CAST(id AS INT) AS id, name FROM VALUES
            (1, 'Bank A'),
            (2, 'Bank B'),
            (3, 'Bank C'),
            (4, 'Hedge Fund X')
        AS t(id, name)
    )");

    auto exposures = spark->sql(R"(
        SELECT CAST(src AS INT) AS src, CAST(dst AS INT) AS dst, CAST(notional AS DOUBLE) AS notional FROM VALUES
            (1, 2, 1.2e9),
            (2, 3, 0.9e9),
            (3, 1, 1.1e9),
            (4, 1, 0.3e9)
        AS t(src, dst, notional)
    )");

    auto scc_rows = GraphFrame(counterparties, exposures).stronglyConnectedComponents(10).collect();

    std::map<int32_t, int64_t> scc_by_id;
    for (auto& row : scc_rows)
        scc_by_id[row.get<int32_t>("id")] = row.get<int64_t>("component");

    // --------------------------------------------------------------------------------
    // Banks A, B, C form a mutual exposure cluster - one SCC.
    // --------------------------------------------------------------------------------
    EXPECT_EQ(scc_by_id[1], scc_by_id[2]);
    EXPECT_EQ(scc_by_id[2], scc_by_id[3]);

    // --------------------------------------------------------------------------------
    // Hedge Fund X has a one-way exposure: not part of the systemic
    // cluster.
    // --------------------------------------------------------------------------------
    EXPECT_NE(scc_by_id[4], scc_by_id[1]);
}

TEST_F(SparkIntegrationTest, CreditRisk_BFSTracesContagionPathFromDefaultingEntity)
{
    auto counterparties = spark->sql(R"(
        SELECT CAST(id AS INT) AS id, name FROM VALUES
            (1, 'Defaulting Bank'),
            (2, 'Exposed Bank A'),
            (3, 'Exposed Bank B'),
            (4, 'Insulated Bank')
        AS t(id, name)
    )");

    // --------------------------------------------------------------------------------
    // Contagion flows: default at 1 stresses 2, which stresses 3.
    // Bank 4 has exposure to 3 but through a CDS hedge (modelled as no
    // edge).
    // --------------------------------------------------------------------------------
    auto exposures = spark->sql(R"(
        SELECT CAST(src AS INT) AS src, CAST(dst AS INT) AS dst FROM VALUES
            (1, 2),
            (2, 3)
        AS t(src, dst)
    )");

    // --------------------------------------------------------------------------------
    // Contagion path from the defaulting bank to Bank B must exist.
    // --------------------------------------------------------------------------------
    auto path = GraphFrame(counterparties, exposures).bfs("id = 1", "id = 3").collect();
    EXPECT_GT(path.size(), 0u);

    // --------------------------------------------------------------------------------
    // Insulated Bank (4) is not reachable from the defaulting entity.
    // --------------------------------------------------------------------------------
    auto no_path = GraphFrame(counterparties, exposures).bfs("id = 1", "id = 4").collect();
    EXPECT_EQ(no_path.size(), 0u);
}

// ---------------------------------------------------------------------------------
// SCENARIO: Anti-Money Laundering - Layering via Intermediary Accounts
//
// AML typology: "layering" moves funds through chains of intermediary accounts
// to obscure origin.  A two-hop motif (a)→(b)→(c) where the amounts are
// "nearly equal" (within 2%) is a structuring signal — the intermediary takes
// a thin cut to avoid triggering round-number detectors.
//
// find() with "(a)-[e1]->(b); (b)-[e2]->(c)" exposes all two-hop chains.
// The test asserts the correct number of chains and that the intermediate
// account (b) appears in every row.
//
// NOTE: Named vertex columns from find() are Spark structs, surfaced in the
// Row as shared_ptr<Row>. Dereference before calling get<> on inner fields.
// ---------------------------------------------------------------------------------
TEST_F(SparkIntegrationTest, AML_MotifFindsTwoHopLayeringChains)
{
    auto accounts = spark->sql(R"(
        SELECT CAST(id AS INT) AS id, name FROM VALUES
            (1, 'Origin Account'),
            (2, 'Layering Shell'),
            (3, 'Destination A'),
            (4, 'Destination B')
        AS t(id, name)
    )");

    // --------------------------------------------------------------------------------
    // Origin → Shell → Destination A  (chain 1)
    // Origin → Shell → Destination B  (chain 2)
    // --------------------------------------------------------------------------------
    auto transfers = spark->sql(R"(
        SELECT CAST(src AS INT) AS src, CAST(dst AS INT) AS dst, CAST(amount AS DOUBLE) AS amount FROM VALUES
            (1, 2, 99800.0),
            (2, 3, 99600.0),
            (2, 4, 99601.0)
        AS t(src, dst, amount)
    )");

    auto chains = GraphFrame(accounts, transfers).find("(a)-[e1]->(b); (b)-[e2]->(c)").collect();

    // --------------------------------------------------------------------------------
    // Two distinct two-hop chains exist.
    // --------------------------------------------------------------------------------
    EXPECT_EQ(chains.size(), 2u);

    // --------------------------------------------------------------------------------
    // In every chain, the middle account (b) must be the layering shell
    // (id=2).
    // --------------------------------------------------------------------------------
    for (auto& row : chains)
    {
        auto b_ptr = row.get<std::shared_ptr<Row>>("b");
        ASSERT_NE(b_ptr, nullptr);
        EXPECT_EQ(b_ptr->get<int32_t>("id"), 2);
    }
}

TEST_F(SparkIntegrationTest, AML_FilterEdgesIsolatesHighValueTransfers)
{
    auto accounts = spark->sql(R"(
        SELECT CAST(id AS INT) AS id, name FROM VALUES
            (1, 'Origin'),
            (2, 'Shell A'),
            (3, 'Shell B'),
            (4, 'Destination')
        AS t(id, name)
    )");

    // --------------------------------------------------------------------------------
    // Mix of transfers: only those above 50 000 are SAR-reportable
    // threshold.
    // --------------------------------------------------------------------------------
    auto transfers = spark->sql(R"(
        SELECT CAST(src AS INT) AS src, CAST(dst AS INT) AS dst, CAST(amount AS DOUBLE) AS amount FROM VALUES
            (1, 2, 75000.0),
            (2, 3, 12000.0),
            (3, 4, 68000.0)
        AS t(src, dst, amount)
    )");

    auto reportable = GraphFrame(accounts, transfers).filterEdges("amount > 50000").collect();

    EXPECT_EQ(reportable.size(), 2u);
}

// ---------------------------------------------------------------------------------
// SCENARIO: Trade Surveillance - Market Manipulation via Wash Trading
//
// Wash trading creates artificial volume by having the same beneficial owner
// buy and sell through different accounts simultaneously.  In a graph where
// edges represent same-second cross-account trades, a two-node SCC (A→B and
// B→A within the same second) is the exact structural fingerprint.
//
// The test sets up wash-trade pairs and non-wash control pairs, then uses
// SCC to identify which account-pairs exhibit the bidirectional pattern.
// ---------------------------------------------------------------------------------
TEST_F(SparkIntegrationTest, WashTrading_SCCDetectsBidirectionalTradePairs)
{
    auto trading_accounts = spark->sql(R"(
        SELECT CAST(id AS INT) AS id, owner FROM VALUES
            (1, 'OwnerX_Acct1'),
            (2, 'OwnerX_Acct2'),
            (3, 'LegitBuyerA'),
            (4, 'LegitSellerB')
        AS t(id, owner)
    )");

    // --------------------------------------------------------------------------------
    // Accounts 1 and 2 are both controlled by OwnerX - bidirectional trade.
    // Accounts 3 and 4 represent a legitimate one-way transaction.
    // --------------------------------------------------------------------------------
    auto same_second_trades = spark->sql(R"(
        SELECT CAST(src AS INT) AS src, CAST(dst AS INT) AS dst, ticker FROM VALUES
            (1, 2, 'AAPL'),
            (2, 1, 'AAPL'),
            (3, 4, 'MSFT')
        AS t(src, dst, ticker)
    )");

    auto scc_rows =
        GraphFrame(trading_accounts, same_second_trades).stronglyConnectedComponents(5).collect();

    std::map<int32_t, int64_t> scc_by_id;
    for (auto& row : scc_rows)
        scc_by_id[row.get<int32_t>("id")] = row.get<int64_t>("component");

    // --------------------------------------------------------------------------------
    // OwnerX's two accounts form a 2-node SCC (Wash-trade signature).
    // --------------------------------------------------------------------------------
    EXPECT_EQ(scc_by_id[1], scc_by_id[2]);

    // --------------------------------------------------------------------------------
    // Legitimate counterparties are NOT in the same SCC (No cycle).
    // --------------------------------------------------------------------------------
    EXPECT_NE(scc_by_id[3], scc_by_id[4]);

    // --------------------------------------------------------------------------------
    // The wash-trade SCC is distinct from the legitimate trade accounts.
    // --------------------------------------------------------------------------------
    EXPECT_NE(scc_by_id[1], scc_by_id[3]);
}

// ---------------------------------------------------------------------------------
// SCENARIO: Post-Trade Settlement - Netting Ring Optimisation
//
// A central clearing counterparty (CCP) can eliminate settlement obligations
// when it detects a netting ring: A owes B owes C owes A.  The CCP cancels
// all three legs and requires only the net residual to settle.
//
// triplets() enumerates every (src, edge, dst) triple; combined with
// filterEdges, the CCP can scan for ring candidates before running full
// SCC on the surviving graph to confirm which rings are truly circular.
//
// NOTE: triplets() columns (src, edge, dst) are Spark structs, surfaced as
// shared_ptr<Row>. col_index() is used to assert column presence without
// making assumptions about the inner field types.
// ---------------------------------------------------------------------------------
TEST_F(SparkIntegrationTest, Settlement_TripletsEnumerateAllObligations)
{
    auto participants = spark->sql(R"(
        SELECT CAST(id AS INT) AS id, name FROM VALUES
            (1, 'Fund A'),
            (2, 'Fund B'),
            (3, 'Fund C')
        AS t(id, name)
    )");

    auto obligations = spark->sql(R"(
        SELECT CAST(src AS INT) AS src, CAST(dst AS INT) AS dst, CAST(amount AS DOUBLE) AS amount FROM VALUES
            (1, 2, 5000000.0),
            (2, 3, 4800000.0),
            (3, 1, 4600000.0)
        AS t(src, dst, amount)
    )");

    auto triplets = GraphFrame(participants, obligations).triplets().collect();

    // --------------------------------------------------------------------------------
    // One triplet per obligation.
    // --------------------------------------------------------------------------------
    EXPECT_EQ(triplets.size(), 3u);

    // --------------------------------------------------------------------------------
    // Every triplet must carry src, edge, dst struct columns.
    // col_index throws if the column is absent — EXPECT_NO_THROW confirms
    // presence.
    // --------------------------------------------------------------------------------
    for (auto& row : triplets)
    {
        EXPECT_NO_THROW(row.col_index("src"));
        EXPECT_NO_THROW(row.col_index("edge"));
        EXPECT_NO_THROW(row.col_index("dst"));
    }

    // --------------------------------------------------------------------------------
    // The src and dst structs must expose a vertex id field.
    // The edge struct must expose the amount field.
    // Dereference shared_ptr<Row> to access inner fields.
    // --------------------------------------------------------------------------------
    for (auto& row : triplets)
    {
        auto src_ptr = row.get<std::shared_ptr<Row>>("src");
        auto edge_ptr = row.get<std::shared_ptr<Row>>("edge");
        auto dst_ptr = row.get<std::shared_ptr<Row>>("dst");

        ASSERT_NE(src_ptr, nullptr);
        ASSERT_NE(edge_ptr, nullptr);
        ASSERT_NE(dst_ptr, nullptr);

        EXPECT_NO_THROW(src_ptr->col_index("id"));
        EXPECT_NO_THROW(dst_ptr->col_index("id"));
        EXPECT_NO_THROW(edge_ptr->col_index("amount"));
    }
}

TEST_F(SparkIntegrationTest, Settlement_FilterEdgesRemovesSmallObligationsBelowNettingThreshold)
{
    auto participants = spark->sql(R"(
        SELECT CAST(id AS INT) AS id, name FROM VALUES
            (1, 'Fund A'), (2, 'Fund B'), (3, 'Fund C'), (4, 'Fund D')
        AS t(id, name)
    )");

    // --------------------------------------------------------------------------------
    // Obligations: two above the CCP netting threshold (1 M), two below.
    // --------------------------------------------------------------------------------
    auto obligations = spark->sql(R"(
        SELECT CAST(src AS INT) AS src, CAST(dst AS INT) AS dst, CAST(amount AS DOUBLE) AS amount FROM VALUES
            (1, 2, 5000000.0),
            (2, 3, 4800000.0),
            (3, 4,  250000.0),
            (4, 1,  180000.0)
        AS t(src, dst, amount)
    )");

    // --------------------------------------------------------------------------------
    // Only obligations at or above the 1 M threshold go to the netting
    // engine.
    // --------------------------------------------------------------------------------
    auto netting_eligible =
        GraphFrame(participants, obligations).filterEdges("amount >= 1000000").collect();

    EXPECT_EQ(netting_eligible.size(), 2u);
}