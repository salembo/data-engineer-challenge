"""v3 with in-memory sources: the supply-network graph and the data catalog.

The fakes' ERP (tests/fakes.py): PLANT_1 <- DC_1 <- STORE_1 and STORE_2 <- DC_9, a parent
that does not exist; PROD_1 (tenant A) has PROD_2 (tenant B, another category) as its
substitute; every product is sourced from PLANT_1; PROD_3 is discontinued.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest
from fakes import W1, W2, W3, Harness
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from arkham.bronze.context import PipelineError
from arkham.catalog.registry import load_registry
from arkham.catalog.stage import check_registry
from arkham.config import DEFAULT_CONFIG_PATH
from arkham.graph import results
from arkham.graph.model import STORE
from arkham.graph.traversal import as_graph_frame
from arkham.lakehouse import TABLES

Derived = tuple[Harness, dict]


def rows(h: Harness, table: str, *columns: str) -> set[tuple[object, ...]]:
    return {tuple(row) for row in h.table(table).select(*columns).collect()}


def test_nodes_and_edges(derived: Derived) -> None:
    h = derived[0]
    assert h.table("graph.nodes").count() == 7  # 4 locations, 3 products, no sandbox nodes
    assert rows(h, "graph.edges", "src_id", "dst_id", "edge_type", "is_cross_tenant") == {
        ("DC_1", "PLANT_1", "SUPPLIED_BY", False),
        ("STORE_1", "DC_1", "SUPPLIED_BY", True),
        ("PROD_1", "PLANT_1", "SOURCED_FROM", False),
        ("PROD_2", "PLANT_1", "SOURCED_FROM", True),
        ("PROD_3", "PLANT_1", "SOURCED_FROM", False),
        ("PROD_1", "PROD_2", "SUBSTITUTED_BY", True),
    }
    # STORE_2's parent DC_9 is not a node: no edge, counted as dangling in the run log.
    dangling = (
        h.table("platform.run_metrics")
        .filter((F.col("table_name") == "graph.edges") & (F.col("outcome") == "flagged"))
        .select("rows")
        .distinct()
        .collect()
    )
    assert [row.rows for row in dangling] == [1]


def test_upstream_dependencies_and_bottlenecks(derived: Derived) -> None:
    h = derived[0]
    assert rows(
        h, "graph.upstream_dependencies", "tenant_id", "store_id", "upstream_node_id", "hops"
    ) == {("TENANT_B", "STORE_1", "DC_1", 1), ("TENANT_B", "STORE_1", "PLANT_1", 2)}
    bottlenecks = {
        row.node_id: (
            row.dependent_stores,
            str(row.store_share),
            row.sourced_products,
            row.is_bottleneck,
        )
        for row in h.table("graph.bottlenecks").collect()
    }
    # Two stores in the network; STORE_2 depends on nothing that exists.
    assert bottlenecks == {
        "DC_1": (1, "0.5000", 0, True),
        "PLANT_1": (1, "0.5000", 3, True),
    }
    assert h.table("graph.cross_tenant_dependencies").count() == 3


def test_substitution_paths(derived: Derived) -> None:
    h = derived[0]
    path = h.table("graph.substitution_paths").collect()
    assert [
        (r.tenant_id, r.product_id, r.substitute_product_id, r.hops, r.path, r.crosses_tenant,
         r.crosses_category, r.is_recommended)
        for r in path
    ] == [("TENANT_A", "PROD_1", "PROD_2", 1, ["PROD_1", "PROD_2"], True, True, True)]  # fmt: skip


def test_a_cycle_ends_and_paths_follow_the_chain(spark: SparkSession) -> None:
    nodes = spark.createDataFrame(
        [(p, "PRODUCT", "C1" if p != "D" else "C2", "T1" if p != "C" else "T2", "ACTIVE")
         for p in ("A", "B", "C", "D")],
        "node_id STRING, node_type STRING, node_subtype STRING, tenant_id STRING, status STRING",
    )  # fmt: skip
    edges = spark.createDataFrame(
        [("A", "B", "SUBSTITUTED_BY"), ("B", "C", "SUBSTITUTED_BY"), ("C", "A", "SUBSTITUTED_BY"),
         ("D", "D", "SUBSTITUTED_BY")],
        "src_id STRING, dst_id STRING, edge_type STRING",
    )  # fmt: skip
    found = {
        (r.product_id, r.substitute_product_id): (r.hops, r.path, r.crosses_tenant)
        for r in results.substitution_paths(nodes, edges, max_hops=25).collect()
    }
    assert found == {
        ("A", "B"): (1, ["A", "B"], False),
        ("A", "C"): (2, ["A", "B", "C"], True),
        ("B", "C"): (1, ["B", "C"], True),
        ("B", "A"): (2, ["B", "C", "A"], True),
        ("C", "A"): (1, ["C", "A"], True),
        ("C", "B"): (2, ["C", "A", "B"], True),
    }
    limited = results.substitution_paths(nodes, edges, max_hops=1)
    assert limited.filter("hops > 1").count() == 0


def test_the_graph_frame_answers_ad_hoc_queries(derived: Derived) -> None:
    """The GraphFrame `make pyspark` offers as `g`: content columns only, and a
    variable-length pattern gives the same store -> upstream pairs as the result table."""
    h = derived[0]
    g = as_graph_frame(h.table("graph.nodes"), h.table("graph.edges"))
    assert g.vertices.columns == ["id", "node_type", "node_subtype", "tenant_id", "status"]
    assert g.edges.columns[:3] == ["src", "dst", "edge_type"]
    assert not {"_row_hash", "_run_id", "_loaded_at"} & set(g.edges.columns)
    found = {
        (row.store, row.upstream)
        for row in g.filterEdges("edge_type = 'SUPPLIED_BY'")
        .find("(s)-[e*1..3]->(u)")
        .where(F.col("s.node_subtype") == STORE)
        .select(F.col("s.id").alias("store"), F.col("u.id").alias("upstream"))
        .collect()
    }
    assert found == rows(h, "graph.upstream_dependencies", "store_id", "upstream_node_id")


def test_graph_results_are_isolated_by_the_starting_node(derived: Derived) -> None:
    h = derived[0]
    owners = h.table("graph.nodes").select(
        F.col("node_id").alias("_start"), F.col("tenant_id").alias("_owner")
    )
    for table, start in (
        ("graph.upstream_dependencies", "store_id"),
        ("graph.bottlenecks", "node_id"),
        ("graph.cross_tenant_dependencies", "src_id"),
        ("graph.substitution_paths", "product_id"),
    ):
        joined = h.table(table).join(owners, F.col(start) == F.col("_start"))
        assert joined.filter(F.col("tenant_id") != F.col("_owner")).count() == 0, table


def test_catalog_lists_every_table_with_its_real_schema(derived: Derived) -> None:
    h = derived[0]
    lake = h.pipeline.lakehouse
    existing = {name for name in TABLES if lake.exists(name)}
    datasets = h.table("catalog.datasets")
    assert {row.table_name for row in datasets.collect()} == existing
    assert datasets.filter("scope = 'TENANT' AND tenant_column IS NULL").count() == 0
    personal = datasets.filter("contains_personal_data").collect()
    assert [(r.table_name, r.personal_data_columns) for r in personal] == [
        ("bronze.open_prices", ["raw_json"])
    ]
    columns = h.table("catalog.columns")
    for table in ("silver.fact_pos_sale", "gold.pipeline_health", "catalog.columns"):
        listed = [
            (r.column_name, r.data_type, r.nullable)
            for r in columns.filter(F.col("table_name") == table).orderBy("ordinal").collect()
        ]
        actual = [(f.name, f.dataType.simpleString(), f.nullable) for f in lake.read(table).schema]
        assert listed == actual, table
    assert columns.filter("description IS NULL").count() == 0
    # No value of an Open Prices owner field reaches the catalog.
    assert columns.filter(F.col("description").contains("someone")).count() == 0


def test_catalog_lineage_and_freshness(derived: Derived) -> None:
    h = derived[0]
    lineage = h.table("catalog.datasets").filter(F.col("table_name") == "gold.pos_sales_daily")
    assert lineage.first().source_objects == [
        "erp.locations",
        "erp.products",
        "erp.tenants",
        "open_food_facts.dump",
        "open_food_facts.product_api",
        "open_prices.prices",
        "partner_api.events",
    ]
    freshness = h.table("catalog.freshness")
    tables = h.table("catalog.datasets").count()
    assert freshness.count() == tables * 3
    sales = {
        r.window_id: (r.row_count, r.rows_scope, r.run_status)
        for r in freshness.filter(F.col("table_name") == "silver.fact_pos_sale").collect()
    }
    assert {w: v[1:] for w, v in sales.items()} == {w: ("window", "SUCCESS") for w in (W1, W2, W3)}
    prices = freshness.filter(
        (F.col("table_name") == "silver.ref_public_price") & (F.col("window_id") == W3)
    ).first()
    assert prices.source_snapshot_date is not None and prices.refreshed_in == W3
    # The run columns hold the final values from the run log once each run finished.
    finished = {
        r.finished_at for r in h.table("platform.run_log").filter("command = 'run'").collect()
    }
    for row in freshness.filter(F.col("table_name") == "gold.reconciliation").collect():
        assert row.run_finished_at in finished


def test_registry_matches_the_code() -> None:
    registry = load_registry(DEFAULT_CONFIG_PATH.parent / "catalog.toml")
    assert set(registry.tables) == set(TABLES)
    assert {n: e.scope for n, e in registry.tables.items()} == {
        n: s.scope for n, s in TABLES.items()
    }
    for name in TABLES:
        if name.split(".")[0] in ("gold", "graph"):
            assert registry.source_objects_of(name), name


def test_an_unregistered_delta_table_fails_the_check(derived: Derived) -> None:
    h = derived[0]
    registry = load_registry(DEFAULT_CONFIG_PATH.parent / "catalog.toml")
    ctx = SimpleNamespace(lakehouse=h.pipeline.lakehouse)
    with pytest.raises(PipelineError, match="no registry entry"):
        check_registry(ctx, registry, {"silver.rogue_table"})  # type: ignore[arg-type]
