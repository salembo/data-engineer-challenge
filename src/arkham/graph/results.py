"""The four result tables that answer the README's traversal questions.

Each result row carries the owner of its starting node as `tenant_id`, which defines
isolation. A path is shown in full to that tenant; for other tenants' nodes it holds only
IDs, and the other columns are owner tenant, type and category, never cost or price
(the v2-v4 PRD, cross-tenant exposure).
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from arkham.graph.model import LOCATION, PRODUCT, SOURCED_FROM, STORE, SUBSTITUTED_BY, SUPPLIED_BY
from arkham.graph.traversal import reachability

UPSTREAM = "graph.upstream_dependencies"
BOTTLENECKS = "graph.bottlenecks"
CROSS_TENANT = "graph.cross_tenant_dependencies"
SUBSTITUTION = "graph.substitution_paths"
UPSTREAM_KEYS = ["store_id", "upstream_node_id"]
BOTTLENECK_KEYS = ["node_id"]
CROSS_TENANT_KEYS = ["src_id", "dst_id", "edge_type"]
SUBSTITUTION_KEYS = ["product_id", "substitute_product_id"]


def _ids(df: DataFrame) -> list[str]:
    """Node IDs of a small node set (the graph has a few hundred nodes)."""
    return [row.node_id for row in df.select("node_id").collect()]


def upstream_dependencies(nodes: DataFrame, edges: DataFrame, max_hops: int) -> DataFrame:
    """Which supply nodes each store depends on, directly or through other nodes."""
    locations = nodes.filter(F.col("node_type") == LOCATION)
    supply_nodes = locations.filter(F.col("node_subtype") != STORE)
    reach = reachability(
        locations.select(F.col("node_id").alias("id")),
        edges.filter(F.col("edge_type") == SUPPLIED_BY).select(
            F.col("src_id").alias("src"), F.col("dst_id").alias("dst")
        ),
        _ids(supply_nodes),
        max_hops,
    )
    stores = locations.filter(F.col("node_subtype") == STORE).select(
        F.col("node_id").alias("id"), "tenant_id"
    )
    upstream = supply_nodes.select(
        F.col("node_id").alias("target"),
        F.col("node_subtype").alias("upstream_node_subtype"),
        F.col("tenant_id").alias("upstream_tenant_id"),
    )
    return (
        reach.join(stores, "id")
        .join(upstream, "target")
        .select(
            "tenant_id",
            F.col("id").alias("store_id"),
            F.col("target").alias("upstream_node_id"),
            "upstream_node_subtype",
            "upstream_tenant_id",
            "hops",
            (F.col("tenant_id") != F.col("upstream_tenant_id")).alias("is_cross_tenant"),
        )
    )


def bottlenecks(
    nodes: DataFrame, edges: DataFrame, dependencies: DataFrame, threshold: float
) -> DataFrame:
    """Supply nodes ranked by how many stores depend on them. Every store has one parent,
    so every supply node is a single point of failure for its stores; a bottleneck is a
    node whose share of all stores reaches the threshold."""
    locations = nodes.filter(F.col("node_type") == LOCATION)
    total_stores = locations.filter(F.col("node_subtype") == STORE).count()
    dependents = dependencies.groupBy("upstream_node_id").agg(
        F.countDistinct("store_id").alias("dependent_stores"),
        F.countDistinct(F.when(F.col("hops") == 1, F.col("store_id"))).alias("direct_stores"),
        F.countDistinct("tenant_id").alias("dependent_tenants"),
    )
    sourced = (
        edges.filter(F.col("edge_type") == SOURCED_FROM)
        .groupBy(F.col("dst_id").alias("upstream_node_id"))
        .agg(F.count(F.lit(1)).alias("sourced_products"))
    )
    zero = F.lit(0).cast("bigint")
    share = F.round(
        F.col("dependent_stores").cast("decimal(18,4)") / F.lit(max(total_stores, 1)), 4
    ).cast("decimal(5,4)")
    return (
        locations.filter(F.col("node_subtype") != STORE)
        .select(
            F.col("node_id").alias("upstream_node_id"),
            "node_subtype",
            "tenant_id",
        )
        .join(dependents, "upstream_node_id", "left")
        .join(sourced, "upstream_node_id", "left")
        .select(
            F.col("upstream_node_id").alias("node_id"),
            "node_subtype",
            "tenant_id",
            F.coalesce(F.col("dependent_stores"), zero).alias("dependent_stores"),
            F.coalesce(F.col("direct_stores"), zero).alias("direct_stores"),
            F.lit(total_stores).cast("bigint").alias("total_stores"),
            F.coalesce(F.col("dependent_tenants"), zero).alias("dependent_tenants"),
            F.coalesce(F.col("sourced_products"), zero).alias("sourced_products"),
        )
        .withColumn("store_share", share)
        .withColumn("is_bottleneck", F.col("store_share") >= F.lit(threshold).cast("decimal(5,4)"))
    )


def cross_tenant_dependencies(edges: DataFrame) -> DataFrame:
    """Every edge where a tenant's node depends on another tenant's node."""
    return edges.filter(F.col("is_cross_tenant")).select(
        "tenant_id", "src_id", "dst_id", "edge_type", "dst_tenant_id"
    )


def substitution_paths(nodes: DataFrame, edges: DataFrame, max_hops: int) -> DataFrame:
    """Every substitute reachable from each product, with the ordered path to it.

    A product has at most one substitute (one ERP column), so the products reachable from
    it form a single chain, and the path to the one at `hops` is the chain up to it. The
    recommended substitute is the nearest one that is not discontinued.
    """
    products = nodes.filter(F.col("node_type") == PRODUCT)
    reach = reachability(
        products.select(F.col("node_id").alias("id")),
        edges.filter(F.col("edge_type") == SUBSTITUTED_BY).select(
            F.col("src_id").alias("src"), F.col("dst_id").alias("dst")
        ),
        _ids(products),
        max_hops,
    )
    chains = reach.groupBy("id").agg(
        F.array_sort(F.collect_list(F.struct("hops", "target"))).alias("_chain")
    )
    paths = reach.join(chains, "id").withColumn(
        "path",
        F.concat(
            F.array(F.col("id")),
            F.transform(
                F.filter("_chain", lambda step: step["hops"] <= F.col("hops")),
                lambda step: step["target"],
            ),
        ),
    )
    attributes = products.select(
        F.col("node_id").alias("_node"),
        F.col("tenant_id").alias("_tenant"),
        F.col("node_subtype").alias("_category"),
        (F.col("status") == "DISCONTINUED").alias("_discontinued"),
    )
    start = attributes.select(
        F.col("_node").alias("id"),
        F.col("_tenant").alias("tenant_id"),
        F.col("_category").alias("category_code"),
    )
    target = attributes.select(
        F.col("_node").alias("target"),
        F.col("_tenant").alias("substitute_tenant_id"),
        F.col("_category").alias("substitute_category_code"),
        F.col("_discontinued").alias("substitute_is_discontinued"),
    )
    steps = (
        paths.select("id", "target", F.explode("path").alias("_node"))
        .join(attributes, "_node")
        .join(start, "id")
        .groupBy("id", "target")
        .agg(
            F.max(F.col("_tenant") != F.col("tenant_id")).alias("crosses_tenant"),
            F.max(F.col("_category") != F.col("category_code")).alias("crosses_category"),
        )
    )
    result = paths.join(start, "id").join(target, "target").join(steps, ["id", "target"])
    nearest = (
        result.filter(~F.col("substitute_is_discontinued"))
        .groupBy("id")
        .agg(F.min("hops").alias("_nearest"))
    )
    return result.join(nearest, "id", "left").select(
        "tenant_id",
        F.col("id").alias("product_id"),
        "category_code",
        F.col("target").alias("substitute_product_id"),
        "hops",
        "path",
        "substitute_tenant_id",
        "substitute_category_code",
        "substitute_is_discontinued",
        "crosses_tenant",
        "crosses_category",
        F.coalesce(
            (F.col("hops") == F.col("_nearest")) & ~F.col("substitute_is_discontinued"),
            F.lit(False),
        ).alias("is_recommended"),
    )
