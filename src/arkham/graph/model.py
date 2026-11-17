"""graph.nodes and graph.edges, from the silver dimensions.

- Nodes: every location and product whose owner is not a sandbox tenant. Tenants are not
  nodes: ownership is the node's `tenant_id`. Suppliers are not a node type either, since
  a product's supplier is always a plant.
- Edges point from the dependent node to the node it depends on: a location to its parent
  supply node (SUPPLIED_BY), a product to its supplier plant (SOURCED_FROM) and a product
  to its substitute (SUBSTITUTED_BY). The ERP holds one value of each per row, so a node
  has at most one outgoing edge of each type.
- An edge whose target is not a node is not created; it is counted as a dangling reference.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from arkham.gold.common import without_sandbox
from arkham.lakehouse import Lakehouse

NODES = "graph.nodes"
EDGES = "graph.edges"
NODE_KEYS = ["node_id"]
EDGE_KEYS = ["src_id", "dst_id", "edge_type"]
INPUTS = ("silver.dim_tenant", "silver.dim_location", "silver.dim_product")

LOCATION, PRODUCT = "LOCATION", "PRODUCT"
SUPPLIED_BY, SOURCED_FROM, SUBSTITUTED_BY = "SUPPLIED_BY", "SOURCED_FROM", "SUBSTITUTED_BY"
STORE = "RETAIL_STORE"  # the ERP's location_type of a store


def nodes(lakehouse: Lakehouse) -> DataFrame:
    locations = without_sandbox(lakehouse.read("silver.dim_location"), lakehouse).select(
        F.col("location_id").alias("node_id"),
        F.lit(LOCATION).alias("node_type"),
        F.col("location_type").alias("node_subtype"),
        "tenant_id",
        F.col("operating_status").alias("status"),
    )
    products = without_sandbox(lakehouse.read("silver.dim_product"), lakehouse).select(
        F.col("product_id").alias("node_id"),
        F.lit(PRODUCT).alias("node_type"),
        F.col("category_code").alias("node_subtype"),
        "tenant_id",
        F.when(F.col("is_discontinued"), F.lit("DISCONTINUED"))
        .otherwise(F.lit("ACTIVE"))
        .alias("status"),
    )
    return locations.unionByName(products)


def candidate_edges(lakehouse: Lakehouse) -> DataFrame:
    """Every reference between dimension rows, before checking that both ends are nodes."""
    locations = lakehouse.read("silver.dim_location")
    products = lakehouse.read("silver.dim_product")
    parts = (
        locations.select(
            F.col("location_id").alias("src_id"),
            F.col("parent_supply_node_id").alias("dst_id"),
            F.lit(SUPPLIED_BY).alias("edge_type"),
        ),
        products.select(
            F.col("product_id").alias("src_id"),
            F.col("primary_supplier_location_id").alias("dst_id"),
            F.lit(SOURCED_FROM).alias("edge_type"),
        ),
        products.select(
            F.col("product_id").alias("src_id"),
            F.col("substitute_product_id").alias("dst_id"),
            F.lit(SUBSTITUTED_BY).alias("edge_type"),
        ),
    )
    union = parts[0].unionByName(parts[1]).unionByName(parts[2])
    return union.filter(F.col("dst_id").isNotNull())


def resolve_edges(candidates: DataFrame, graph_nodes: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Edges whose two ends are nodes, and the dangling ones (target missing). A candidate
    whose source is not a node (a sandbox location or product) is dropped."""
    sources = graph_nodes.select(F.col("node_id").alias("src_id"), "tenant_id")
    targets = graph_nodes.select(
        F.col("node_id").alias("_target_id"), F.col("tenant_id").alias("dst_tenant_id")
    )
    with_ends = candidates.join(sources, "src_id").join(
        targets, F.col("dst_id") == F.col("_target_id"), "left"
    )
    edges = with_ends.filter(F.col("_target_id").isNotNull()).select(
        *EDGE_KEYS,
        "tenant_id",
        "dst_tenant_id",
        (~F.col("tenant_id").eqNullSafe(F.col("dst_tenant_id"))).alias("is_cross_tenant"),
    )
    dangling = with_ends.filter(F.col("_target_id").isNull()).select(*EDGE_KEYS, "tenant_id")
    return edges, dangling
