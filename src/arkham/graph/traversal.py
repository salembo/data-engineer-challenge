"""Reachability with GraphFrames: from each node, every target it reaches and in how many hops.

GraphFrames' shortest paths to a set of landmark nodes follow edge direction (from source
to destination), which here means from the dependent node to what it depends on. Each
node is reached once, at its shortest distance, so a cycle cannot make a traversal loop;
results beyond `max_hops` are dropped.
"""

from __future__ import annotations

from graphframes import GraphFrame
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from arkham.lakehouse import HASH_EXCLUDED, empty_frame

REACH_SCHEMA = StructType(
    [
        StructField("id", StringType()),
        StructField("target", StringType()),
        StructField("hops", IntegerType()),
    ]
)


def as_graph_frame(nodes: DataFrame, edges: DataFrame) -> GraphFrame:
    """graph.nodes and graph.edges as a GraphFrame, for ad hoc queries (`make pyspark`).

    GraphFrames needs `id` on vertices and `src`/`dst` on edges, so `node_id`, `src_id` and
    `dst_id` are renamed; every other content column is kept as an attribute, and the
    control columns (run, load time, row hash) are dropped.
    """
    vertices = nodes.drop(*HASH_EXCLUDED).withColumnRenamed("node_id", "id")
    links = (
        edges.drop(*HASH_EXCLUDED)
        .withColumnRenamed("src_id", "src")
        .withColumnRenamed("dst_id", "dst")
    )
    return GraphFrame(vertices, links)


def reachability(
    vertex_ids: DataFrame, edges: DataFrame, landmarks: list[str], max_hops: int
) -> DataFrame:
    """(id, target, hops) for every landmark `target` reachable from vertex `id`.

    `vertex_ids` has an `id` column; `edges` has `src` and `dst`. A node never lists itself.
    """
    if not landmarks:
        return empty_frame(vertex_ids.sparkSession, REACH_SCHEMA)
    graph = GraphFrame(vertex_ids.select("id"), edges.select("src", "dst"))
    distances = graph.shortestPaths(landmarks=sorted(landmarks))
    return (
        distances.select("id", F.explode("distances").alias("target", "hops"))
        .filter((F.col("id") != F.col("target")) & (F.col("hops") <= max_hops))
        .select(
            F.col("id").cast("string").alias("id"),
            F.col("target").cast("string").alias("target"),
            F.col("hops").cast("int").alias("hops"),
        )
    )
