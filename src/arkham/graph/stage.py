"""The graph stage of a run: base tables, then the traversal results, in a fixed order.

Everything is recomputed from the silver dimensions and synchronised with the result, so
the same dimensions always give the same graph and a run with no changes writes nothing.
"""

from __future__ import annotations

from arkham.bronze.context import RunContext
from arkham.graph import model, results
from arkham.lakehouse import TABLES
from arkham.publish import publish, stable
from arkham.runlog import FILTERED, FLAGGED

GRAPH_TABLES = tuple(name for name, spec in TABLES.items() if spec.layer == "graph")


def run(ctx: RunContext) -> None:
    lake = ctx.lakehouse
    if not all(lake.exists(table) for table in model.INPUTS):
        return
    settings = ctx.config.graph
    # GraphFrames' iterative algorithms may checkpoint; keep it inside the run's scratch.
    ctx.spark.sparkContext.setCheckpointDir(lake.scratch_path(ctx.run_id, "graph_checkpoints"))

    nodes = stable(ctx, model.nodes(lake), "graph_nodes")
    candidates = stable(ctx, model.candidate_edges(lake), "graph_candidate_edges")
    edges, dangling = model.resolve_edges(candidates, nodes)
    edges = stable(ctx, edges, "graph_edges")
    publish(ctx, model.NODES, nodes, model.NODE_KEYS)
    publish(ctx, model.EDGES, edges, model.EDGE_KEYS)
    dangling_count = dangling.count()
    if dangling_count:
        ctx.record.count(model.EDGES, FLAGGED, dangling_count, "dangling: target is not a node")
    dropped = candidates.count() - edges.count() - dangling_count
    if dropped:
        ctx.record.count(model.EDGES, FILTERED, dropped, "source is not a node (sandbox)")

    dependencies = stable(
        ctx, results.upstream_dependencies(nodes, edges, settings.max_hops), "graph_upstream"
    )
    publish(ctx, results.UPSTREAM, dependencies, results.UPSTREAM_KEYS)
    publish(
        ctx,
        results.BOTTLENECKS,
        results.bottlenecks(nodes, edges, dependencies, settings.bottleneck_store_share),
        results.BOTTLENECK_KEYS,
    )
    publish(
        ctx,
        results.CROSS_TENANT,
        results.cross_tenant_dependencies(edges),
        results.CROSS_TENANT_KEYS,
    )
    publish(
        ctx,
        results.SUBSTITUTION,
        results.substitution_paths(nodes, edges, settings.max_hops),
        results.SUBSTITUTION_KEYS,
    )
