"""Interactive PySpark session with every lakehouse table registered by name, and the
supply network and product graph ready for GraphFrames as `g`.

Usage (inside the pipeline container): python -i -m arkham.shell
"""

from __future__ import annotations

import os
import tempfile
from pathlib import Path

from arkham.graph.model import EDGES, NODES
from arkham.graph.traversal import as_graph_frame
from arkham.lakehouse import Lakehouse
from arkham.spark import build_spark_session

spark = build_spark_session("arkham-shell")
lakehouse = Lakehouse(
    spark, Path(os.environ.get("ARKHAM_LAKEHOUSE_ROOT", Path.cwd() / "lakehouse"))
)
tables = lakehouse.register()

# GraphFrames' iterative algorithms (connectedComponents, detectingCycles, k_core) need a
# checkpoint directory: a temporary one, outside the lakehouse, gone with the container.
spark.sparkContext.setCheckpointDir(tempfile.mkdtemp(prefix="graphframes-checkpoints-"))
g = (
    as_graph_frame(lakehouse.read(NODES), lakehouse.read(EDGES))
    if NODES in tables and EDGES in tables
    else None
)

print("Spark session: `spark`. Registered tables:")
for name in tables:
    print(f"  {name}")
print(
    'Example: spark.sql("SELECT _window_id, COUNT(*) FROM bronze.partner_events GROUP BY 1").show()'
)
if g is None:
    print("No graph yet: run the pipeline first (make run-pending).")
else:
    print(
        "Graph: `g`, a GraphFrame of graph.nodes (vertex `id` = node_id) and graph.edges "
        "(`src` -> `dst`, from the dependent node to the node it depends on).\n"
        'Example: g.find("(s)-[e]->(dc)").where("e.edge_type = \'SUPPLIED_BY\'").show()\n'
        "More graph queries: docs/queries.md"
    )
