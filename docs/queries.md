# Example queries

Every table is registered as `<layer>.<table>` in `make pyspark`, so these queries run as `spark.sql("""...""").show(truncate=False)`. Tenant-scoped tables are filtered by the column that `catalog.datasets.tenant_column` names (always `tenant_id`).

## Data catalog

### Which tables exist?

```sql
SELECT table_name, layer, scope, tenant_column, load_pattern, description
FROM catalog.datasets
ORDER BY layer, table_name
```

### What is a table's schema?

```sql
SELECT ordinal, column_name, data_type, nullable, is_key, is_tenant_column, is_personal_data, description
FROM catalog.columns
WHERE table_name = 'gold.pos_sales_daily'
ORDER BY ordinal
```

### Where does a table come from?

Direct upstream tables and source objects:

```sql
SELECT upstream, upstream_type, upstream_system, transformation
FROM catalog.lineage
WHERE downstream = 'gold.pos_sales_daily'
```

The source objects it ultimately comes from:

```sql
SELECT table_name, source_objects
FROM catalog.datasets
WHERE table_name = 'gold.pos_sales_daily'
```

The full upstream chain, one level per step (here three levels):

```sql
WITH l1 AS (SELECT upstream AS t1 FROM catalog.lineage WHERE downstream = 'gold.pos_sales_daily'),
     l2 AS (SELECT l1.t1, l.upstream AS t2 FROM l1 JOIN catalog.lineage l ON l.downstream = l1.t1),
     l3 AS (SELECT l2.*, l.upstream AS t3 FROM l2 LEFT JOIN catalog.lineage l ON l.downstream = l2.t2)
SELECT * FROM l3 ORDER BY t1, t2, t3
```

### How fresh is a table?

```sql
SELECT window_id, row_count, rows_scope, run_status, run_finished_at, fallback_used,
       source_snapshot_date, refreshed_in
FROM catalog.freshness
WHERE table_name = 'silver.ref_public_price'
ORDER BY window_id
```

Tables that used a public-source fallback in some window:

```sql
SELECT DISTINCT table_name, window_id FROM catalog.freshness WHERE fallback_used
```

## Supply network and product graph

### Upstream supply bottlenecks

```sql
SELECT node_id, node_subtype, tenant_id, dependent_stores, direct_stores, store_share,
       dependent_tenants, sourced_products, is_bottleneck
FROM graph.bottlenecks
ORDER BY dependent_stores DESC, node_id
```

Stores that depend on a node, and at how many hops:

```sql
SELECT store_id, tenant_id, hops
FROM graph.upstream_dependencies
WHERE upstream_node_id = 'DC_CENTRAL_HUB_01'
ORDER BY hops, store_id
```

### Cross-tenant logistics dependencies

Direct dependencies on another tenant's node:

```sql
SELECT tenant_id, edge_type, dst_tenant_id, COUNT(*) AS edges
FROM graph.cross_tenant_dependencies
GROUP BY tenant_id, edge_type, dst_tenant_id
ORDER BY tenant_id, edge_type
```

Stores that depend on another tenant's plant or distribution center, directly or through their own:

```sql
SELECT tenant_id, upstream_tenant_id, COUNT(DISTINCT store_id) AS stores
FROM graph.upstream_dependencies
WHERE is_cross_tenant
GROUP BY tenant_id, upstream_tenant_id
```

### Multi-hop product substitution

What can replace a product, nearest first, and the recommended substitute:

```sql
SELECT substitute_product_id, hops, path, substitute_tenant_id, substitute_category_code,
       substitute_is_discontinued, crosses_tenant, crosses_category, is_recommended
FROM graph.substitution_paths
WHERE product_id = 'PROD_0042'
ORDER BY hops
```

The same question for one tenant only, the way a tenant would see it:

```sql
SELECT product_id, substitute_product_id, hops, path
FROM graph.substitution_paths
WHERE tenant_id = 'TENANT_RTL_003' AND is_recommended
```

### Ad hoc queries with GraphFrames

`make pyspark` builds `g`, a GraphFrame of `graph.nodes` and `graph.edges`. Vertices have `id` (the `node_id`) and the node columns; edges have `src`, `dst` and the edge columns, and point from the dependent node to the node it depends on. Every call returns a DataFrame. The results below are from the real data after W1, W2 and W3.

```python
from pyspark import StorageLevel
from pyspark.sql import functions as F
```

Stores behind a distribution center, at 1 to 3 hops (72: 48 direct, 24 through `DC_WEST_HUB_04`). A variable-length pattern suits a few hops:

```python
(
    g.filterEdges("edge_type = 'SUPPLIED_BY'")
    .find("(s)-[e*1..3]->(dc)")
    .where("s.node_subtype = 'RETAIL_STORE' AND dc.id = 'DC_CENTRAL_HUB_01'")
    .groupBy("_hop").agg(F.countDistinct("s.id").alias("stores"))
    .show()
)
```

Every route from a store to a plant. For long paths, use `bfs` (shortest paths) or `all_paths` (every simple path): a variable-length pattern becomes one chain of joins per length, which is costly beyond a few hops.

```python
g.all_paths(
    "id = 'STORE_0003'",
    "node_subtype = 'SUPPLIER_PLANT'",
    edge_filter="edge_type = 'SUPPLIED_BY'",
    max_path_length=5,
).show(truncate=False)
# [STORE_0003, DC_WEST_HUB_04, DC_CENTRAL_HUB_01, PLANT_NA_01], len 3
```

Graph plus business data: the net revenue of the stores behind a node, by currency. The sales are those recorded at those stores by any tenant; filter `pos_sales_daily.tenant_id` for one tenant's view.

```python
stores = (
    g.filterEdges("edge_type = 'SUPPLIED_BY'")
    .find("(s)-[e*1..3]->(dc)")
    .where("s.node_subtype = 'RETAIL_STORE' AND dc.id = 'DC_CENTRAL_HUB_01'")
    .select(F.col("s.id").alias("location_id"))
    .distinct()
)
(
    stores.join(spark.table("gold.pos_sales_daily"), "location_id")
    .groupBy("currency")
    .agg((F.sum("net_revenue_cents") / 100).alias("net_revenue"))
    .show()
)
# EUR 351,760.72, MXN 156,550.17, USD 600,444.58
```

Graph algorithms:

```python
g.pageRank(resetProbability=0.15, maxIter=10).vertices.orderBy(F.desc("pagerank")).show(5)
g.connectedComponents().groupBy("component").count().show()   # one component
g.detectingCycles().show()                                     # no cycles
g.triangleCount(StorageLevel.MEMORY_AND_DISK).show()           # see the note below
```

- **Checkpoints:** `connectedComponents`, `detectingCycles` and `k_core` need a checkpoint directory, which the shell sets.
- **`triangleCount`:** in GraphFrames 0.12.3 the Python method requires its storage level, so call it with `StorageLevel.MEMORY_AND_DISK`.
- **Memory:** a long session grows to about 1.5 GB of the container's 2 GB; avoid caching large DataFrames.

## Gold

Net inventory by store and category for one tenant:

```sql
SELECT location_id, category_code, SUM(net_position_units) AS net_units, operating_status
FROM gold.inventory_position
WHERE tenant_id = 'TENANT_CPG_002'
GROUP BY location_id, category_code, operating_status
ORDER BY location_id, category_code
```

Revenue per store and currency, with public price coverage:

```sql
SELECT tenant_id, location_id, currency, SUM(sale_lines) AS lines,
       SUM(net_revenue_cents) / 100 AS net_revenue,
       SUM(CASE WHEN price_match_level <> 'NONE' THEN sale_lines ELSE 0 END) AS lines_with_public_price
FROM gold.pos_sales_daily
GROUP BY tenant_id, location_id, currency
ORDER BY tenant_id, location_id
```

Reconciliation of one window:

```sql
SELECT source_object, tenant_id, source_rows, bronze_rows, silver_rows, quarantined, filtered,
       superseded, unexplained_rows
FROM gold.reconciliation
WHERE window_id = '2026-10-W2'
ORDER BY source_object, tenant_id
```
