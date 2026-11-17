"""Invariants: properties every load must satisfy, whatever the source data and the window
calendar (`make verify`).

No expected value here depends on the provided dataset. Each check compares the lakehouse
with itself: silver with bronze, gold with silver, health with reconciliation, the catalog
with the Delta tables, and the graph with its own rules. They hold after any window, with
new windows or new source data, so a failure is a defect and never a change in the data.

The exact numbers of the provided dataset are the baseline checks in acceptance.py
(`make verify-v1`, `verify-v2`, `verify-v3`). Re-run identity is `make verify-rerun`.
"""

from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from arkham.pipeline import Pipeline
from arkham.silver import partner


@dataclass
class Check:
    label: str
    measured: object
    expected: object
    note: str = ""

    @property
    def passed(self) -> bool:
        return self.measured == self.expected


BUSINESS_TABLES = (
    "gold.inventory_position",
    "gold.inventory_daily",
    "gold.pos_sales_daily",
    "gold.pos_promo_performance",
)
GRAPH_RESULTS = {
    # result table -> column holding the starting node, whose owner must be `tenant_id`
    "graph.upstream_dependencies": "store_id",
    "graph.bottlenecks": "node_id",
    "graph.cross_tenant_dependencies": "src_id",
    "graph.substitution_paths": "product_id",
}
CATALOG = ("catalog.datasets", "catalog.columns", "catalog.lineage", "catalog.freshness")


def totals(df: DataFrame, *columns: str) -> tuple[int, ...]:
    row = df.agg(*(F.sum(c).alias(c) for c in columns)).first()
    return tuple(int(row[c] or 0) for c in columns)


def real_deliveries(pipeline: Pipeline) -> DataFrame:
    """Every event delivered by the partner API, every revision, without the sandbox."""
    lake = pipeline.lakehouse
    sandbox = lake.read("silver.dim_tenant").filter("is_sandbox").select("tenant_id")
    events = lake.read("bronze.partner_events").select(
        F.from_json("raw_json", partner.EVENT_SCHEMA).alias("e")
    )
    return events.select("e.*").join(sandbox, "tenant_id", "left_anti")


def sandbox_tenants(pipeline: Pipeline) -> list[str]:
    dim = pipeline.lakehouse.read("silver.dim_tenant").filter("is_sandbox")
    return [row.tenant_id for row in dim.select("tenant_id").collect()]


def string_values(df: DataFrame) -> DataFrame:
    """Every string value of a table, including array elements, as one column `value`."""
    parts = []
    for field in df.schema.fields:
        type_name = field.dataType.simpleString()
        if type_name == "string":
            parts.append(df.select(F.col(field.name).alias("value")))
        elif type_name == "array<string>":
            parts.append(df.select(F.explode_outer(field.name).alias("value")))
    union = parts[0]
    for part in parts[1:]:
        union = union.unionByName(part)
    return union.filter(F.col("value").isNotNull())


def lakehouse_tables(pipeline: Pipeline) -> set[str]:
    """Every Delta table under the lakehouse root, as `layer.table`."""
    root = pipeline.lakehouse.root
    return {
        f"{layer.name}.{table.name}"
        for layer in root.iterdir()
        if layer.is_dir() and not layer.name.startswith("_")
        for table in layer.iterdir()
        if (table / "_delta_log").is_dir()
    }


def mismatches(left: DataFrame, right: DataFrame, keys: list[str], *values: str) -> int:
    """Keys where the two frames disagree on any value, a missing key counting as one."""
    joined = left.alias("l").join(right.alias("r"), keys, "full_outer")
    differ = " OR ".join(f"NOT (l.{v} <=> r.{v})" for v in values)
    return joined.filter(differ).count()


# ---- Runs ----


def run_checks(pipeline: Pipeline) -> list[Check]:
    ids = [w.id for w in pipeline.config.calendar.windows]
    complete = [w for w in ids if pipeline.runlog.is_complete(w)]
    return [
        Check(
            "complete windows are the first ones of the calendar, in order",
            complete,
            ids[: len(complete)],
        ),
    ]


# ---- Silver ----


def silver_checks(pipeline: Pipeline) -> list[Check]:
    read = pipeline.lakehouse.read
    sales, audits = read("silver.fact_pos_sale"), read("silver.fact_shelf_audit")
    quarantine = read("platform.quarantine").filter(F.col("source_table") == partner.SOURCE)
    held = (
        sales.select("event_id", "revision", "sequence_cursor")
        .unionByName(audits.select("event_id", "revision", "sequence_cursor"))
        .unionByName(
            quarantine.select(F.col("record_key").alias("event_id"), "revision", "sequence_cursor")
        )
    )
    newest = (
        real_deliveries(pipeline).groupBy("event_id").agg(F.max(partner.version()).alias("newest"))
    )
    versions = held.select("event_id", partner.version().alias("held"))
    reverted = newest.join(versions, "event_id").filter(F.col("held") != F.col("newest")).count()
    places = held.groupBy("event_id").count()
    sandbox = sandbox_tenants(pipeline)
    leaked = sum(
        read(table).filter(F.col("tenant_id").isin(sandbox)).count()
        for table in (
            "silver.fact_inventory_movement",
            "silver.fact_pos_sale",
            "silver.fact_shelf_audit",
        )
    )
    return [
        Check("events not on their newest revision", reverted, 0),
        Check(
            "delivered events held nowhere (sales, audits or quarantine)",
            newest.join(places, "event_id", "left_anti").count(),
            0,
        ),
        Check("events held in more than one place", places.filter("count > 1").count(), 0),
        Check("sales with a null discount_rate", sales.filter("discount_rate IS NULL").count(), 0),
        Check("sandbox rows in silver facts", leaked, 0),
    ]


# ---- Gold ----


def gold_checks(pipeline: Pipeline) -> list[Check]:
    read = pipeline.lakehouse.read
    position, daily = read("gold.inventory_position"), read("gold.inventory_daily")
    posted = read("silver.fact_inventory_movement").filter("is_posted")
    counted = position.groupBy("tenant_id").agg(F.sum("movement_count").alias("movements"))
    expected_counts = posted.groupBy("tenant_id").agg(F.count(F.lit(1)).alias("movements"))
    by_category = ["tenant_id", "location_id", "category_code"]

    sales_daily, promo = read("gold.pos_sales_daily"), read("gold.pos_promo_performance")
    keys = ["tenant_id", "location_id", "product_id", "currency", "sale_date"]
    silver_lines = (
        read("silver.fact_pos_sale")
        .filter(F.col("record_state") == "ACTIVE")
        .groupBy(
            "tenant_id",
            "location_id",
            "product_id",
            "currency",
            F.to_date("event_occurred_at").alias("sale_date"),
        )
        .agg(F.count(F.lit(1)).alias("sale_lines"))
    )
    store_currency = ["tenant_id", "location_id", "currency"]
    measures = ("sale_lines", "units_sold", "net_revenue_cents", "discount_cents")

    def by_store(df: DataFrame) -> DataFrame:
        return df.groupBy(*store_currency).agg(*(F.sum(m).alias(m) for m in measures))

    grain = ["tenant_id", "location_id", "currency", "promo_campaign_id", "promo_mechanic_type"]
    sandbox = sandbox_tenants(pipeline)
    leaked = sum(read(t).filter(F.col("tenant_id").isin(sandbox)).count() for t in BUSINESS_TABLES)

    reconciliation, health = read("gold.reconciliation"), read("gold.pipeline_health")
    outcomes = ("written", "quarantined", "filtered", "superseded", "flagged")
    health_sums = health.groupBy("window_id", "tenant_id", "source_object").agg(
        *(F.sum(c).alias(c) for c in outcomes)
    )
    reconciled = reconciliation.select(
        "window_id",
        "tenant_id",
        "source_object",
        F.col("silver_rows").alias("written"),
        *outcomes[1:],
    )
    uncovered = health.filter(
        (F.col("table_name") == "silver.fact_pos_sale")
        & ((F.col("sales_with_public_price") + F.col("sales_without_public_price")) > 0)
        & F.col("public_price_coverage").isNull()
    ).count()
    return [
        Check(
            "tenants whose movements counted differ from posted movements in silver",
            mismatches(counted, expected_counts, ["tenant_id"], "movements"),
            0,
        ),
        Check(
            "stores and categories whose daily net units differ from their position",
            mismatches(
                daily.groupBy(*by_category).agg(F.sum("net_units").alias("net")),
                position.groupBy(*by_category).agg(F.sum("net_position_units").alias("net")),
                by_category,
                "net",
            ),
            0,
        ),
        Check(
            "inventory_daily rows without a period or velocity",
            daily.filter("period_id IS NULL OR velocity_units_per_day IS NULL").count(),
            0,
        ),
        Check(
            "pos_sales_daily rows that differ from silver's active lines",
            mismatches(sales_daily, silver_lines, keys, "sale_lines"),
            0,
        ),
        Check(
            "sales rows with an unknown currency or no price_match_level",
            sales_daily.filter(
                "currency IS NULL OR currency = 'UNKNOWN' OR price_match_level IS NULL"
            ).count(),
            0,
        ),
        Check(
            "pos_promo_performance rows with a null in the grain",
            promo.filter(" OR ".join(f"{c} IS NULL" for c in grain)).count(),
            0,
        ),
        Check(
            "stores and currencies whose promotion totals differ from their sales",
            mismatches(by_store(promo), by_store(sales_daily), store_currency, *measures),
            0,
        ),
        Check("sandbox rows in the business gold tables", leaked, 0),
        Check(
            "reconciliation rows with unexplained_rows <> 0",
            reconciliation.filter("unexplained_rows <> 0").count(),
            0,
        ),
        Check(
            "pipeline_health counts that differ from reconciliation",
            mismatches(
                health_sums,
                reconciled,
                ["window_id", "tenant_id", "source_object"],
                *outcomes,
            ),
            0,
        ),
        Check("tenant-windows with sales but no price coverage", uncovered, 0),
    ]


# ---- Graph ----


def graph_checks(pipeline: Pipeline) -> list[Check]:
    read = pipeline.lakehouse.read
    settings = pipeline.config.graph
    nodes, edges = read("graph.nodes"), read("graph.edges")
    node_ids = nodes.select(F.col("node_id").alias("id"))
    owners = nodes.select(F.col("node_id").alias("_start"), F.col("tenant_id").alias("_owner"))
    foreign = sum(
        read(table)
        .join(owners, F.col(start) == F.col("_start"))
        .filter(F.col("tenant_id") != F.col("_owner"))
        .count()
        for table, start in GRAPH_RESULTS.items()
    )
    dangling = (
        edges.join(node_ids, edges.src_id == node_ids.id, "left_anti").count()
        + edges.join(node_ids, edges.dst_id == node_ids.id, "left_anti").count()
    )
    dst_owner = nodes.select(F.col("node_id").alias("dst_id"), F.col("tenant_id").alias("owner"))
    wrong_flag = (
        edges.join(dst_owner, "dst_id", "left")
        .filter(
            ~F.col("dst_tenant_id").eqNullSafe(F.col("owner"))
            | (F.col("is_cross_tenant") != (F.col("tenant_id") != F.col("dst_tenant_id")))
        )
        .count()
    )
    cross_keys = ["src_id", "dst_id", "edge_type"]
    cross_edges = edges.filter("is_cross_tenant").select(*cross_keys, F.lit(1).alias("n"))
    cross_table = read("graph.cross_tenant_dependencies").select(*cross_keys, F.lit(1).alias("n"))
    threshold = F.lit(settings.bottleneck_store_share).cast("decimal(5,4)")
    bottlenecks = read("graph.bottlenecks")
    upstream, paths = read("graph.upstream_dependencies"), read("graph.substitution_paths")
    out_of_range = (
        upstream.filter(f"hops < 1 OR hops > {int(settings.max_hops)}").count()
        + paths.filter(f"hops < 1 OR hops > {int(settings.max_hops)}").count()
    )
    return [
        Check("graph result rows whose tenant_id is not the starting node's owner", foreign, 0),
        Check("edge endpoints that are not nodes", dangling, 0),
        Check("edges whose target owner or cross-tenant flag is wrong", wrong_flag, 0),
        Check(
            "cross-tenant edges missing from or extra in graph.cross_tenant_dependencies",
            mismatches(cross_edges, cross_table, cross_keys, "n"),
            0,
        ),
        Check(
            "bottleneck flags that disagree with the configured threshold",
            bottlenecks.filter(
                F.col("is_bottleneck") != (F.col("store_share") >= threshold)
            ).count(),
            0,
        ),
        Check("traversal rows outside 1 to max_hops hops", out_of_range, 0),
        Check(
            "substitution paths whose length is not hops + 1",
            paths.filter("size(path) <> hops + 1").count(),
            0,
        ),
    ]


# ---- Catalog and tables ----


def catalog_checks(pipeline: Pipeline) -> list[Check]:
    lake = pipeline.lakehouse
    read = lake.read
    root_tables = lakehouse_tables(pipeline)
    datasets = read("catalog.datasets")
    listed = {row.table_name for row in datasets.select("table_name").collect()}
    incomplete = datasets.filter(
        "description IS NULL OR description = '' OR layer IS NULL OR scope IS NULL "
        "OR size(primary_key) = 0 OR (scope = 'TENANT' AND tenant_column IS NULL)"
    ).count()

    columns = read("catalog.columns")
    cataloged: dict[str, list[tuple[str, str, bool]]] = {}
    for row in columns.orderBy("table_name", "ordinal").collect():
        cataloged.setdefault(row.table_name, []).append(
            (row.column_name, row.data_type, row.nullable)
        )
    differing = sorted(
        table
        for table in root_tables
        if cataloged.get(table)
        != [(f.name, f.dataType.simpleString(), f.nullable) for f in read(table).schema.fields]
    )

    edges: dict[str, set[str]] = {}
    kinds: dict[str, str] = {}
    for row in read("catalog.lineage").collect():
        edges.setdefault(row.downstream, set()).add(row.upstream)
        kinds[row.upstream] = row.upstream_type
    unreachable = []
    for table in sorted(t for t in listed if t.split(".")[0] in ("gold", "graph")):
        pending, seen, found = [table], set(), False
        while pending and not found:
            current = pending.pop()
            if current in seen:
                continue
            seen.add(current)
            for upstream in edges.get(current, ()):
                found = found or kinds.get(upstream) == "source_object"
                pending.append(upstream)
        if not found:
            unreachable.append(table)

    complete = [w.id for w in pipeline.config.calendar.windows if pipeline.runlog.is_complete(w.id)]
    freshness = read("catalog.freshness")
    personal_outside_bronze = sorted(
        row.table_name
        for row in datasets.filter("contains_personal_data AND layer <> 'bronze'").collect()
    )
    leaked = 0
    if lake.exists("bronze.open_prices"):
        owners = (
            read("bronze.open_prices")
            .select(F.get_json_object("raw_json", "$.owner").alias("value"))
            .filter(F.col("value").isNotNull())
            .distinct()
        )
        leaked = sum(
            string_values(read(table)).join(owners, "value", "left_semi").count()
            for table in CATALOG
        )

    duplicated = []
    for row in datasets.select("table_name", "primary_key").collect():
        if row.table_name not in root_tables:
            continue
        table = read(row.table_name)
        key = list(row.primary_key)
        if table.count() != table.select(*key).distinct().count():
            duplicated.append(row.table_name)

    return [
        Check("Delta tables missing from catalog.datasets", sorted(root_tables - listed), []),
        Check("catalog.datasets entries without a Delta table", sorted(listed - root_tables), []),
        Check("datasets without description, layer, scope, key or tenant column", incomplete, 0),
        Check("tables whose catalog.columns differ from the Delta schema", differing, []),
        Check(
            "columns without a description",
            columns.filter("description IS NULL OR description = ''").count(),
            0,
        ),
        Check("gold and graph tables whose lineage reaches no source object", unreachable, []),
        Check(
            "catalog.freshness rows (tables x processed windows)",
            freshness.count(),
            len(listed) * len(complete),
        ),
        Check(
            "freshness rows of global tables without a snapshot date",
            freshness.filter("scope = 'GLOBAL' AND source_snapshot_date IS NULL").count(),
            0,
        ),
        Check("tables outside bronze flagged as personal data", personal_outside_bronze, []),
        Check("Open Prices usernames in catalog tables", leaked, 0),
        Check("tables with duplicate values of their declared key", duplicated, []),
    ]


def run_invariant_checks(pipeline: Pipeline) -> list[Check]:
    """Every invariant, on whatever has been loaded. Before any window is complete there
    is nothing to check yet, which fails the first check."""
    checks = run_checks(pipeline)
    complete = [w.id for w in pipeline.config.calendar.windows if pipeline.runlog.is_complete(w.id)]
    checks.append(
        Check(
            "at least one window processed",
            bool(complete),
            True,
            "" if complete else "run the pipeline first: make run-pending",
        )
    )
    if not complete:
        return checks
    return [
        *checks,
        *silver_checks(pipeline),
        *gold_checks(pipeline),
        *graph_checks(pipeline),
        *catalog_checks(pipeline),
    ]
