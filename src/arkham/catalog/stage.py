"""The catalog stage, the last of a run: catalog.datasets, lineage, freshness and columns.

Rules (the v2-v4 PRD, v3 data catalog):
- The registry declares what a table is; names and types of columns are read from the
  Delta tables themselves.
- Registry and reality must agree: a Delta table with no registry entry fails any run; a
  registry entry with no table fails at the end of a window run. The reference load skips
  that second half, and so does a window run for the reference-load tables while the
  reference data has never been loaded. The scratch folder is ignored.
- Freshness: one row per table and processed window, with the run that processed the
  window. Global tables also carry the snapshot date of their public sources.
- The catalog holds table metadata only, never values: personal data is flagged on the
  column that contains it (`bronze.open_prices.raw_json`).
"""

from __future__ import annotations

from datetime import date, datetime

from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DateType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from arkham.bronze.context import PipelineError, RunContext
from arkham.bronze.public_apis import BASELINE, PARTITION_KEY, PRICES_SOURCE, PRODUCT_SOURCE
from arkham.catalog.registry import Registry, load_registry
from arkham.gold.health import RunInfo, chosen_runs
from arkham.lakehouse import TABLES, empty_frame, jvm_frame, with_lineage
from arkham.publish import publish, stable
from arkham.runlog import RUN_LOG, RUN_LOG_SCHEMA, RUN_METRICS, RUN_METRICS_SCHEMA

DATASETS = "catalog.datasets"
COLUMNS = "catalog.columns"
LINEAGE = "catalog.lineage"
FRESHNESS = "catalog.freshness"
CATALOG_TABLES = (DATASETS, COLUMNS, LINEAGE, FRESHNESS)
RUN_COLUMNS = ("run_id", "run_status", "run_finished_at")

WRITE_PATTERNS = {
    "bronze": "replaceWhere on the window's partition",
    "silver": "MERGE on the key, updating rows whose content changed",
    "platform": "MERGE on the key",
    "gold": "recomputed, then MERGE on the grain; rows no longer in the result are deleted",
    "graph": "recomputed, then MERGE on the grain; rows no longer in the result are deleted",
    "catalog": "recomputed, then MERGE on the grain; rows no longer in the result are deleted",
}
# Public source object -> how its snapshot date is found (dump date, or landing manifests).
DUMP_SOURCE = "open_food_facts.dump"
LANDED_SOURCES = {
    "open_food_facts.product_api": PRODUCT_SOURCE,
    "open_prices.prices": PRICES_SOURCE,
}

DATASETS_SCHEMA = StructType(
    [
        StructField("table_name", StringType()),
        StructField("layer", StringType()),
        StructField("description", StringType()),
        StructField("scope", StringType()),
        StructField("tenant_column", StringType()),
        StructField("primary_key", ArrayType(StringType())),
        StructField("partition_columns", ArrayType(StringType())),
        StructField("load_pattern", StringType()),
        StructField("write_pattern", StringType()),
        StructField("contains_personal_data", BooleanType()),
        StructField("personal_data_columns", ArrayType(StringType())),
        StructField("source_objects", ArrayType(StringType())),
        StructField("storage_location", StringType()),
    ]
)
LINEAGE_SCHEMA = StructType(
    [
        StructField("upstream", StringType()),
        StructField("upstream_type", StringType()),
        StructField("upstream_system", StringType()),
        StructField("downstream", StringType()),
        StructField("transformation", StringType()),
    ]
)
COLUMNS_SCHEMA = StructType(
    [
        StructField("table_name", StringType()),
        StructField("column_name", StringType()),
        StructField("ordinal", IntegerType()),
        StructField("data_type", StringType()),
        StructField("nullable", BooleanType()),
        StructField("description", StringType()),
        StructField("is_key", BooleanType()),
        StructField("is_tenant_column", BooleanType()),
        StructField("is_partition_column", BooleanType()),
        StructField("is_personal_data", BooleanType()),
    ]
)
FRESHNESS_SCHEMA = StructType(
    [
        StructField("table_name", StringType()),
        StructField("window_id", StringType()),
        StructField("scope", StringType()),
        StructField("row_count", LongType()),
        StructField("rows_scope", StringType()),
        StructField("source_snapshot_date", DateType()),
        StructField("refreshed_in", StringType()),
        StructField("fallback_used", BooleanType()),
        StructField("run_id", StringType()),
        StructField("run_status", StringType()),
        StructField("run_finished_at", TimestampType()),
        StructField("last_loaded_at", TimestampType()),
    ]
)
RUN_ATTRIBUTES_SCHEMA = StructType(
    [
        StructField("window_id", StringType()),
        StructField("run_id", StringType()),
        StructField("run_status", StringType()),
        StructField("run_finished_at", TimestampType()),
    ]
)
# Columns that attribute a row to a window, in order of preference.
WINDOW_COLUMNS = ("_window_id", "window_id", "_snapshot_window_id")


def discovered_tables(ctx: RunContext) -> set[str]:
    """Every Delta table under the lakehouse root, as layer.table (scratch is ignored)."""
    root = ctx.lakehouse.root
    if not root.exists():
        return set()
    return {
        f"{layer.name}.{table.name}"
        for layer in root.iterdir()
        if layer.is_dir() and not layer.name.startswith("_")
        for table in layer.iterdir()
        if (table / "_delta_log").is_dir()
    }


def check_registry(ctx: RunContext, registry: Registry, discovered: set[str]) -> None:
    """Fails the run when the registry, the code and the lakehouse disagree."""
    problems = []
    declared = set(registry.tables)
    if declared != set(TABLES):
        problems.append(
            "registry and code disagree: only in the registry "
            f"{sorted(declared - set(TABLES))}, only in the code {sorted(set(TABLES) - declared)}"
        )
    for name in sorted(declared & set(TABLES)):
        if registry.tables[name].scope != TABLES[name].scope:
            problems.append(f"{name}: scope differs between the registry and the code")
    unregistered = sorted(discovered - declared)
    if unregistered:
        problems.append(f"Delta tables with no registry entry: {unregistered}")
    for name in sorted(discovered & declared):
        entry = registry.tables[name]
        columns = set(ctx.lakehouse.read(name).columns)
        if entry.tenant_column and entry.tenant_column not in columns:
            problems.append(f"{name}: tenant column {entry.tenant_column} is not in the table")
        missing_key = [c for c in entry.primary_key if c not in columns]
        if missing_key:
            problems.append(f"{name}: key columns {missing_key} are not in the table")
    if problems:
        raise PipelineError("Catalog registry check failed:\n  " + "\n  ".join(problems))


def check_missing_tables(ctx: RunContext, registry: Registry, reference_loaded: bool) -> None:
    """At the end of a window run, every registered table must exist."""
    if ctx.record.command != "run":
        return
    expected = {
        name
        for name, entry in registry.tables.items()
        # The run log writes its own tables when the run ends, after this check.
        if entry.load_pattern != "operational"
        and (reference_loaded or entry.load_pattern != "reference")
    }
    missing = sorted(expected - discovered_tables(ctx))
    if missing:
        raise PipelineError(f"Registered tables missing at the end of the run: {missing}")


def listed_tables(registry: Registry, discovered: set[str]) -> set[str]:
    """The tables the catalog describes: every registered Delta table that exists, plus
    the catalog's own four and the run log's two. Those exist by the end of every run (the
    catalog writes its tables below, the run log its own when the run ends), so listing
    them from the first run keeps the catalog identical on a re-run."""
    always = set(CATALOG_TABLES) | {
        name for name, entry in registry.tables.items() if entry.load_pattern == "operational"
    }
    return (discovered | always) & set(registry.tables)


def _nullable(schema: StructType) -> StructType:
    return StructType([StructField(f.name, f.dataType, True) for f in schema.fields])


def known_schemas(ctx: RunContext) -> dict[str, StructType]:
    """Schemas known without reading a table: the catalog's own tables as they are written
    (lineage columns added; every column nullable after the scratch round trip) and the
    run log's declared schemas. Every other table's schema is read from Delta."""
    own = {
        table: _nullable(
            with_lineage(empty_frame(ctx.spark, schema), ctx.run_id, ctx.loaded_at).schema
        )
        for table, schema in (
            (DATASETS, DATASETS_SCHEMA),
            (COLUMNS, COLUMNS_SCHEMA),
            (LINEAGE, LINEAGE_SCHEMA),
            (FRESHNESS, FRESHNESS_SCHEMA),
        )
    }
    return {**own, RUN_LOG: RUN_LOG_SCHEMA, RUN_METRICS: RUN_METRICS_SCHEMA}


def dataset_rows(registry: Registry, listed: set[str]) -> list[tuple[object, ...]]:
    rows = []
    for name, entry in sorted(registry.tables.items()):
        if name not in listed:
            continue
        spec = TABLES[name]
        rows.append(
            (
                name,
                entry.layer,
                entry.description,
                entry.scope,
                entry.tenant_column,
                list(entry.primary_key),
                [spec.partition_column] if spec.partition_column else [],
                entry.load_pattern,
                WRITE_PATTERNS[entry.layer],
                bool(entry.personal_data_columns),
                list(entry.personal_data_columns),
                registry.source_objects_of(name),
                f"lakehouse/{spec.layer}/{spec.name}",
            )
        )
    return rows


def lineage_rows(registry: Registry) -> list[tuple[object, ...]]:
    rows = []
    for name, entry in sorted(registry.tables.items()):
        for upstream in entry.upstream:
            source = registry.sources.get(upstream)
            rows.append(
                (
                    upstream,
                    "source_object" if source else "table",
                    source.system if source else None,
                    name,
                    entry.transformation,
                )
            )
    return rows


def _column_rows(registry: Registry, table: str, schema: StructType) -> list[tuple[object, ...]]:
    entry, spec = registry.tables[table], TABLES[table]
    return [
        (
            table,
            field.name,
            position,
            field.dataType.simpleString(),
            field.nullable,
            registry.column_description(table, field.name),
            field.name in entry.primary_key,
            field.name == entry.tenant_column,
            field.name == spec.partition_column,
            field.name in entry.personal_data_columns,
        )
        for position, field in enumerate(schema.fields, start=1)
    ]


def column_rows(ctx: RunContext, registry: Registry, listed: set[str]) -> list[tuple[object, ...]]:
    """Columns of every listed table: read from its Delta schema, or known (see above)."""
    known = known_schemas(ctx)
    rows: list[tuple[object, ...]] = []
    for table in sorted(listed):
        schema = known.get(table) or ctx.lakehouse.read(table).schema
        rows.extend(_column_rows(registry, table, schema))
    return rows


def _extracted_on(ctx: RunContext, source: str, partition: str) -> date | None:
    manifest = ctx.landing.read_manifest(source, PARTITION_KEY, partition)
    if not manifest or not manifest.get("extracted_at"):
        return None
    return datetime.fromisoformat(manifest["extracted_at"]).date()


def _snapshot_dates(ctx: RunContext, window_ids: list[str]) -> dict[tuple[str, str], date | None]:
    """(source object, window) -> snapshot date of the public source as of that window:
    the dump's snapshot date, or the latest extraction landed up to that window."""
    dates: dict[tuple[str, str], date | None] = {}
    dump = None
    if ctx.lakehouse.exists("bronze.off_products"):
        row = ctx.lakehouse.read("bronze.off_products").agg(F.max("_snapshot_date")).first()
        dump = row[0] if row else None
        if isinstance(dump, str):
            dump = date.fromisoformat(dump)
    for index, window_id in enumerate(window_ids):
        dates[(DUMP_SOURCE, window_id)] = dump
        for source_object, landed in LANDED_SOURCES.items():
            partitions = [*window_ids[: index + 1]] + (
                [BASELINE] if landed == PRICES_SOURCE else []
            )
            found = [d for p in partitions if (d := _extracted_on(ctx, landed, p)) is not None]
            dates[(source_object, window_id)] = max(found) if found else None
    return dates


def _data_counts(
    ctx: RunContext, table: str
) -> tuple[str, dict[object, tuple[int, object]], tuple[int, object]]:
    """Rows and latest load time of a data table, per window when a column attributes its
    rows to windows, and in total."""
    df = ctx.lakehouse.read(table)
    loaded = F.max("_loaded_at") if "_loaded_at" in df.columns else F.lit(None)
    by_window = next((c for c in WINDOW_COLUMNS if c in df.columns), None)
    counts: dict[object, tuple[int, object]] = {}
    if by_window:
        counts = {
            row[0]: (row[1], row[2])
            for row in df.groupBy(by_window)
            .agg(F.count(F.lit(1)), loaded.cast("timestamp"))
            .collect()
        }
    total = df.agg(F.count(F.lit(1)), loaded.cast("timestamp")).first()
    # A window column that is never set (the dump's _window_id) attributes nothing.
    scope = "window" if any(key is not None for key in counts) else "table"
    return scope, counts, (total[0], total[1])


def freshness_rows(
    ctx: RunContext,
    registry: Registry,
    runs: dict[str, RunInfo],
    listed: set[str],
    catalog_counts: dict[str, int],
) -> list[tuple[object, ...]]:
    """One row per listed table and processed window. Data tables are counted from Delta;
    the catalog's tables from the rows this run writes; the run log is not counted (it
    grows with every run, re-runs included). Neither carries a load time of its own: theirs
    is the run's, in run_finished_at."""
    windows = [w.id for w in ctx.config.calendar.windows if w.id in runs]
    snapshots = _snapshot_dates(ctx, windows)
    rows = []
    for table in sorted(listed):
        entry = registry.tables[table]
        if table in CATALOG_TABLES or entry.load_pattern == "operational":
            scope, counts, total = "table", {}, (catalog_counts.get(table), None)
            if entry.load_pattern == "operational":
                scope = "runs"
        else:
            scope, counts, total = _data_counts(ctx, table)
        public = [
            s for s in registry.source_objects_of(table) if s in (DUMP_SOURCE, *LANDED_SOURCES)
        ]
        for window_id in windows:
            run = runs[window_id]
            count, last_loaded = counts.get(window_id, (0, None)) if scope == "window" else total
            snapshot = refreshed = None
            if entry.scope == "GLOBAL":
                found = [d for s in public if (d := snapshots.get((s, window_id))) is not None]
                snapshot = max(found) if found else None
                refreshed = "reference" if entry.load_pattern == "reference" else window_id
            rows.append(
                (
                    table,
                    window_id,
                    entry.scope,
                    count,
                    scope,
                    snapshot,
                    refreshed,
                    bool(run.fallback_sources),
                    run.run_id,
                    run.status,
                    run.finished_at,
                    last_loaded,
                )
            )
    # catalog.freshness counts itself per window from the rows it is about to write.
    per_window = {window_id: sum(1 for r in rows if r[1] == window_id) for window_id in windows}
    return [(*r[:3], per_window[r[1]], "window", *r[5:]) if r[0] == FRESHNESS else r for r in rows]


def run(ctx: RunContext) -> None:
    registry = load_registry(ctx.config.catalog_registry)
    discovered = discovered_tables(ctx)
    check_registry(ctx, registry, discovered)
    runs = chosen_runs(ctx)
    listed = listed_tables(registry, discovered)
    datasets = dataset_rows(registry, listed)
    lineage = lineage_rows(registry)
    columns = column_rows(ctx, registry, listed)
    counts = {DATASETS: len(datasets), LINEAGE: len(lineage), COLUMNS: len(columns)}
    freshness = freshness_rows(ctx, registry, runs, listed, counts)
    spark = ctx.spark
    publish(ctx, DATASETS, jvm_frame(spark, datasets, DATASETS_SCHEMA), ["table_name"])
    publish(ctx, LINEAGE, jvm_frame(spark, lineage, LINEAGE_SCHEMA), ["upstream", "downstream"])
    publish(
        ctx, FRESHNESS, jvm_frame(spark, freshness, FRESHNESS_SCHEMA), ["table_name", "window_id"]
    )
    publish(ctx, COLUMNS, jvm_frame(spark, columns, COLUMNS_SCHEMA), ["table_name", "column_name"])
    check_missing_tables(ctx, registry, reference_loaded="reference" in runs)


def refresh_run_attributes(ctx: RunContext) -> None:
    """Runs after the run is recorded as finished: the run columns of catalog.freshness
    take the final values from the run log (see gold.stage.refresh_run_attributes)."""
    lake = ctx.lakehouse
    if not lake.exists(FRESHNESS):
        return
    runs = chosen_runs(ctx, include_current=False)
    attributes = jvm_frame(
        ctx.spark,
        [
            (key, run.run_id, run.status, run.finished_at)
            for key, run in runs.items()
            if key != "reference"
        ],
        RUN_ATTRIBUTES_SCHEMA,
    )
    current = lake.read(FRESHNESS).drop("_row_hash", "_run_id", "_loaded_at")
    order = current.columns
    refreshed = current.drop(*RUN_COLUMNS).join(attributes, "window_id", "left").select(*order)
    rows = stable(ctx, with_lineage(refreshed, ctx.run_id, ctx.loaded_at), "catalog_refresh")
    lake.sync(FRESHNESS, rows, ["table_name", "window_id"])
