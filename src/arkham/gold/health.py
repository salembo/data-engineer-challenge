"""Health gold: gold.reconciliation and gold.pipeline_health.

Both are computed from the state of the tables, never from the counts in the run log,
which depend on run order (the v2-v4 PRD, health and reconciliation):

1. Every bronze row is classified on its own: is it the current row in silver, in
   quarantine, filtered (sandbox, duplicates, rows that are not candidates), or superseded
   by a newer version? Each test is independent of the others, so a row that ends up in
   none or in two of them shows as `unexplained_rows`. Rows are attributed to the window,
   or bronze partition, in which they arrived.
2. `source_rows` comes from the run log: the count taken from the source at extraction
   time by the latest successful run of the window (or by the run in progress).
3. reconciliation aggregates the classified rows per window, tenant and source object;
   pipeline_health aggregates the same rows per window, tenant and silver table, so its
   counts always add up to reconciliation's.

For loads that are not windowed, `window_id` holds the bronze partition: `baseline` for
the Open Prices baseline and the snapshot date for the Open Food Facts dump.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)
from pyspark.sql.window import Window as SparkWindow

from arkham.bronze.context import RunContext
from arkham.bronze.public_apis import BASELINE
from arkham.gold.common import GLOBAL
from arkham.gtin import INVALID_CHECK_DIGIT, TOO_LONG, VALID, gtin14_column
from arkham.lakehouse import Lakehouse, empty_frame, jvm_frame
from arkham.runlog import (
    FILTERED,
    NO_TENANT,
    READ,
    RUN_LOG,
    RUN_METRICS,
    SUCCESS,
    SUCCESS_WITH_FALLBACK,
    SUCCESSFUL,
)
from arkham.silver import dimensions, partner, quarantine, reference

RECONCILIATION = "gold.reconciliation"
HEALTH = "gold.pipeline_health"
RECONCILIATION_KEYS = ["window_id", "tenant_id", "source_object"]
HEALTH_KEYS = ["window_id", "tenant_id", "table_name"]

LEDGER = "bronze.erp_inventory_ledger"
EVENTS = "bronze.partner_events"
DUMP = "bronze.off_products"
PRODUCT_API = "bronze.off_product_api"
PRICES = "bronze.open_prices"
# bronze table -> (source object, silver table it feeds)
SOURCE_OBJECTS = {
    "bronze.erp_tenants": ("erp.tenants", "silver.dim_tenant"),
    "bronze.erp_locations": ("erp.locations", "silver.dim_location"),
    "bronze.erp_products": ("erp.products", "silver.dim_product"),
    LEDGER: ("erp.inventory_ledger", "silver.fact_inventory_movement"),
    EVENTS: ("partner_api.events", partner.SALES),  # audits go to partner.AUDITS
    DUMP: ("open_food_facts.dump", "silver.ref_product_public"),
    PRODUCT_API: ("open_food_facts.product_api", "silver.ref_product_public"),
    PRICES: ("open_prices.prices", "silver.ref_public_price"),
}
TENANT_SOURCES = ("bronze.erp_tenants", "bronze.erp_locations", "bronze.erp_products", LEDGER)
GLOBAL_SOURCES = (PRODUCT_API, PRICES)
REFERENCE = "reference"  # run key of the reference load

# One row per bronze row (or per group of rows filtered before bronze).
ROW_SCHEMA = StructType(
    [
        StructField("window_id", StringType()),
        StructField("tenant_id", StringType()),
        StructField("source_object", StringType()),
        StructField("bronze_table", StringType()),
        StructField("table_name", StringType()),
        StructField("rows", LongType()),
        StructField("in_bronze", BooleanType()),
        StructField("is_silver", BooleanType()),
        StructField("is_quarantined", BooleanType()),
        StructField("is_filtered", BooleanType()),
        StructField("is_superseded", BooleanType()),
        StructField("is_flagged", BooleanType()),
        StructField("is_late", BooleanType()),
        StructField("is_reused", BooleanType()),
        StructField("tenant_mismatch", BooleanType()),
        StructField("gtin_status", StringType()),
        StructField("quarantine_reason", StringType()),
        StructField("is_active_sale", BooleanType()),
        StructField("has_public_price", BooleanType()),
    ]
)
SOURCE_COUNT_SCHEMA = StructType(
    [
        StructField("window_id", StringType()),
        StructField("tenant_id", StringType()),
        StructField("bronze_table", StringType()),
        StructField("source_rows", LongType()),
        StructField("filtered_before_bronze", LongType()),
    ]
)
RUN_SCHEMA = StructType(
    [
        StructField("run_key", StringType()),
        StructField("run_id", StringType()),
        StructField("run_status", StringType()),
        StructField("run_started_at", TimestampType()),
        StructField("run_duration_seconds", LongType()),
        StructField("fallback_used", BooleanType()),
        StructField("fallback_sources", ArrayType(StringType())),
    ]
)


def _shape(df: DataFrame, **values: Column) -> DataFrame:
    """Selects the row schema: given columns first, then df's own, then a default."""
    columns = []
    for field in ROW_SCHEMA.fields:
        if field.name in values:
            column = values[field.name]
        elif field.name in df.columns:
            column = F.col(field.name)
        elif isinstance(field.dataType, BooleanType):
            column = F.lit(False)
        elif field.name == "rows":
            column = F.lit(1)
        else:
            column = F.lit(None)
        if isinstance(field.dataType, BooleanType):
            column = F.coalesce(column, F.lit(False))
        columns.append(column.cast(field.dataType).alias(field.name))
    return df.select(*columns)


QUARANTINE_SCHEMA = StructType(
    [
        StructField("record_key", StringType()),
        StructField("_q_window", StringType()),
        StructField("_q_reason", StringType()),
        StructField("_q_cursor", LongType()),
    ]
)


def _quarantined(lake: Lakehouse, source_table: str) -> DataFrame:
    """The current quarantined rows of one bronze table."""
    if not lake.exists(quarantine.TABLE):
        return empty_frame(lake.spark, QUARANTINE_SCHEMA)
    rows = lake.read(quarantine.TABLE)
    return rows.filter(F.col("source_table") == source_table).select(
        "record_key",
        F.col("window_id").alias("_q_window"),
        F.col("reason").alias("_q_reason"),
        F.col("sequence_cursor").alias("_q_cursor"),
    )


def _window_tenants(lake: Lakehouse) -> DataFrame:
    """The sandbox flag of each tenant in each window's own snapshot, as silver used it."""
    return dimensions.tenants(lake.read("bronze.erp_tenants")).select(
        F.col("_snapshot_window_id").alias("_window_id"),
        "tenant_id",
        "is_sandbox",
    )


def _differs(left: str, right: str) -> Column:
    return ~F.col(left).eqNullSafe(F.col(right))


def ledger_rows(lake: Lakehouse) -> DataFrame:
    source_object, table_name = SOURCE_OBJECTS[LEDGER]
    bronze = lake.read(LEDGER).select("_window_id", "tenant_id", "ledger_entry_id")
    facts = lake.read("silver.fact_inventory_movement").select(
        "ledger_entry_id",
        F.col("_window_id").alias("_fact_window"),
        F.col("is_late_arriving").alias("is_late"),
        F.col("is_reference_doc_reused").alias("is_reused"),
        "location_owner_tenant_id",
        "product_owner_tenant_id",
    )
    held = _quarantined(lake, LEDGER)
    rows = (
        bronze.join(_window_tenants(lake), ["_window_id", "tenant_id"], "left")
        .join(facts, "ledger_entry_id", "left")
        .join(
            held,
            (F.col("ledger_entry_id").cast("string") == F.col("record_key"))
            & (F.col("_window_id") == F.col("_q_window")),
            "left",
        )
    )
    is_silver = F.col("_fact_window").isNotNull() & (F.col("_fact_window") == F.col("_window_id"))
    return _shape(
        rows,
        window_id=F.col("_window_id"),
        tenant_id=F.coalesce(F.col("tenant_id"), F.lit(NO_TENANT)),
        source_object=F.lit(source_object),
        bronze_table=F.lit(LEDGER),
        table_name=F.lit(table_name),
        in_bronze=F.lit(True),
        is_silver=is_silver,
        is_quarantined=F.col("record_key").isNotNull(),
        is_filtered=F.col("is_sandbox"),
        is_flagged=is_silver & (F.col("is_late") | F.col("is_reused")),
        is_late=is_silver & F.col("is_late"),
        is_reused=is_silver & F.col("is_reused"),
        tenant_mismatch=is_silver
        & (
            _differs("tenant_id", "location_owner_tenant_id")
            | _differs("tenant_id", "product_owner_tenant_id")
        ),
        quarantine_reason=F.col("_q_reason"),
    )


def partner_rows(lake: Lakehouse, sale_prices: DataFrame | None) -> DataFrame:
    source_object, _ = SOURCE_OBJECTS[EVENTS]
    raw = F.col("raw_json")
    bronze = lake.read(EVENTS).select(
        "_window_id",
        "sequence_cursor",
        "event_id",
        F.get_json_object(raw, "$.tenant_id").alias("tenant_id"),
        F.get_json_object(raw, "$.event_type").alias("event_type"),
        F.get_json_object(raw, "$.revision").try_cast("int").alias("revision"),
    )
    with_tenant = bronze.join(_window_tenants(lake), ["_window_id", "tenant_id"], "left")
    real = ~F.coalesce(F.col("is_sandbox"), F.lit(False))
    newest = (
        with_tenant.filter(real & F.col("event_id").isNotNull() & F.col("revision").isNotNull())
        .groupBy("event_id")
        .agg(F.max(partner.version()).alias("_newest"))
    )
    current_parts = []
    for table in (partner.SALES, partner.AUDITS):
        if lake.exists(table):
            current_parts.append(
                lake.read(table).select(
                    F.col("sequence_cursor").alias("_current_cursor"),
                    F.col("is_late_arriving").alias("is_late"),
                    "location_owner_tenant_id",
                    "product_owner_tenant_id",
                    F.col("event_gtin_status").alias("gtin_status"),
                    "record_state",
                    F.lit(table).alias("_current_table"),
                )
            )
    rows = with_tenant.join(newest, "event_id", "left")
    if current_parts:
        current = current_parts[0]
        for part in current_parts[1:]:
            current = current.unionByName(part)
        rows = rows.join(current, F.col("sequence_cursor") == F.col("_current_cursor"), "left")
    else:
        rows = rows.select(
            "*",
            F.lit(None).cast("bigint").alias("_current_cursor"),
            F.lit(None).cast("string").alias("_current_table"),
            F.lit(None).cast("string").alias("record_state"),
        )
    held = _quarantined(lake, EVENTS).select("_q_cursor", "_q_reason")
    rows = rows.join(held, F.col("sequence_cursor") == F.col("_q_cursor"), "left")
    if sale_prices is not None:
        rows = rows.join(
            sale_prices.select(
                F.col("event_id").alias("_priced_event"),
                (F.col("price_match_level") != "NONE").alias("has_public_price"),
            ),
            F.col("event_id") == F.col("_priced_event"),
            "left",
        )
    is_silver = F.col("_current_cursor").isNotNull()
    table_name = F.when(
        F.col("event_type") == partner.SHELF_AUDIT, F.lit(partner.AUDITS)
    ).otherwise(F.lit(partner.SALES))
    return _shape(
        rows,
        window_id=F.col("_window_id"),
        tenant_id=F.coalesce(F.col("tenant_id"), F.lit(NO_TENANT)),
        source_object=F.lit(source_object),
        bronze_table=F.lit(EVENTS),
        table_name=table_name,
        in_bronze=F.lit(True),
        is_silver=is_silver,
        is_quarantined=F.col("_q_cursor").isNotNull(),
        is_filtered=~real,
        is_superseded=real & F.col("_newest").isNotNull() & (partner.version() < F.col("_newest")),
        is_flagged=is_silver & F.col("is_late"),
        is_late=is_silver & F.col("is_late"),
        tenant_mismatch=is_silver
        & (
            _differs("tenant_id", "location_owner_tenant_id")
            | _differs("tenant_id", "product_owner_tenant_id")
        ),
        gtin_status=F.when(is_silver, F.col("gtin_status")),
        quarantine_reason=F.col("_q_reason"),
        is_active_sale=is_silver
        & (F.col("_current_table") == partner.SALES)
        & (F.col("record_state") == "ACTIVE"),
    )


def dimension_rows(lake: Lakehouse, latest: str | None) -> list[DataFrame]:
    parts = []
    for bronze_table, key in (
        ("bronze.erp_tenants", "tenant_id"),
        ("bronze.erp_locations", "location_id"),
        ("bronze.erp_products", "product_id"),
    ):
        source_object, silver_table = SOURCE_OBJECTS[bronze_table]
        if not lake.exists(bronze_table):
            continue
        bronze = lake.read(bronze_table).select(
            "_window_id", *dict.fromkeys(["tenant_id", key]), F.col(key).alias("_key")
        )
        rows = bronze
        if lake.exists(silver_table):
            silver = lake.read(silver_table)
            status = F.col("gtin_status") if "gtin_status" in silver.columns else F.lit(None)
            rows = rows.join(
                silver.select(
                    F.col(key).alias("_silver_key"),
                    F.col("_snapshot_window_id").alias("_silver_window"),
                    status.cast("string").alias("gtin_status"),
                ),
                (F.col("_key") == F.col("_silver_key"))
                & (F.col("_window_id") == F.col("_silver_window")),
                "left",
            )
        else:
            rows = rows.select(
                "*",
                F.lit(None).cast("string").alias("_silver_key"),
                F.lit(None).cast("string").alias("gtin_status"),
            )
        held = _quarantined(lake, bronze_table)
        rows = rows.join(
            held,
            (F.col("_key") == F.col("record_key")) & (F.col("_window_id") == F.col("_q_window")),
            "left",
        )
        is_silver = F.col("_silver_key").isNotNull()
        parts.append(
            _shape(
                rows,
                window_id=F.col("_window_id"),
                tenant_id=F.coalesce(F.col("tenant_id"), F.lit(NO_TENANT)),
                source_object=F.lit(source_object),
                bronze_table=F.lit(bronze_table),
                table_name=F.lit(silver_table),
                in_bronze=F.lit(True),
                is_silver=is_silver,
                is_quarantined=F.col("record_key").isNotNull(),
                # Silver holds the latest snapshot; every earlier one is superseded.
                is_superseded=F.lit(latest).isNull() | (F.col("_window_id") != F.lit(latest)),
                is_flagged=is_silver
                & F.col("gtin_status").isNotNull()
                & (F.col("gtin_status") != VALID),
                gtin_status=F.when(is_silver, F.col("gtin_status")),
                quarantine_reason=F.col("_q_reason"),
            )
        )
    return parts


def _product_rows(
    lake: Lakehouse, bronze_table: str, candidates: DataFrame, source_label: str
) -> DataFrame:
    """Rows of a public product source: the version that won in ref_product_public is in
    silver; the other candidates, including the losing source's row, are superseded; rows
    that are not candidates (no barcode, product not found) are filtered."""
    source_object, table_name = SOURCE_OBJECTS[bronze_table]
    best_first = SparkWindow.partitionBy("gtin14", "_candidate").orderBy(
        F.col("last_modified_t").desc_nulls_last(), F.col("_partition").desc()
    )
    ranked = candidates.withColumn("_rank", F.row_number().over(best_first))
    if lake.exists(reference.PRODUCTS):
        winners = lake.read(reference.PRODUCTS).select(
            F.col("gtin14").alias("_ref_gtin14"), F.col("source").alias("_ref_source")
        )
        ranked = ranked.join(winners, F.col("gtin14") == F.col("_ref_gtin14"), "left")
    else:
        ranked = ranked.select("*", F.lit(None).cast("string").alias("_ref_source"))
    won = F.col("_candidate") & (F.col("_rank") == 1) & (F.col("_ref_source") == source_label)
    return _shape(
        ranked,
        window_id=F.col("_partition"),
        tenant_id=F.lit(GLOBAL),
        source_object=F.lit(source_object),
        bronze_table=F.lit(bronze_table),
        table_name=F.lit(table_name),
        in_bronze=F.lit(True),
        is_silver=won,
        is_filtered=~F.col("_candidate"),
        is_superseded=F.col("_candidate") & F.col("_ref_source").isNotNull() & ~won,
    )


def product_api_rows(lake: Lakehouse) -> DataFrame:
    api = lake.read(PRODUCT_API).select(
        F.col("_window_id").alias("_partition"),
        gtin14_column(F.col("code")).alias("gtin14"),
        F.from_json("raw_json", reference.PRODUCT_API_SCHEMA).alias("r"),
    )
    candidates = api.select(
        "_partition",
        "gtin14",
        F.col("r.product.last_modified_t").try_cast("bigint").alias("last_modified_t"),
        (F.col("r.status") == 1).alias("_found"),
    ).withColumn(
        "_candidate", F.coalesce(F.col("_found"), F.lit(False)) & F.col("gtin14").isNotNull()
    )
    return _product_rows(lake, PRODUCT_API, candidates, "api")


def dump_rows(lake: Lakehouse) -> DataFrame:
    dump = lake.read(DUMP)
    modified = F.col("last_modified_t") if "last_modified_t" in dump.columns else F.lit(None)
    candidates = dump.select(
        F.col("_snapshot_date").cast("string").alias("_partition"),
        gtin14_column(F.col("code")).alias("gtin14"),
        modified.try_cast("bigint").alias("last_modified_t"),
    ).withColumn("_candidate", F.col("gtin14").isNotNull())
    return _product_rows(lake, DUMP, candidates, "dump")


def price_rows(lake: Lakehouse) -> DataFrame:
    """The same resolution as silver: the newest version of each price id is current,
    unless it is marked as a duplicate of another price."""
    source_object, table_name = SOURCE_OBJECTS[PRICES]
    parsed = (
        lake.read(PRICES)
        .select("_window_id", F.from_json("raw_json", reference.PRICE_SCHEMA).alias("p"))
        .select("_window_id", "p.id", "p.updated", "p.duplicate_of")
    )
    newest_first = SparkWindow.partitionBy("id").orderBy(
        F.col("updated").desc(), F.col("_window_id").desc()
    )
    ranked = parsed.withColumn("_rank", F.row_number().over(newest_first))
    if lake.exists(reference.PRICES):
        held = lake.read(reference.PRICES).select(
            F.col("id").alias("_silver_id"), F.col("source_partition").alias("_silver_partition")
        )
        ranked = ranked.join(
            held,
            (F.col("id") == F.col("_silver_id"))
            & (F.col("_window_id") == F.col("_silver_partition")),
            "left",
        )
    else:
        ranked = ranked.select("*", F.lit(None).cast("bigint").alias("_silver_id"))
    current = F.col("_rank") == 1
    return _shape(
        ranked,
        window_id=F.col("_window_id"),
        tenant_id=F.lit(GLOBAL),
        source_object=F.lit(source_object),
        bronze_table=F.lit(PRICES),
        table_name=F.lit(table_name),
        in_bronze=F.lit(True),
        is_silver=current & F.col("duplicate_of").isNull() & F.col("_silver_id").isNotNull(),
        is_filtered=current & F.col("duplicate_of").isNotNull(),
        is_superseded=~current,
    )


@dataclass
class RunInfo:
    run_id: str
    status: str
    started_at: datetime
    finished_at: datetime | None
    fallback_sources: list[str]
    # (table, outcome, tenant or None, rows, detail)
    metrics: list[tuple[str, str, str | None, int, str | None]]


def chosen_runs(ctx: RunContext, include_current: bool = True) -> dict[str, RunInfo]:
    """The run whose counts and attributes describe each window (key: window ID) and the
    reference data (key REFERENCE): the latest successful one, or the run in progress."""
    lake, record = ctx.lakehouse, ctx.record
    runs: dict[str, RunInfo] = {}
    if lake.exists(RUN_LOG):
        log = lake.read(RUN_LOG).filter(
            F.col("command").isin("run", "reference") & F.col("status").isin(*SUCCESSFUL)
        )
        latest = SparkWindow.partitionBy("command", "window_id").orderBy(F.col("started_at").desc())
        chosen = log.withColumn("_rank", F.row_number().over(latest)).filter("_rank = 1").collect()
        ids = [row.run_id for row in chosen]
        metrics: dict[str, list[tuple[str, str, str | None, int, str | None]]] = {}
        if ids and lake.exists(RUN_METRICS):
            table = lake.read(RUN_METRICS)
            tenant = F.col("tenant_id") if "tenant_id" in table.columns else F.lit(None)
            for row in (
                table.filter(F.col("run_id").isin(ids))
                .select(
                    "run_id", "table_name", "outcome", tenant.alias("tenant_id"), "rows", "detail"
                )
                .collect()
            ):
                metrics.setdefault(row.run_id, []).append(
                    (row.table_name, row.outcome, row.tenant_id, row.rows, row.detail)
                )
        for row in chosen:
            key = row.window_id if row.command == "run" else REFERENCE
            runs[key] = RunInfo(
                row.run_id,
                row.status,
                row.started_at,
                row.finished_at,
                list(row.fallback_sources or []),
                metrics.get(row.run_id, []),
            )
    if include_current and record.command in ("run", "reference"):
        # The run in progress replaces the last one of its window: gold is its last stage,
        # so it finishes with this status unless writing gold itself fails. Its duration
        # is provisional until `refresh_run_attributes` runs after the run is finished.
        key = record.window_id if record.command == "run" else REFERENCE
        current_metrics = [
            (table, outcome, None, rows, detail)
            for (table, outcome), (rows, detail) in record.metrics.items()
        ] + [
            (table, outcome, tenant, rows, None)
            for (table, outcome, tenant), rows in record.tenant_metrics.items()
        ]
        runs[key] = RunInfo(
            record.run_id,
            SUCCESS_WITH_FALLBACK if record.fallback_sources else SUCCESS,
            record.started_at,
            datetime.now(UTC),
            list(record.fallback_sources),
            current_metrics,
        )
    return runs


def source_counts(ctx: RunContext, runs: dict[str, RunInfo]) -> DataFrame:
    """`source_rows` per bronze partition, tenant and table, from each chosen run."""
    rows: list[tuple[str, str, str, int, int | None]] = []
    for key, run in runs.items():
        read = [m for m in run.metrics if m[1] == READ]
        if key != REFERENCE:
            for table, _, tenant, count, _ in read:
                if table in (*TENANT_SOURCES, EVENTS) and tenant is not None:
                    rows.append((key, tenant, table, count, None))
                elif table in GLOBAL_SOURCES and tenant is None:
                    rows.append((key, GLOBAL, table, count, None))
            continue
        filtered = {m[0]: m[3] for m in run.metrics if m[1] == FILTERED and m[2] is None}
        for table, _, tenant, count, detail in read:
            if tenant is not None:
                continue
            if table == PRICES:
                rows.append((BASELINE, GLOBAL, PRICES, count, None))
            elif table == DUMP and detail and detail.startswith("snapshot "):
                snapshot = detail.removeprefix("snapshot ").strip()
                rows.append((snapshot, GLOBAL, DUMP, count, filtered.get(DUMP)))
    return jvm_frame(ctx.spark, rows, SOURCE_COUNT_SCHEMA)


def run_attributes(ctx: RunContext, runs: dict[str, RunInfo]) -> DataFrame:
    rows = []
    for key, run in runs.items():
        finished = run.finished_at
        duration = (
            int((finished - run.started_at).total_seconds())
            if finished is not None and run.started_at is not None
            else None
        )
        rows.append(
            (
                key,
                run.run_id,
                run.status,
                run.started_at,
                duration,
                bool(run.fallback_sources),
                sorted(run.fallback_sources),
            )
        )
    return jvm_frame(ctx.spark, rows, RUN_SCHEMA)


def classified_rows(ctx: RunContext, sale_prices: DataFrame | None, counts: DataFrame) -> DataFrame:
    """Every bronze row, classified, plus the rows the dump filtered before bronze."""
    lake = ctx.lakehouse
    parts: list[DataFrame] = []
    if lake.exists("bronze.erp_tenants"):
        latest = dimensions.latest_window(lake, ctx.config.calendar)
        parts.extend(dimension_rows(lake, latest))
        if lake.exists(LEDGER) and lake.exists("silver.fact_inventory_movement"):
            parts.append(ledger_rows(lake))
        if lake.exists(EVENTS):
            parts.append(partner_rows(lake, sale_prices))
    if lake.exists(PRODUCT_API):
        parts.append(product_api_rows(lake))
    if lake.exists(DUMP):
        parts.append(dump_rows(lake))
        source_object, table_name = SOURCE_OBJECTS[DUMP]
        before_bronze = counts.filter(
            (F.col("bronze_table") == DUMP) & F.col("filtered_before_bronze").isNotNull()
        )
        parts.append(
            _shape(
                before_bronze,
                tenant_id=F.lit(GLOBAL),
                source_object=F.lit(source_object),
                table_name=F.lit(table_name),
                rows=F.col("filtered_before_bronze"),
                in_bronze=F.lit(False),
                is_filtered=F.lit(True),
            )
        )
    if lake.exists(PRICES):
        parts.append(price_rows(lake))
    if not parts:
        return jvm_frame(ctx.spark, [], ROW_SCHEMA)
    rows = parts[0]
    for part in parts[1:]:
        rows = rows.unionByName(part)
    return rows


def _sum_where(condition: Column) -> Column:
    return F.sum(F.when(condition, F.col("rows")).otherwise(F.lit(0))).cast("bigint")


def _outcome_totals() -> list[Column]:
    return [
        _sum_where(F.col("is_silver")).alias("silver_rows"),
        _sum_where(F.col("is_quarantined")).alias("quarantined"),
        _sum_where(F.col("is_filtered")).alias("filtered"),
        _sum_where(F.col("is_superseded")).alias("superseded"),
        _sum_where(F.col("is_flagged")).alias("flagged"),
    ]


def reconciliation(rows: DataFrame, counts: DataFrame) -> DataFrame:
    totals = rows.groupBy("window_id", "tenant_id", "source_object", "bronze_table").agg(
        _sum_where(F.col("in_bronze")).alias("bronze_rows"),
        F.sum("rows").cast("bigint").alias("_received"),
        *_outcome_totals(),
    )
    mapping = F.create_map(
        *(item for table, (obj, _) in SOURCE_OBJECTS.items() for item in (F.lit(table), F.lit(obj)))
    )
    sources = counts.select(
        "window_id",
        "tenant_id",
        "bronze_table",
        mapping[F.col("bronze_table")].alias("source_object"),
        "source_rows",
    )
    joined = totals.join(
        sources, ["window_id", "tenant_id", "source_object", "bronze_table"], "full_outer"
    )
    zero = F.lit(0).cast("bigint")
    counted = [
        F.coalesce(F.col(c), zero).alias(c)
        for c in ("bronze_rows", "silver_rows", "quarantined", "filtered", "superseded", "flagged")
    ]
    result = joined.select(
        *RECONCILIATION_KEYS,
        "bronze_table",
        "source_rows",
        *counted,
        F.coalesce(F.col("_received"), zero).alias("_received"),
    )
    # The rows the source delivered (or, without a recorded count, the rows received)
    # minus every explained outcome.
    explained = (
        F.col("silver_rows") + F.col("quarantined") + F.col("filtered") + F.col("superseded")
    )
    return result.select(
        *RECONCILIATION_KEYS,
        "bronze_table",
        "source_rows",
        "bronze_rows",
        "silver_rows",
        "quarantined",
        "filtered",
        "superseded",
        "flagged",
        (F.coalesce(F.col("source_rows"), F.col("_received")) - explained).alias(
            "unexplained_rows"
        ),
    )


RUN_COLUMNS = ("run_id", "run_status", "run_started_at", "run_duration_seconds")


def _run_key(window_ids: list[str]) -> Column:
    """Window rows describe their window's run; partitions of reference loads (baseline,
    dump snapshot) describe the reference run."""
    return F.when(F.col("window_id").isin(*window_ids), F.col("window_id")).otherwise(
        F.lit(REFERENCE)
    )


def with_final_run_attributes(
    health: DataFrame, runs: DataFrame, window_ids: list[str]
) -> DataFrame:
    """pipeline_health rows with their run columns taken again from `runs`."""
    columns = health.columns
    return (
        health.drop(*RUN_COLUMNS)
        .withColumn("run_key", _run_key(window_ids))
        .join(runs.select("run_key", *RUN_COLUMNS), "run_key", "left")
        .select(*columns)
    )


REASON_TYPE = ArrayType(
    StructType([StructField("reason", StringType()), StructField("rows", LongType())])
)


def pipeline_health(rows: DataFrame, runs: DataFrame, window_ids: list[str]) -> DataFrame:
    group = ["window_id", "tenant_id", "table_name", "source_object"]
    totals = rows.groupBy(*group).agg(
        F.sum("rows").cast("bigint").alias("rows_received"),
        *_outcome_totals(),
        _sum_where(F.col("is_late")).alias("late_arriving_rows"),
        _sum_where(F.col("is_reused")).alias("reused_document_rows"),
        _sum_where(F.col("tenant_mismatch")).alias("tenant_mismatch_rows"),
        _sum_where(F.col("gtin_status") == VALID).alias("barcodes_valid"),
        _sum_where(F.col("gtin_status") == INVALID_CHECK_DIGIT).alias(
            "barcodes_invalid_check_digit"
        ),
        _sum_where(F.col("gtin_status") == TOO_LONG).alias("barcodes_too_long"),
        _sum_where(F.col("is_active_sale") & F.col("has_public_price")).alias(
            "sales_with_public_price"
        ),
        _sum_where(F.col("is_active_sale") & ~F.col("has_public_price")).alias(
            "sales_without_public_price"
        ),
    )
    reasons = (
        rows.filter(F.col("is_quarantined"))
        .groupBy(*group, F.coalesce(F.col("quarantine_reason"), F.lit("unknown")).alias("reason"))
        .agg(F.sum("rows").cast("bigint").alias("n"))
        .groupBy(*group)
        .agg(
            F.sort_array(F.collect_list(F.struct("reason", F.col("n").alias("rows")))).alias(
                "_reasons"
            )
        )
    )
    active = F.col("sales_with_public_price") + F.col("sales_without_public_price")
    return (
        totals.join(reasons, group, "left")
        .withColumn("run_key", _run_key(window_ids))
        .join(runs, "run_key", "left")
        .select(
            *HEALTH_KEYS,
            "source_object",
            "rows_received",
            F.col("silver_rows").alias("written"),
            "quarantined",
            "filtered",
            "superseded",
            "flagged",
            F.coalesce(F.col("_reasons"), F.array().cast(REASON_TYPE)).alias("quarantine_reasons"),
            "late_arriving_rows",
            "reused_document_rows",
            "tenant_mismatch_rows",
            "barcodes_valid",
            "barcodes_invalid_check_digit",
            "barcodes_too_long",
            "sales_with_public_price",
            "sales_without_public_price",
            F.when(
                active > 0,
                F.round(
                    F.col("sales_with_public_price").cast("decimal(20,4)")
                    / active.cast("decimal(20,4)"),
                    4,
                ),
            )
            .cast("decimal(5,4)")
            .alias("public_price_coverage"),
            F.coalesce(F.col("fallback_used"), F.lit(False)).alias("fallback_used"),
            F.coalesce(F.col("fallback_sources"), F.array().cast("array<string>")).alias(
                "fallback_sources"
            ),
            "run_id",
            "run_status",
            "run_started_at",
            "run_duration_seconds",
        )
    )
