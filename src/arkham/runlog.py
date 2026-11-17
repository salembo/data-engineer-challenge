"""Run log: one row per run with its status, and row counts per table and outcome.

A run is recorded as RUNNING when it starts and updated when it ends. A run
that is killed stays RUNNING, so its window counts as incomplete until a
later run of the same window succeeds.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, Row, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)
from pyspark.sql.window import Window as SparkWindow

from arkham.lakehouse import Lakehouse, jvm_frame

RUNNING = "RUNNING"
SUCCESS = "SUCCESS"
SUCCESS_WITH_FALLBACK = "SUCCESS_WITH_FALLBACK"
FAILED = "FAILED"
SUCCESSFUL = (SUCCESS, SUCCESS_WITH_FALLBACK)

# Outcomes counted per table and window (docs/prd.md, NFR-8).
READ, WRITTEN, QUARANTINED, FILTERED, SUPERSEDED, FLAGGED = (
    "read",
    "written",
    "quarantined",
    "filtered",
    "superseded",
    "flagged",
)

RUN_LOG = "platform.run_log"
RUN_METRICS = "platform.run_metrics"
# The tenant of a record that carries none, in per-tenant counts and in reconciliation.
NO_TENANT = "UNKNOWN"

RUN_LOG_SCHEMA = StructType(
    [
        StructField("run_id", StringType()),
        StructField("command", StringType()),
        StructField("window_id", StringType()),
        StructField("status", StringType()),
        StructField("started_at", TimestampType()),
        StructField("finished_at", TimestampType()),
        StructField("fallback_sources", ArrayType(StringType())),
        StructField("error_message", StringType()),
    ]
)
RUN_METRICS_SCHEMA = StructType(
    [
        StructField("run_id", StringType()),
        StructField("window_id", StringType()),
        StructField("table_name", StringType()),
        StructField("outcome", StringType()),
        StructField("rows", LongType()),
        StructField("detail", StringType()),
        # Null on a table's total; set on the per-tenant breakdown of a source count
        # (added in v2: rows written before it keep it null).
        StructField("tenant_id", StringType()),
    ]
)


@dataclass
class RunRecord:
    run_id: str
    command: str
    window_id: str | None
    started_at: datetime
    status: str = RUNNING
    finished_at: datetime | None = None
    fallback_sources: list[str] = field(default_factory=list)
    error_message: str | None = None
    # (table, outcome) -> (rows, detail); counting the same pair again adds to it.
    metrics: dict[tuple[str, str], tuple[int, str | None]] = field(default_factory=dict)
    # (table, outcome, tenant) -> rows: a per-tenant breakdown of a total in `metrics`.
    tenant_metrics: dict[tuple[str, str, str], int] = field(default_factory=dict)

    def count(self, table_name: str, outcome: str, rows: int, detail: str | None = None) -> None:
        previous_rows, previous_detail = self.metrics.get((table_name, outcome), (0, None))
        self.metrics[(table_name, outcome)] = (previous_rows + int(rows), detail or previous_detail)

    def count_by_tenant(self, table_name: str, outcome: str, rows: dict[str | None, int]) -> None:
        """Records the total and its breakdown by tenant (the v2-v4 PRD, source count).
        Rows with no tenant are counted under NO_TENANT, never as a null tenant, which
        would collide with the total."""
        self.count(table_name, outcome, sum(rows.values()))
        for tenant_id, tenant_rows in rows.items():
            key = (table_name, outcome, tenant_id if tenant_id is not None else NO_TENANT)
            self.tenant_metrics[key] = self.tenant_metrics.get(key, 0) + int(tenant_rows)

    def fallback(self, description: str) -> None:
        self.fallback_sources.append(description)


class RunLog:
    def __init__(self, spark: SparkSession, lakehouse: Lakehouse) -> None:
        self.spark = spark
        self.lakehouse = lakehouse

    def _upsert(self, table: str, df: DataFrame, keys: list[str], schema: StructType) -> None:
        path = self.lakehouse.path(table)
        if not self.lakehouse.exists(table):
            # Created from the declared schema, where every column is nullable; every later
            # write is a MERGE on the key.
            DeltaTable.createIfNotExists(self.spark).location(path).addColumns(schema).execute()
        else:
            self._add_missing_columns(table, schema)
        condition = " AND ".join(f"t.{k} <=> s.{k}" for k in keys)
        (
            DeltaTable.forPath(self.spark, path)
            .alias("t")
            .merge(df.alias("s"), condition)
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )

    def _add_missing_columns(self, table: str, schema: StructType) -> None:
        """Adds columns declared after the table was created (nullable, at the end)."""
        existing = set(self.lakehouse.read(table).columns)
        missing = [f for f in schema.fields if f.name not in existing]
        if missing:
            columns = ", ".join(f"{f.name} {f.dataType.simpleString()}" for f in missing)
            path = self.lakehouse.path(table)
            self.spark.sql(f"ALTER TABLE delta.`{path}` ADD COLUMNS ({columns})")

    def save(self, record: RunRecord) -> None:
        run = jvm_frame(
            self.spark,
            [
                (
                    record.run_id,
                    record.command,
                    record.window_id,
                    record.status,
                    record.started_at,
                    record.finished_at,
                    record.fallback_sources,
                    record.error_message,
                )
            ],
            RUN_LOG_SCHEMA,
        )
        self._upsert(RUN_LOG, run, ["run_id"], RUN_LOG_SCHEMA)
        if record.metrics:
            totals = [
                (record.run_id, record.window_id, table, outcome, rows, detail, None)
                for (table, outcome), (rows, detail) in sorted(record.metrics.items())
            ]
            by_tenant = [
                (record.run_id, record.window_id, table, outcome, rows, None, tenant)
                for (table, outcome, tenant), rows in sorted(record.tenant_metrics.items())
            ]
            metrics = jvm_frame(self.spark, totals + by_tenant, RUN_METRICS_SCHEMA)
            self._upsert(
                RUN_METRICS,
                metrics,
                ["run_id", "table_name", "outcome", "tenant_id"],
                RUN_METRICS_SCHEMA,
            )

    def start(self, record: RunRecord) -> None:
        record.status = RUNNING
        self.save(record)
        # Columns added to the metrics schema exist before any stage reads the table.
        if self.lakehouse.exists(RUN_METRICS):
            self._add_missing_columns(RUN_METRICS, RUN_METRICS_SCHEMA)

    def finish(self, record: RunRecord, error: BaseException | None = None) -> None:
        record.finished_at = datetime.now(UTC)
        if error is not None:
            record.status = FAILED
            record.error_message = f"{type(error).__name__}: {error}"[:2000]
        else:
            record.status = SUCCESS_WITH_FALLBACK if record.fallback_sources else SUCCESS
        self.save(record)

    def latest_runs(self) -> dict[str | None, Row]:
        """Latest run per window (key None holds the latest reference run)."""
        if not self.lakehouse.exists(RUN_LOG):
            return {}
        log = self.lakehouse.read(RUN_LOG)
        latest = SparkWindow.partitionBy("command", "window_id").orderBy(F.col("started_at").desc())
        rows = (
            log.withColumn("rank", F.row_number().over(latest))
            .filter("rank = 1")
            .drop("rank")
            .collect()
        )
        # Only window runs and reference loads decide completeness; a silver rebuild does not.
        latest_by_key: dict[str | None, Row] = {}
        for row in rows:
            if row.command == "run":
                latest_by_key[row.window_id] = row
            elif row.command == "reference":
                latest_by_key[None] = row
        return latest_by_key

    def is_complete(self, window_id: str) -> bool:
        latest = self.latest_runs().get(window_id)
        return latest is not None and latest.status in SUCCESSFUL

    def reference_loaded(self) -> bool:
        latest = self.latest_runs().get(None)
        return latest is not None and latest.status in SUCCESSFUL
