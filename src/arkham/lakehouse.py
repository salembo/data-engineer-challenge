"""Delta table locations, the two write patterns and table fingerprints.

Tables are path-based Delta tables under the lakehouse root
(lakehouse/<layer>/<table>) and are registered as <layer>.<table> in each
Spark session, so they can be queried by name without a shared metastore.

Write patterns (CLAUDE.md): bronze replaces one partition (replaceWhere);
silver and platform tables are written with MERGE on their key; gold tables are
synchronised with their recomputed result (`sync`), which only writes rows that changed.
"""

from __future__ import annotations

import shutil
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from delta.tables import DeltaTable
from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, DataType, StructType

CONTROL_COLUMNS = ("_window_id", "_run_id", "_loaded_at", "_source")
# Columns that legitimately change on every re-run; fingerprints ignore them.
VOLATILE_COLUMNS = ("_run_id", "_loaded_at")
# Excluded from a silver row's content hash, which decides whether a MERGE updates it.
HASH_EXCLUDED = ("_run_id", "_loaded_at", "_row_hash")


@dataclass(frozen=True)
class TableSpec:
    layer: str
    name: str
    partition_column: str | None
    scope: str  # TENANT, GLOBAL or PLATFORM (docs/prd.md, tenancy and scope)
    # Columns besides VOLATILE_COLUMNS that change on every run by design; fingerprints
    # ignore them (the v2-v4 PRD, volatile columns of gold.pipeline_health).
    volatile: tuple[str, ...] = ()

    @property
    def qualified_name(self) -> str:
        return f"{self.layer}.{self.name}"


TABLES: dict[str, TableSpec] = {
    spec.qualified_name: spec
    for spec in (
        TableSpec("bronze", "erp_tenants", "_window_id", "TENANT"),
        TableSpec("bronze", "erp_locations", "_window_id", "TENANT"),
        TableSpec("bronze", "erp_products", "_window_id", "TENANT"),
        TableSpec("bronze", "erp_inventory_ledger", "_window_id", "TENANT"),
        TableSpec("bronze", "partner_events", "_window_id", "TENANT"),
        TableSpec("bronze", "off_products", "_snapshot_date", "GLOBAL"),
        TableSpec("bronze", "off_product_api", "_window_id", "GLOBAL"),
        TableSpec("bronze", "open_prices", "_window_id", "GLOBAL"),
        TableSpec("silver", "dim_tenant", None, "TENANT"),
        TableSpec("silver", "dim_location", None, "TENANT"),
        TableSpec("silver", "dim_product", None, "TENANT"),
        TableSpec("silver", "fact_inventory_movement", None, "TENANT"),
        TableSpec("silver", "fact_pos_sale", None, "TENANT"),
        TableSpec("silver", "fact_shelf_audit", None, "TENANT"),
        TableSpec("silver", "ref_product_public", None, "GLOBAL"),
        TableSpec("silver", "ref_public_price", None, "GLOBAL"),
        TableSpec("platform", "quarantine", None, "PLATFORM"),
        TableSpec("platform", "run_log", None, "PLATFORM"),
        TableSpec("platform", "run_metrics", None, "PLATFORM"),
        TableSpec("gold", "inventory_position", None, "TENANT"),
        TableSpec("gold", "inventory_daily", None, "TENANT"),
        TableSpec("gold", "pos_sales_daily", None, "TENANT"),
        TableSpec("gold", "pos_promo_performance", None, "TENANT"),
        TableSpec("gold", "reconciliation", None, "TENANT"),
        TableSpec(
            "gold",
            "pipeline_health",
            None,
            "TENANT",
            volatile=("run_id", "run_status", "run_started_at", "run_duration_seconds"),
        ),
        TableSpec("graph", "nodes", None, "TENANT"),
        TableSpec("graph", "edges", None, "TENANT"),
        TableSpec("graph", "upstream_dependencies", None, "TENANT"),
        TableSpec("graph", "bottlenecks", None, "TENANT"),
        TableSpec("graph", "cross_tenant_dependencies", None, "TENANT"),
        TableSpec("graph", "substitution_paths", None, "TENANT"),
        TableSpec("catalog", "datasets", None, "PLATFORM"),
        TableSpec("catalog", "columns", None, "PLATFORM"),
        TableSpec("catalog", "lineage", None, "PLATFORM"),
        TableSpec(
            "catalog",
            "freshness",
            None,
            "PLATFORM",
            volatile=("run_id", "run_status", "run_finished_at", "last_loaded_at"),
        ),
    )
}


def sql_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


class Lakehouse:
    def __init__(self, spark: SparkSession, root: Path) -> None:
        self.spark = spark
        self.root = root

    def path(self, qualified_name: str) -> str:
        spec = TABLES[qualified_name]
        return str(self.root / spec.layer / spec.name)

    def exists(self, qualified_name: str) -> bool:
        return DeltaTable.isDeltaTable(self.spark, self.path(qualified_name))

    def read(self, qualified_name: str) -> DataFrame:
        return self.spark.read.format("delta").load(self.path(qualified_name))

    def write_partition(self, qualified_name: str, df: DataFrame, partition_value: str) -> None:
        """Replaces exactly one partition of a table; every other partition is untouched.

        An empty DataFrame clears the partition, so a re-run that finds no data leaves no
        stale rows behind. New source columns are added to the table (mergeSchema); a
        changed type fails the write.
        """
        spec = TABLES[qualified_name]
        if spec.partition_column is None:
            raise ValueError(f"{qualified_name} is not partitioned")
        (
            df.write.format("delta")
            .mode("overwrite")
            .option("replaceWhere", f"{spec.partition_column} = {sql_string(partition_value)}")
            .option("mergeSchema", "true")
            .partitionBy(spec.partition_column)
            .save(self.path(qualified_name))
        )

    def merge(
        self,
        qualified_name: str,
        df: DataFrame,
        keys: list[str],
        delete_missing: bool = False,
    ) -> None:
        """MERGE on `keys`: inserts new rows and updates a row only when its content hash
        changed, so re-running with the same input changes nothing. With
        `delete_missing`, rows absent from `df` are deleted (for tables recomputed whole
        from bronze, such as dimensions and public references)."""
        path = self.path(qualified_name)
        if not self.exists(qualified_name):
            df.limit(0).write.format("delta").save(path)
        condition = " AND ".join(f"t.{key} <=> s.{key}" for key in keys)
        builder = (
            DeltaTable.forPath(self.spark, path)
            .alias("t")
            .merge(df.alias("s"), condition)
            .whenMatchedUpdateAll(condition="t._row_hash <> s._row_hash")
            .whenNotMatchedInsertAll()
        )
        if delete_missing:
            builder = builder.whenNotMatchedBySourceDelete()
        builder.execute()

    def sync(self, qualified_name: str, df: DataFrame, keys: list[str]) -> dict[str, int]:
        """Makes the table equal to `df`, writing only what changed: rows that are new or
        whose content hash differs are merged, and rows whose key is no longer in `df` are
        deleted. Unchanged rows are never matched, so a re-run with the same result adds no
        commit to the Delta history. `df` should be stable (read from a file), because it
        is read more than once. Returns the rows upserted and deleted."""
        path = self.path(qualified_name)
        if not self.exists(qualified_name):
            df.write.format("delta").save(path)
            return {"upserted": self.read(qualified_name).count(), "deleted": 0}
        target = self.read(qualified_name)
        on_key = [df[key].eqNullSafe(target[key]) for key in keys]
        changed = df.join(
            target.select(*keys, "_row_hash"),
            [*on_key, df["_row_hash"] == target["_row_hash"]],
            "left_anti",
        )
        upserted = changed.count()
        if upserted:
            condition = " AND ".join(f"t.{key} <=> s.{key}" for key in keys)
            (
                DeltaTable.forPath(self.spark, path)
                .alias("t")
                .merge(changed.alias("s"), condition)
                .whenMatchedUpdateAll()
                .whenNotMatchedInsertAll()
                .execute()
            )
        gone = target.select(*keys).join(
            df.select(*keys), [target[key].eqNullSafe(df[key]) for key in keys], "left_anti"
        )
        deleted = gone.count()
        if deleted:
            self.delete_keys(qualified_name, gone, keys)
        return {"upserted": upserted, "deleted": deleted}

    def version(self, qualified_name: str) -> int | None:
        """The table's latest Delta version, or None when it does not exist."""
        if not self.exists(qualified_name):
            return None
        latest = DeltaTable.forPath(self.spark, self.path(qualified_name)).history(1)
        return int(latest.select("version").first()[0])

    def commits_since(self, qualified_name: str, version: int | None) -> int:
        """How many commits the table received after `version` (all of them for None)."""
        current = self.version(qualified_name)
        if current is None:
            return 0
        return current - version if version is not None else current + 1

    def delete_keys(self, qualified_name: str, keys_df: DataFrame, keys: list[str]) -> None:
        """Deletes the rows whose key appears in `keys_df` (a MERGE with a delete clause)."""
        if not self.exists(qualified_name):
            return
        condition = " AND ".join(f"t.{key} <=> s.{key}" for key in keys)
        (
            DeltaTable.forPath(self.spark, self.path(qualified_name))
            .alias("t")
            .merge(keys_df.select(*keys).distinct().alias("s"), condition)
            .whenMatchedDelete()
            .execute()
        )

    def scratch_path(self, run_id: str, name: str) -> str:
        """A temporary folder for one run's intermediate results (lakehouse/_scratch/<run>)."""
        return str(self.root / "_scratch" / run_id / name)

    def clear_scratch(self, run_id: str | None = None) -> None:
        """Removes one run's scratch folder, or every one (left behind by killed runs)."""
        target = self.root / "_scratch" / run_id if run_id else self.root / "_scratch"
        shutil.rmtree(target, ignore_errors=True)

    def drop(self, qualified_name: str) -> None:
        """Removes a table entirely. Only `rebuild-silver` uses it, before replaying bronze."""
        shutil.rmtree(self.path(qualified_name), ignore_errors=True)
        # Delta caches each table's log by path; a table recreated at the same path must
        # not be read through the cached log of the deleted one.
        self.spark.sparkContext._jvm.org.apache.spark.sql.delta.DeltaLog.clearCache()

    def register(self) -> list[str]:
        """Registers every existing table as <layer>.<table> in this Spark session."""
        registered = []
        for qualified_name, spec in TABLES.items():
            if not self.exists(qualified_name):
                continue
            self.spark.sql(f"CREATE DATABASE IF NOT EXISTS {spec.layer}")
            self.spark.sql(
                f"CREATE TABLE IF NOT EXISTS {qualified_name} USING DELTA "
                f"LOCATION {sql_string(self.path(qualified_name))}"
            )
            registered.append(qualified_name)
        return registered

    def fingerprint(self, qualified_name: str) -> list[dict[str, object]]:
        """Row count and an order-independent content hash per partition, ignoring the
        columns that change on every run (run ID, load time and the table's own volatile
        columns, and then the row hash too, since it covers them)."""
        spec = TABLES[qualified_name]
        df = self.read(qualified_name)
        ignored = (*VOLATILE_COLUMNS, *spec.volatile, *(("_row_hash",) if spec.volatile else ()))
        content = [c for c in sorted(df.columns) if c not in ignored]
        row_hash: Column = F.xxhash64(F.to_json(F.struct(*content))).cast("decimal(38,0)")
        group = [spec.partition_column] if spec.partition_column else []
        summary = df.groupBy(*group).agg(
            F.count(F.lit(1)).alias("rows"), F.sum(row_hash).cast("string").alias("hash")
        )
        rows = (summary.orderBy(*group) if group else summary).collect()
        return [
            {
                "table": qualified_name,
                "partition": str(row[spec.partition_column]) if spec.partition_column else None,
                "rows": row["rows"],
                "hash": row["hash"],
            }
            for row in rows
        ]


def _literal(value: object, data_type: DataType) -> Column:
    if isinstance(data_type, ArrayType) and value is not None:
        elements = [_literal(item, data_type.elementType) for item in value]  # type: ignore[attr-defined]
        return F.array(*elements).cast(data_type)
    return F.lit(value).cast(data_type)


def jvm_frame(spark: SparkSession, rows: list[tuple[object, ...]], schema: StructType) -> DataFrame:
    """A small DataFrame of literal rows, built inside the JVM.

    `spark.createDataFrame` on Python data starts Python worker processes, which stay
    alive for the whole session and cost about 150 MB of the container's 2 GB.
    """
    if not rows:
        return empty_frame(spark, schema)
    structs = [
        F.struct(
            *(
                _literal(value, field.dataType).alias(field.name)
                for value, field in zip(row, schema.fields, strict=True)
            )
        )
        for row in rows
    ]
    return spark.range(1).select(F.inline(F.array(*structs)))


def empty_frame(spark: SparkSession, schema: StructType) -> DataFrame:
    """A DataFrame with `schema` and no rows, built inside the JVM."""
    return spark.range(0).select(
        *(F.lit(None).cast(field.dataType).alias(field.name) for field in schema.fields)
    )


def with_row_hash(df: DataFrame) -> DataFrame:
    """Adds `_row_hash`: a hash of every column except the run ID and load time."""
    content = [c for c in sorted(df.columns) if c not in HASH_EXCLUDED]
    return df.withColumn("_row_hash", F.xxhash64(F.to_json(F.struct(*content))))


def with_lineage(df: DataFrame, run_id: str, loaded_at: datetime) -> DataFrame:
    """Silver lineage: the row hash, plus the run and time that last wrote the row."""
    return (
        with_row_hash(df)
        .withColumn("_run_id", F.lit(run_id))
        .withColumn("_loaded_at", F.lit(loaded_at).cast("timestamp"))
    )


def with_control_columns(
    df: DataFrame, window_id: str | None, run_id: str, loaded_at: datetime, source: str
) -> DataFrame:
    return (
        df.withColumn("_window_id", F.lit(window_id).cast("string"))
        .withColumn("_run_id", F.lit(run_id))
        .withColumn("_loaded_at", F.lit(loaded_at).cast("timestamp"))
        .withColumn("_source", F.lit(source))
    )
