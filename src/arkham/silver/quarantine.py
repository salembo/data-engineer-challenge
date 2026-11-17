"""Quarantine: records that fail a silver rule, kept with the reason instead of dropped.

platform.quarantine holds one row per source record (key: source table and record
key) with the current version of the record, the target table it was meant for,
its tenant when known, and every rule it failed.
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from arkham.bronze.context import RunContext
from arkham.lakehouse import with_lineage

TABLE = "platform.quarantine"
KEYS = ["source_table", "record_key"]


def reasons(*rules: tuple[Column, str]) -> Column:
    """Every failed rule, joined with "; "; an empty string when the record passes."""
    return F.concat_ws("; ", *(F.when(condition, F.lit(reason)) for condition, reason in rules))


def quarantine_rows(
    df: DataFrame,
    *,
    source_table: str,
    target_table: str,
    record_key: Column,
    reason: Column,
    raw_record: Column,
    tenant_id: Column | None = None,
    window_id: Column | None = None,
    revision: Column | None = None,
    sequence_cursor: Column | None = None,
) -> DataFrame:
    return df.select(
        F.lit(source_table).alias("source_table"),
        record_key.cast("string").alias("record_key"),
        F.lit(target_table).alias("target_table"),
        (tenant_id if tenant_id is not None else F.lit(None)).cast("string").alias("tenant_id"),
        (window_id if window_id is not None else F.lit(None)).cast("string").alias("window_id"),
        reason.alias("reason"),
        (revision if revision is not None else F.lit(None)).cast("int").alias("revision"),
        (sequence_cursor if sequence_cursor is not None else F.lit(None))
        .cast("bigint")
        .alias("sequence_cursor"),
        raw_record.cast("string").alias("raw_record"),
    )


def write(ctx: RunContext, rows: DataFrame) -> None:
    ctx.lakehouse.merge(TABLE, with_lineage(rows, ctx.run_id, ctx.loaded_at), KEYS)


def release(ctx: RunContext, source_table: str, record_keys: DataFrame, key: str) -> None:
    """Removes records from quarantine once their current version passes every rule."""
    ctx.lakehouse.delete_keys(
        TABLE,
        record_keys.select(
            F.lit(source_table).alias("source_table"),
            F.col(key).cast("string").alias("record_key"),
        ),
        KEYS,
    )
