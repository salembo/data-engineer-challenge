"""How derived layers (gold, graph, catalog) write: recompute, fix on disk, synchronise."""

from __future__ import annotations

from pyspark.sql import DataFrame

from arkham.bronze.context import RunContext
from arkham.lakehouse import with_lineage
from arkham.runlog import WRITTEN


def stable(ctx: RunContext, df: DataFrame, name: str) -> DataFrame:
    """Writes `df` to the run's scratch folder and reads it back, so a result that is used
    more than once is computed once and cannot change between uses."""
    path = ctx.lakehouse.scratch_path(ctx.run_id, name)
    df.write.mode("overwrite").parquet(path)
    return ctx.spark.read.parquet(path)


def publish(ctx: RunContext, table: str, df: DataFrame, keys: list[str]) -> int:
    """Synchronises a derived table with its recomputed result: only rows that changed are
    written (Lakehouse.sync), and the table's row count is recorded in the run log."""
    rows = stable(ctx, with_lineage(df, ctx.run_id, ctx.loaded_at), f"publish_{table}")
    changes = ctx.lakehouse.sync(table, rows, keys)
    written = ctx.lakehouse.read(table).count()
    ctx.record.count(
        table, WRITTEN, written, f"{changes['upserted']} upserted, {changes['deleted']} deleted"
    )
    return written
