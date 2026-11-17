"""What every bronze loader needs during one run."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from arkham.config import PipelineConfig
from arkham.lakehouse import TABLES, Lakehouse, with_control_columns
from arkham.landing import LandingZone
from arkham.runlog import RunRecord


class PipelineError(RuntimeError):
    """A run cannot proceed; the message says why and what to do."""


@dataclass
class RunContext:
    spark: SparkSession
    config: PipelineConfig
    lakehouse: Lakehouse
    landing: LandingZone
    fallback_root: Path
    record: RunRecord
    loaded_at: datetime

    @property
    def run_id(self) -> str:
        return self.record.run_id

    def write_bronze(
        self,
        table: str,
        df: DataFrame,
        partition_value: str,
        source: str,
        window_id: str | None,
    ) -> int:
        """Adds the control columns, replaces the table's partition and returns its row count."""
        self.lakehouse.write_partition(
            table,
            with_control_columns(df, window_id, self.run_id, self.loaded_at, source),
            partition_value,
        )
        return self.count_partition(table, partition_value)

    def count_partition(self, table: str, partition_value: str) -> int:
        column = TABLES[table].partition_column
        return self.lakehouse.read(table).filter(F.col(column) == F.lit(partition_value)).count()
