"""Reads from the legacy ERP over JDBC."""

from __future__ import annotations

from datetime import datetime
from typing import Protocol

from pyspark.sql import DataFrame, SparkSession

from arkham.settings import ErpConnection


def jdbc_options(connection: ErpConnection) -> dict[str, str]:
    """JDBC options for every ERP read: read-only and pinned to UTC on the connection."""
    return {
        "url": connection.jdbc_url,
        "user": connection.user,
        "password": connection.password,
        "driver": "org.postgresql.Driver",
        # Runs on every new connection, so timestamptz values arrive in UTC even if
        # the role or server default changes.
        "sessionInitStatement": "SET TIME ZONE 'UTC'",
        # pgjdbc connection properties: refuse writes and name the client in pg_stat_activity.
        "readOnly": "true",
        "ApplicationName": "arkham-pipeline",
        "fetchsize": "10000",
    }


def read_erp_query(spark: SparkSession, connection: ErpConnection, query: str) -> DataFrame:
    return spark.read.format("jdbc").options(**jdbc_options(connection), query=query).load()


class ErpSource(Protocol):
    """What the pipeline reads from the ERP; tests replace it with fixture data."""

    def read_table(self, table: str) -> DataFrame: ...

    def read_ledger(self, start: datetime, end: datetime) -> DataFrame: ...


class JdbcErpSource:
    """The ERP over JDBC. Column types come from the source; JSONB columns arrive as text."""

    LEDGER = "arkham_erp.inventory_ledger"

    def __init__(self, spark: SparkSession, connection: ErpConnection) -> None:
        self.spark = spark
        self.connection = connection

    def read_table(self, table: str) -> DataFrame:
        return read_erp_query(self.spark, self.connection, f"SELECT * FROM {table}")

    def read_ledger(self, start: datetime, end: datetime) -> DataFrame:
        # Range on the arrival time at the source, start included and end excluded.
        return read_erp_query(
            self.spark,
            self.connection,
            f"SELECT * FROM {self.LEDGER} "
            f"WHERE ingested_at >= TIMESTAMPTZ '{start.isoformat()}' "
            f"AND ingested_at < TIMESTAMPTZ '{end.isoformat()}'",
        )
