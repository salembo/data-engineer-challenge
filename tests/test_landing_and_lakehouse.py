from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest
from pyspark.sql import DataFrame, SparkSession

from arkham.lakehouse import Lakehouse, with_control_columns
from arkham.landing import LandingZone, one_line

TABLE = "bronze.erp_tenants"


def test_landed_partition_is_complete_only_with_its_manifest(tmp_path: Path) -> None:
    landing = LandingZone(tmp_path)
    assert landing.read_manifest("src", "window_id", "W1") is None
    landing.write_partition(
        "src", "window_id", "W1", {"records": 2}, responses=[b'{"a":\n1}', b"{}"]
    )
    assert landing.read_manifest("src", "window_id", "W1") == {"records": 2}
    assert landing.read_responses("src", "window_id", "W1") == [b'{"a": 1}', b"{}"]
    assert landing.partitions("src", "window_id") == ["W1"]


def test_failed_landing_keeps_the_previous_partition(tmp_path: Path) -> None:
    landing = LandingZone(tmp_path)
    landing.write_partition("src", "window_id", "W1", {"version": 1}, responses=[b"{}"])

    def fail(_: Path) -> None:
        raise RuntimeError("download interrupted")

    with pytest.raises(RuntimeError):
        landing.write_partition("src", "window_id", "W1", {"version": 2}, populate=fail)
    assert landing.read_manifest("src", "window_id", "W1") == {"version": 1}
    assert not [p for p in (tmp_path / "src").iterdir() if p.name.startswith(".staging")]


def test_one_line_only_touches_whitespace_newlines() -> None:
    assert one_line(b'{\n  "a": "x\\ny"\r\n}') == b'{   "a": "x\\ny"  }'


def tenants(spark: SparkSession, window_id: str, run_id: str, names: list[str]) -> DataFrame:
    df = spark.createDataFrame([(name,) for name in names], "tenant_id STRING")
    return with_control_columns(df, window_id, run_id, datetime.now(UTC), "test")


def test_write_partition_replaces_only_its_partition(spark: SparkSession, tmp_path: Path) -> None:
    lake = Lakehouse(spark, tmp_path)
    lake.write_partition(TABLE, tenants(spark, "W1", "r1", ["A", "B"]), "W1")
    lake.write_partition(TABLE, tenants(spark, "W2", "r1", ["C"]), "W2")
    before = lake.fingerprint(TABLE)

    # Same content under a new run ID: the fingerprint ignores run ID and load time.
    lake.write_partition(TABLE, tenants(spark, "W1", "r2", ["B", "A"]), "W1")
    assert lake.fingerprint(TABLE) == before

    # An empty re-run of W1 clears W1 and leaves W2 alone.
    lake.write_partition(TABLE, tenants(spark, "W1", "r3", []), "W1")
    after = {item["partition"]: item["rows"] for item in lake.fingerprint(TABLE)}
    assert after == {"W2": 1}


def test_fingerprint_detects_a_changed_value(spark: SparkSession, tmp_path: Path) -> None:
    lake = Lakehouse(spark, tmp_path)
    lake.write_partition(TABLE, tenants(spark, "W1", "r1", ["A", "B"]), "W1")
    before = lake.fingerprint(TABLE)
    lake.write_partition(TABLE, tenants(spark, "W1", "r2", ["A", "C"]), "W1")
    assert lake.fingerprint(TABLE)[0]["hash"] != before[0]["hash"]
