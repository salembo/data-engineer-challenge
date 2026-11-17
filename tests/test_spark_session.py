from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest
from delta.tables import DeltaTable
from pyspark.sql import SparkSession

from arkham.spark import jar_config
from arkham.spark_packages import SPARK_PACKAGES


def test_session_and_jvm_time_zone_are_utc(spark: SparkSession) -> None:
    jvm = spark.sparkContext._jvm
    assert spark.conf.get("spark.sql.session.timeZone") == "UTC"
    assert jvm.java.util.TimeZone.getDefault().getID() == "UTC"


def test_timestamp_without_offset_is_read_as_utc(spark: SparkSession) -> None:
    epoch = spark.sql("SELECT unix_timestamp(TIMESTAMP '2026-10-01 00:00:00') AS s").first().s
    assert epoch == int(datetime(2026, 10, 1, tzinfo=UTC).timestamp())


def test_delta_merge_rerun_leaves_the_same_rows(spark: SparkSession, tmp_path: Path) -> None:
    path = str(tmp_path / "target")
    spark.createDataFrame([(1, "a"), (2, "b")], "id INT, value STRING").write.format("delta").save(
        path
    )
    updates = spark.createDataFrame([(2, "b2"), (3, "c")], "id INT, value STRING")
    target = DeltaTable.forPath(spark, path)

    def merge() -> list[tuple[int, str]]:
        (
            target.alias("t")
            .merge(updates.alias("u"), "t.id = u.id")
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
        return sorted(tuple(row) for row in spark.read.format("delta").load(path).collect())

    first = merge()
    assert first == [(1, "a"), (2, "b2"), (3, "c")]
    assert merge() == first
    assert target.history().count() == 3


def test_jar_config_uses_local_jars_when_a_directory_is_given(tmp_path: Path) -> None:
    (tmp_path / "b.jar").touch()
    (tmp_path / "a.jar").touch()
    key, value = jar_config(tmp_path)
    assert key == "spark.jars"
    assert value == f"{tmp_path / 'a.jar'},{tmp_path / 'b.jar'}"


def test_jar_config_rejects_an_empty_directory(tmp_path: Path) -> None:
    with pytest.raises(RuntimeError, match="No JARs found"):
        jar_config(tmp_path)


def test_jar_config_downloads_packages_without_a_directory() -> None:
    assert jar_config(None) == ("spark.jars.packages", ",".join(SPARK_PACKAGES))
