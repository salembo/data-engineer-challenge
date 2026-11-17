from __future__ import annotations

from collections.abc import Iterator

import pytest
from fakes import W1, W2, W3, Harness, harness
from pyspark.sql import SparkSession

from arkham.runlog import SUCCESS
from arkham.settings import SparkSettings
from arkham.spark import build_spark_session


@pytest.fixture(scope="session")
def spark() -> Iterator[SparkSession]:
    """One small local session for the whole test run. Uses the JARs baked into the
    image when ARKHAM_SPARK_JARS_DIR is set, so tests never reach the network. The heap
    matches the pipeline's (1 GB): the whole suite runs in this one JVM."""
    configured = SparkSettings.from_env()
    session = build_spark_session(
        "arkham-tests",
        SparkSettings(
            master="local[1]",
            driver_memory="1g",
            shuffle_partitions=1,
            jars_dir=configured.jars_dir,
        ),
    )
    yield session
    session.stop()


@pytest.fixture(autouse=True, scope="module")
def _release_cached_tables(spark: SparkSession) -> Iterator[None]:
    """Each test module writes its own lakehouse; drop Delta's cached logs and Spark's
    cached data after it, so they do not pile up in the shared JVM."""
    yield
    spark.catalog.clearCache()
    spark.sparkContext._jvm.org.apache.spark.sql.delta.DeltaLog.clearCache()


@pytest.fixture(scope="session")
def derived(
    spark: SparkSession, tmp_path_factory: pytest.TempPathFactory
) -> tuple[Harness, dict[tuple[object, ...], int]]:
    """W1, W2 and W3 through every layer, shared by the gold, graph and catalog tests.
    Also returns the sale lines per pos_sales_daily key as they were after W1."""
    h = harness(spark, tmp_path_factory.mktemp("derived"))
    assert h.pipeline.run_window(W1).status == SUCCESS
    after_w1 = {
        (row.tenant_id, row.location_id, row.product_id, row.currency, row.sale_date): (
            row.sale_lines
        )
        for row in h.table("gold.pos_sales_daily").collect()
    }
    for window_id in (W2, W3):
        assert h.pipeline.run_window(window_id).status == SUCCESS
    return h, after_w1
