"""Spark session factory: Delta Lake, UTC everywhere and memory sized for the container."""

from __future__ import annotations

import re
from pathlib import Path

from pyspark.sql import SparkSession

from arkham.settings import SparkSettings
from arkham.spark_packages import SPARK_PACKAGES

# Applied to the driver JVM (local mode runs everything in it):
# - UTC as the JVM default, so JDBC and timestamp conversions never use a local zone;
# - fail fast on OutOfMemoryError instead of hanging under the container limit;
# - bound the JVM memory that lives outside the heap set by spark.driver.memory:
#   metaspace, the JIT code cache and direct buffers (which default to the heap size).
JVM_OPTIONS = " ".join(
    [
        "-Duser.timezone=UTC",
        "-XX:+ExitOnOutOfMemoryError",
        "-XX:MaxMetaspaceSize=256m",
        "-XX:ReservedCodeCacheSize=128m",
        "-XX:MaxDirectMemorySize=256m",
    ]
)


def jar_config(jars_dir: Path | None) -> tuple[str, str]:
    """Returns the Spark setting that puts Delta and the JDBC driver on the classpath.

    With a directory of JARs resolved at build time (the Docker image), the run is
    offline. Without one (a local run), Spark downloads the same coordinates.
    """
    if jars_dir is None:
        return "spark.jars.packages", ",".join(SPARK_PACKAGES)
    jars = sorted(str(path) for path in jars_dir.glob("*.jar"))
    if not jars:
        raise RuntimeError(f"No JARs found in {jars_dir}; rebuild the pipeline image.")
    return "spark.jars", ",".join(jars)


def jvm_options(master: str) -> str:
    """JVM_OPTIONS, plus the processor count the JVM sizes itself by.

    Without it the JVM sees every host core (12 on a laptop) and starts GC, JIT and pool
    threads for all of them, although Spark only uses the cores of `local[N]`.
    """
    match = re.fullmatch(r"local\[(\d+)\]", master)
    return f"{JVM_OPTIONS} -XX:ActiveProcessorCount={match.group(1)}" if match else JVM_OPTIONS


def build_spark_session(app_name: str, settings: SparkSettings | None = None) -> SparkSession:
    settings = settings or SparkSettings.from_env()
    jar_key, jar_value = jar_config(settings.jars_dir)
    java_options = jvm_options(settings.master)
    spark = (
        SparkSession.builder.appName(app_name)
        .master(settings.master)
        .config("spark.driver.memory", settings.driver_memory)
        .config("spark.driver.extraJavaOptions", java_options)
        .config("spark.executor.extraJavaOptions", java_options)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", str(settings.shuffle_partitions))
        # Delta rebuilds table state with 50 tasks by default, far too many for these tables.
        .config("spark.databricks.delta.snapshotPartitions", str(settings.shuffle_partitions))
        .config("spark.ui.enabled", "false")
        .config("spark.ui.showConsoleProgress", "false")
        # The driver keeps a record of past queries and jobs in its heap even without the UI;
        # a run issues hundreds of small queries, so keep only the most recent ones.
        .config("spark.sql.ui.retainedExecutions", "20")
        .config("spark.ui.retainedJobs", "50")
        .config("spark.ui.retainedStages", "50")
        # Spark keeps one cached thread per concurrent exchange or query stage, up to 1,024
        # per pool. A run issues hundreds of small queries back to back, which grew the JVM
        # past 1,100 threads and ~600 MB of native memory. The pipeline runs its queries one
        # after another, so a few threads per pool are enough.
        .config("spark.sql.shuffleExchange.maxThreadThreshold", "8")
        .config("spark.sql.broadcastExchange.maxThreadThreshold", "8")
        .config("spark.sql.resultQueryStage.maxThreadThreshold", "8")
        .config("spark.sql.subquery.maxThreadThreshold", "4")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
        .config(jar_key, jar_value)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark
