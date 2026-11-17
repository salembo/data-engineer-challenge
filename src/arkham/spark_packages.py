"""Maven coordinates of the JARs the Spark session needs beyond PySpark itself.

`make lock-jars` resolves them, with their dependencies, into
docker/pipeline/spark-jars.lock (URL and SHA-256 of each JAR); the Docker build
downloads and verifies exactly those files, so a run never downloads JARs. After
changing this list, run `make lock-jars`, then `make build`: the build fails while the
lock is stale. Delta publishes one artifact per Spark minor version: the ``4.1`` suffix
must match the ``pyspark`` pin in pyproject.toml.

This module has no imports on purpose: the Docker build loads it on its own,
before the rest of the package is copied.
"""

SPARK_PACKAGES: tuple[str, ...] = (
    "io.delta:delta-spark_4.1_2.13:4.4.1",
    "org.postgresql:postgresql:42.7.14",
    # Built against Spark 4.1.2; its Python API is the graphframes-py package (pyproject.toml).
    "io.graphframes:graphframes-spark4_2.13:0.12.3",
)
