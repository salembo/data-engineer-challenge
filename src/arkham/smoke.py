"""End-to-end check of the pipeline runtime against the running source services.

Verifies that the image can do what the pipeline needs, without writing to the
lakehouse: Spark with Delta in UTC, JDBC reads from the ERP as the read-only
role, a full-size ledger read written to Delta within the memory limit, and
access to the partner API.

Usage (inside the pipeline container): python -m arkham.smoke
"""

from __future__ import annotations

import json
import sys
import tempfile
import time
import urllib.request
from collections.abc import Callable
from pathlib import Path

from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from arkham.erp import read_erp_query
from arkham.settings import ErpConnection, partner_api_url_from_env
from arkham.spark import build_spark_session

CGROUP = Path("/sys/fs/cgroup")


class SmokeCheck:
    def __init__(self) -> None:
        self.failures = 0

    def check(self, label: str, passed: bool, detail: object) -> None:
        status = "ok  " if passed else "FAIL"
        print(f"{status}  {label:<52} {detail}", flush=True)
        if not passed:
            self.failures += 1

    def run(self, label: str, step: Callable[[], None]) -> None:
        print(f"== {label}", flush=True)
        try:
            step()
        except Exception as error:  # noqa: BLE001 - report every failure and keep checking
            self.check(label, False, f"{type(error).__name__}: {error}")


def check_runtime(smoke: SmokeCheck, spark: SparkSession) -> None:
    jvm = spark.sparkContext._jvm
    smoke.check("Spark version", spark.version.startswith("4.1."), spark.version)
    smoke.check("Java version", True, jvm.System.getProperty("java.version"))
    smoke.check(
        "Spark session time zone",
        spark.conf.get("spark.sql.session.timeZone") == "UTC",
        spark.conf.get("spark.sql.session.timeZone"),
    )
    jvm_zone = jvm.java.util.TimeZone.getDefault().getID()
    smoke.check("JVM default time zone", jvm_zone == "UTC", jvm_zone)


def check_erp(smoke: SmokeCheck, spark: SparkSession, connection: ErpConnection) -> None:
    # Spark's rendering of each timestamptz must equal Postgres's own UTC rendering.
    tenants = read_erp_query(
        spark,
        connection,
        "SELECT created_at, "
        "TO_CHAR(created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS') AS created_at_utc_text "
        "FROM arkham_erp.tenants",
    )
    rows = tenants.count()
    mismatches = tenants.filter(
        F.date_format("created_at", "yyyy-MM-dd HH:mm:ss") != F.col("created_at_utc_text")
    ).count()
    smoke.check("ERP tenants readable as the read-only role", rows > 0, f"{rows} rows")
    smoke.check(
        "timestamptz read as the same UTC instant", mismatches == 0, f"{mismatches} mismatches"
    )

    read_only = read_erp_query(
        spark, connection, "SELECT current_setting('transaction_read_only') AS read_only"
    ).first()
    smoke.check("JDBC connection is read-only", read_only.read_only == "on", read_only.read_only)


def check_delta(smoke: SmokeCheck, spark: SparkSession, connection: ErpConnection) -> None:
    with tempfile.TemporaryDirectory(prefix="arkham-smoke-") as scratch:
        # The ledger is the largest legacy table: reading all of it, JSON included,
        # and writing it to Delta exercises the memory budget of the container.
        expected = (
            read_erp_query(
                spark, connection, "SELECT COUNT(*) AS n FROM arkham_erp.inventory_ledger"
            )
            .first()
            .n
        )
        ledger_path = f"{scratch}/ledger"
        started = time.monotonic()
        read_erp_query(spark, connection, "SELECT * FROM arkham_erp.inventory_ledger").write.format(
            "delta"
        ).save(ledger_path)
        written = spark.read.format("delta").load(ledger_path).count()
        smoke.check(
            "full ledger written to Delta",
            written == expected,
            f"{written} of {expected} rows in {time.monotonic() - started:.0f}s",
        )

        # MERGE twice with the same input: the second run must change nothing.
        target_path = f"{scratch}/merge_target"
        spark.createDataFrame([(1, "a"), (2, "b")], "id INT, value STRING").write.format(
            "delta"
        ).save(target_path)
        updates = spark.createDataFrame([(2, "b2"), (3, "c")], "id INT, value STRING")
        target = DeltaTable.forPath(spark, target_path)
        for _ in range(2):
            (
                target.alias("t")
                .merge(updates.alias("u"), "t.id = u.id")
                .whenMatchedUpdateAll()
                .whenNotMatchedInsertAll()
                .execute()
            )
        rows = sorted(tuple(row) for row in spark.read.format("delta").load(target_path).collect())
        smoke.check("Delta MERGE is idempotent", rows == [(1, "a"), (2, "b2"), (3, "c")], f"{rows}")
        versions = target.history().count()
        smoke.check("Delta history records every commit", versions == 3, f"{versions} versions")


def check_partner_api(smoke: SmokeCheck, base_url: str) -> None:
    with urllib.request.urlopen(f"{base_url}/health", timeout=5) as response:
        status = json.load(response)["status"]
    smoke.check("partner API health", status == "healthy", status)


def report_memory(smoke: SmokeCheck) -> None:
    peak_file, limit_file, events_file = (
        CGROUP / "memory.peak",
        CGROUP / "memory.max",
        CGROUP / "memory.events",
    )
    if not peak_file.exists():
        smoke.check("container memory", True, "cgroup v2 counters not available")
        return
    mib = 1024 * 1024
    peak = int(peak_file.read_text()) // mib
    limit = limit_file.read_text().strip()
    limit_text = "no limit" if limit == "max" else f"{int(limit) // mib} MiB"
    events = dict(line.split() for line in events_file.read_text().splitlines())
    smoke.check("container memory peak (includes file cache)", True, f"{peak} MiB of {limit_text}")
    smoke.check("no OOM kills", events.get("oom_kill") == "0", f"oom_kill={events.get('oom_kill')}")


def main() -> int:
    smoke = SmokeCheck()
    connection = ErpConnection.from_env()
    spark = build_spark_session("arkham-smoke")
    try:
        smoke.run("Runtime", lambda: check_runtime(smoke, spark))
        smoke.run("ERP over JDBC", lambda: check_erp(smoke, spark, connection))
        smoke.run("Delta Lake", lambda: check_delta(smoke, spark, connection))
        smoke.run("Partner API", lambda: check_partner_api(smoke, partner_api_url_from_env()))
    finally:
        spark.stop()
    smoke.run("Memory", lambda: report_memory(smoke))

    print()
    if smoke.failures:
        print(f"Smoke check FAILED: {smoke.failures} check(s) did not pass.")
        return 1
    print("Smoke check passed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
