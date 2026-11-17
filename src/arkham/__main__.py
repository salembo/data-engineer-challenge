"""Command line: python -m arkham <command>. See `python -m arkham --help`."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path

from arkham.acceptance import baseline_differences, run_checks, run_v2_checks, run_v3_checks
from arkham.catalog.stage import CATALOG_TABLES, FRESHNESS
from arkham.config import ConfigError, PipelineConfig, load_config
from arkham.erp import JdbcErpSource
from arkham.gold.health import HEALTH
from arkham.gold.stage import GOLD_TABLES
from arkham.graph.stage import GRAPH_TABLES
from arkham.http import HttpClient
from arkham.invariants import Check, run_invariant_checks
from arkham.pipeline import Pipeline, PipelineError, Sources
from arkham.runlog import SUCCESSFUL, RunRecord
from arkham.settings import ErpConnection, partner_api_url_from_env
from arkham.spark import build_spark_session

REPO_ROOT = Path(__file__).resolve().parents[2]
# The exact numbers of the provided dataset, per version (acceptance.py).
BASELINE_CHECKS = {"verify-v1": run_checks, "verify-v2": run_v2_checks, "verify-v3": run_v3_checks}


def _path_from_env(name: str, default: str) -> Path:
    return Path(os.environ.get(name, REPO_ROOT / default))


def build_pipeline(config: PipelineConfig) -> Pipeline:
    spark = build_spark_session("arkham-pipeline")
    http = config.public_http
    sources = Sources(
        erp=JdbcErpSource(spark, ErpConnection.from_env()),
        partner_api=HttpClient(user_agent=http.user_agent, timeout_seconds=http.timeout_seconds),
        partner_api_url=partner_api_url_from_env(),
        public_http=HttpClient(
            user_agent=http.user_agent,
            timeout_seconds=http.timeout_seconds,
            max_retries=http.max_retries,
            min_interval_seconds=http.min_interval_seconds,
        ),
    )
    return Pipeline(
        spark,
        config,
        sources,
        lakehouse_root=_path_from_env("ARKHAM_LAKEHOUSE_ROOT", "lakehouse"),
        landing_root=_path_from_env("ARKHAM_LANDING_ROOT", "landing"),
        fallback_root=_path_from_env("ARKHAM_FALLBACK_ROOT", "fallback"),
    )


def print_run(record: RunRecord) -> None:
    target = record.window_id or "reference data"
    print(f"\n{record.command} {target}: {record.status} (run {record.run_id})")
    for (table, outcome), (rows, detail) in sorted(record.metrics.items()):
        suffix = f"  ({detail})" if detail else ""
        print(f"  {table:<32} {outcome:<11} {rows:>9,}{suffix}")
        for (t, o, tenant), tenant_rows in sorted(record.tenant_metrics.items()):
            if (t, o) == (table, outcome):
                print(f"  {'':<32} {'':<11} {tenant_rows:>9,}  {tenant}")
    for fallback in record.fallback_sources:
        print(f"  fallback: {fallback}")


def print_checks(checks: list[Check]) -> int:
    for check in checks:
        mark = "ok  " if check.passed else "FAIL"
        note = f"  ({check.note})" if check.note else ""
        print(f"{mark}  {check.label:<62} {check.measured} (expected {check.expected}){note}")
    failed = [check for check in checks if not check.passed]
    print(f"\n{len(checks) - len(failed)} of {len(checks)} checks passed.")
    return 1 if failed else 0


def print_not_applicable(differences: list[str]) -> int:
    """The baseline numbers describe only the provided dataset; elsewhere they are skipped,
    not failed."""
    print(
        "Not applicable: these checks compare the lakehouse with the exact numbers of the "
        "provided dataset (W1, W2 and W3 of the legacy sources as delivered), and this "
        "environment differs:"
    )
    for difference in differences:
        print(f"  - {difference}")
    print("Run `make verify` for the checks that hold on any data.")
    return 0


def print_status(pipeline: Pipeline) -> None:
    latest = pipeline.runlog.latest_runs()
    reference = latest.get(None)
    print("Reference data (Open Food Facts dump, Open Prices baseline):")
    print(f"  {reference.status if reference else 'never loaded'}")
    print("\nWindows:")
    for window in pipeline.config.calendar.windows:
        run = latest.get(window.id)
        state = "complete" if run and run.status in SUCCESSFUL else "pending"
        detail = f"last run {run.run_id} {run.status}" if run else "never run"
        print(
            f"  {window.id}  {window.start:%Y-%m-%d %H:%M} -> {window.end:%Y-%m-%d %H:%M} UTC"
            f"  {state:<9} {detail}"
        )
    print("\nRows per table and partition:")
    for item in pipeline.fingerprints():
        partition = item["partition"] or "-"
        print(f"  {item['table']:<34} {partition:<12} {item['rows']:>9,}")


def pending_steps(pipeline: Pipeline) -> list[list[str]]:
    steps: list[list[str]] = []
    if not pipeline.runlog.reference_loaded():
        steps.append(["reference"])
    steps.extend(["run", "--window", window_id] for window_id in pipeline.pending_windows())
    return steps


def run_pending_in_separate_processes() -> int:
    """Loads the reference data if needed, then each pending window in order, every step in
    its own process. One JVM per step keeps the memory of a window from carrying into the
    next one; running them all in one session once ran out of heap on a first load.

    This process starts no JVM itself: `spark.stop()` does not end PySpark's JVM, so an
    idle JVM here would sit next to each step's and double the container's memory. The
    plan comes from a short-lived `plan-pending` process instead.
    """
    plan = subprocess.run(
        [sys.executable, "-m", "arkham", "plan-pending"],
        stdout=subprocess.PIPE,
        text=True,
        check=False,
    )
    if plan.returncode != 0:
        print(plan.stdout)
        return plan.returncode
    steps: list[list[str]] = json.loads(plan.stdout.strip().splitlines()[-1])
    if not steps:
        print("Nothing pending: the reference data and every window are complete.")
        return 0
    for step in steps:
        print(f"\n>>> python -m arkham {' '.join(step)}", flush=True)
        result = subprocess.run([sys.executable, "-m", "arkham", *step], check=False)
        if result.returncode != 0:
            print(f"Stopped: `{' '.join(step)}` failed; later windows were not run.")
            return result.returncode
    return 0


def compare(before: list[dict[str, object]], after: list[dict[str, object]]) -> list[str]:
    def index(items: list[dict[str, object]]) -> dict[tuple[object, object], dict[str, object]]:
        return {(item["table"], item["partition"]): item for item in items}

    old, new = index(before), index(after)
    differences = []
    for key in sorted(set(old) | set(new), key=str):
        if old.get(key) != new.get(key):
            differences.append(f"{key[0]} [{key[1]}]: {old.get(key)} -> {new.get(key)}")
    return differences


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m arkham", description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    run = commands.add_parser("run", help="Run one window")
    run.add_argument("--window", required=True)
    commands.add_parser("run-pending", help="Load reference data if needed, then pending windows")
    reference = commands.add_parser("reference", help="Load the dump and the Open Prices baseline")
    reference.add_argument("--force-download", action="store_true")
    reference.add_argument(
        "--skip-download", action="store_true", help="Use the committed dump fixture"
    )
    commands.add_parser(
        "rebuild-silver", help="Recreate silver and quarantine from bronze, then gold"
    )
    commands.add_parser("rebuild-gold", help="Recompute gold, the graph and the catalog")
    commands.add_parser("status", help="Window state and row counts")
    fingerprint = commands.add_parser("fingerprint", help="Row count and hash per partition")
    fingerprint.add_argument("--output", type=Path)
    verify = commands.add_parser(
        "verify-rerun", help="Run a window again and check that every table is unchanged"
    )
    verify.add_argument("--window", required=True)
    commands.add_parser("verify", help="Check the invariants, which hold on any data")
    commands.add_parser("verify-v1", help="Check the v1 baseline numbers of the provided data")
    commands.add_parser("verify-v2", help="Check the v2 baseline numbers of the provided data")
    commands.add_parser("verify-v3", help="Check the v3 baseline numbers (catalog, graph)")
    commands.add_parser("plan-pending", help=argparse.SUPPRESS)
    args = parser.parse_args(argv)

    # Check the configuration and the requested window before Spark starts: a mistake
    # there gets an explanation and exit code 2, not a stack trace.
    try:
        config = load_config()
        if getattr(args, "window", None) is not None:
            config.calendar.get(args.window)
    except ConfigError as error:
        print(f"error: {error}", file=sys.stderr)
        return 2

    if args.command == "run-pending":
        return run_pending_in_separate_processes()

    pipeline = build_pipeline(config)
    try:
        if args.command == "run":
            print_run(pipeline.run_window(args.window))
        elif args.command == "plan-pending":
            pipeline.lakehouse.clear_scratch()  # left behind by killed runs, if any
            print(json.dumps(pending_steps(pipeline)))
        elif args.command == "reference":
            print_run(pipeline.load_reference(args.force_download, args.skip_download))
        elif args.command == "rebuild-silver":
            print_run(pipeline.rebuild_silver())
        elif args.command == "rebuild-gold":
            print_run(pipeline.rebuild_gold())
        elif args.command == "verify":
            return print_checks(run_invariant_checks(pipeline))
        elif args.command in BASELINE_CHECKS:
            legacy_root = _path_from_env("ARKHAM_LEGACY_ROOT", "legacy_sources")
            differences = baseline_differences(pipeline.config.calendar, legacy_root)
            if differences:
                return print_not_applicable(differences)
            return print_checks(BASELINE_CHECKS[args.command](pipeline))
        elif args.command == "status":
            print_status(pipeline)
        elif args.command == "fingerprint":
            items = pipeline.fingerprints()
            text = json.dumps(items, indent=2)
            if args.output:
                args.output.write_text(text)
            print(text)
        elif args.command == "verify-rerun":
            before = pipeline.fingerprints()
            derived = (*GOLD_TABLES, *GRAPH_TABLES, *CATALOG_TABLES)
            versions = {table: pipeline.lakehouse.version(table) for table in derived}
            print_run(pipeline.run_window(args.window))
            differences = compare(before, pipeline.fingerprints())
            # A re-run with no changes writes nothing to the derived layers, except the run
            # columns and load times of the window that ran in gold.pipeline_health and
            # catalog.freshness (the v2-v4 PRD, writes and re-runs).
            rewritten = {
                table: commits
                for table in derived
                if table not in (HEALTH, FRESHNESS)
                and (commits := pipeline.lakehouse.commits_since(table, versions[table]))
            }
            failed = False
            if differences:
                print(f"\nRe-running {args.window} changed {len(differences)} partition(s):")
                print("\n".join(f"  {line}" for line in differences))
                failed = True
            if rewritten:
                print(f"\nRe-running {args.window} added commits to derived tables: {rewritten}")
                failed = True
            if failed:
                return 1
            print(
                f"\nRe-running {args.window} left every bronze, silver, gold, graph and catalog "
                "table identical, and added no commit to any derived table except "
                "gold.pipeline_health and catalog.freshness."
            )
        return 0
    except (PipelineError, ConfigError) as error:
        print(f"error: {error}", file=sys.stderr)
        return 2
    finally:
        pipeline.spark.stop()


if __name__ == "__main__":
    sys.exit(main())
