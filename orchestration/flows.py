"""Prefect layer (v4, optional): flows that wrap the arkham CLI.

The flows add retries and a run view (the Prefect UI) on top of the CLI, and change
nothing else: every step runs as `python -m arkham <command>` in its own process (one JVM
per step, like `make run-pending`), with the pipeline's own environment. This process
only imports Prefect, never Spark, so its memory stays small next to the step's JVM.

Every step is safe to retry: a window run, a re-run and the reference load all leave
the tables identical when repeated (docs/prd.md and the v2-v4 PRD).

Usage (in the orchestrator container, see `make prefect-pending`):
    python orchestration/flows.py pending
    python orchestration/flows.py run --window 2026-10-W1
    python orchestration/flows.py verify-rerun --window 2026-10-W1
"""

from __future__ import annotations

import argparse
import json
import os
import shlex
import subprocess
import sys
import tempfile
from pathlib import Path

from prefect import flow, get_run_logger, task

# The pipeline's interpreter and CLI; ARKHAM_CLI overrides it (the tests use a stub).
DEFAULT_CLI = "/opt/venv/bin/python -m arkham"
STEP_RETRIES = 2
# Seconds between attempts of a step (the tests shorten it).
STEP_RETRY_DELAY_SECONDS = int(os.environ.get("ARKHAM_STEP_RETRY_DELAY_SECONDS", "30"))


def cli_command() -> list[str]:
    return shlex.split(os.environ.get("ARKHAM_CLI", DEFAULT_CLI))


def run_cli(args: list[str]) -> tuple[int, list[str]]:
    """Runs one CLI command, streams its output to the run's log, and returns the exit
    code and the output lines."""
    logger = get_run_logger()
    command = [*cli_command(), *args]
    logger.info("$ %s", " ".join(command))
    lines: list[str] = []
    with subprocess.Popen(
        command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1
    ) as process:
        assert process.stdout is not None
        for line in process.stdout:
            line = line.rstrip("\n")
            lines.append(line)
            # Spark's own logging is noise in the run view; the CLI's lines are not.
            if " WARN " not in line and " INFO " not in line:
                logger.info(line)
    return process.returncode, lines


def parse_plan(lines: list[str]) -> list[list[str]]:
    """The steps `plan-pending` prints as JSON on its last line."""
    for line in reversed(lines):
        if line.startswith("["):
            return json.loads(line)
    raise RuntimeError("plan-pending printed no plan")


@task(name="plan pending steps", retries=STEP_RETRIES, retry_delay_seconds=STEP_RETRY_DELAY_SECONDS)
def plan_pending() -> list[list[str]]:
    code, lines = run_cli(["plan-pending"])
    if code != 0:
        raise RuntimeError(f"plan-pending failed with exit code {code}")
    return parse_plan(lines)


@task(
    name="arkham step",
    task_run_name="{label}",
    retries=STEP_RETRIES,
    retry_delay_seconds=STEP_RETRY_DELAY_SECONDS,
)
def run_step(step: list[str], label: str) -> None:
    """One CLI step. A failure raises, so Prefect retries it; the step is idempotent."""
    code, _ = run_cli(step)
    if code != 0:
        raise RuntimeError(f"`{' '.join(step)}` failed with exit code {code}")


@task(name="fingerprint", retries=STEP_RETRIES, retry_delay_seconds=STEP_RETRY_DELAY_SECONDS)
def fingerprint() -> list[dict[str, object]]:
    """Row count and content hash per table and partition (`python -m arkham fingerprint`)."""
    with tempfile.TemporaryDirectory(prefix="arkham-fingerprint-") as directory:
        output = Path(directory) / "fingerprint.json"
        code, _ = run_cli(["fingerprint", "--output", str(output)])
        if code != 0:
            raise RuntimeError(f"fingerprint failed with exit code {code}")
        return json.loads(output.read_text())


@flow(name="arkham-pending")
def pending() -> list[str]:
    """Loads the reference data if it never succeeded, then every pending window in
    order; stops at the first step that still fails after its retries."""
    steps = plan_pending()
    labels = [" ".join(step) for step in steps]
    for step, label in zip(steps, labels, strict=True):
        run_step(step, label)
    if not steps:
        get_run_logger().info("Nothing pending: the reference data and every window are complete.")
    return labels


@flow(name="arkham-window")
def window(window_id: str) -> None:
    """Runs (or re-runs) one window."""
    run_step(["run", "--window", window_id], f"run {window_id}")


@flow(name="arkham-verify-rerun")
def verify_rerun(window_id: str) -> None:
    """Re-runs a window through the flow and checks that every table is unchanged: the
    same window gives the same tables through the flow as through the CLI."""
    before = fingerprint()
    run_step(["run", "--window", window_id], f"run {window_id}")
    after = fingerprint()

    def index(items: list[dict[str, object]]) -> dict[tuple[object, object], object]:
        return {(item["table"], item["partition"]): item for item in items}

    old, new = index(before), index(after)
    changed = sorted(str(key) for key in set(old) | set(new) if old.get(key) != new.get(key))
    if changed:
        raise RuntimeError(f"Re-running {window_id} changed {len(changed)} partition(s): {changed}")
    get_run_logger().info(
        "Re-running %s through the flow left all %d table partitions identical.",
        window_id,
        len(old),
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="flows.py", description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("pending", help="Reference data if needed, then pending windows")
    for name in ("run", "verify-rerun"):
        command = commands.add_parser(name)
        command.add_argument("--window", required=True)
    args = parser.parse_args(argv)
    try:
        if args.command == "pending":
            pending()
        elif args.command == "run":
            window(args.window)
        else:
            verify_rerun(args.window)
    except Exception as error:  # noqa: BLE001 - the flow run already holds the details
        print(f"error: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
