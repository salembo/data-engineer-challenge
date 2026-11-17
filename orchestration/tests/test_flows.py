"""The Prefect flows against a fake CLI (fake_cli.py), on a temporary Prefect server.

Run with `make prefect-test` (in the orchestrator image, which has Prefect)."""

from __future__ import annotations

import json
import os
import sys
from collections.abc import Iterator
from pathlib import Path

import pytest

os.environ["ARKHAM_STEP_RETRY_DELAY_SECONDS"] = "0"
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import flows  # noqa: E402
from prefect.testing.utilities import prefect_test_harness  # noqa: E402

FAKE_CLI = Path(__file__).with_name("fake_cli.py")


@pytest.fixture(autouse=True, scope="session")
def prefect_server() -> Iterator[None]:
    with prefect_test_harness():
        yield


@pytest.fixture
def cli(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    monkeypatch.setenv("FAKE_CLI_DIR", str(tmp_path))
    monkeypatch.setenv("ARKHAM_CLI", f"{sys.executable} {FAKE_CLI}")
    return tmp_path


def calls(state: Path) -> list[str]:
    return (state / "calls.log").read_text().splitlines()


def test_pending_runs_the_planned_steps_in_order(cli: Path) -> None:
    assert flows.pending() == ["reference", "run --window W1", "run --window W2"]
    assert calls(cli) == ["plan-pending", "reference", "run --window W1", "run --window W2"]


def test_a_failed_step_is_retried(cli: Path) -> None:
    (cli / "failures.json").write_text(json.dumps({"run --window W2": 1}))
    flows.pending()
    assert calls(cli).count("run --window W2") == 2


def test_a_step_that_keeps_failing_stops_the_flow(cli: Path) -> None:
    (cli / "failures.json").write_text(json.dumps({"run --window W1": -1}))
    with pytest.raises(RuntimeError, match="run --window W1"):
        flows.pending()
    # One attempt plus the retries, and no later window.
    assert calls(cli).count("run --window W1") == 1 + flows.STEP_RETRIES
    assert "run --window W2" not in calls(cli)


def test_verify_rerun_compares_the_tables(cli: Path) -> None:
    (cli / "tables.json").write_text(json.dumps([{"table": "t", "partition": None, "rows": 1}]))
    flows.verify_rerun("W1")
    # The fingerprint calls carry a temporary output path; compare the commands only.
    commands = [call.split(" /")[0] for call in calls(cli)]
    assert commands == ["fingerprint --output", "run --window W1", "fingerprint --output"]


def test_verify_rerun_fails_when_a_table_changes(cli: Path) -> None:
    tables = cli / "tables.json"
    tables.write_text(json.dumps([{"table": "t", "partition": None, "rows": 1}]))
    (cli / "failures.json").write_text("{}")
    original = flows.run_cli

    def changing_cli(args: list[str]) -> tuple[int, list[str]]:
        result = original(args)
        if args[0] == "run":  # the re-run changes a table
            tables.write_text(json.dumps([{"table": "t", "partition": None, "rows": 2}]))
        return result

    flows.run_cli = changing_cli  # type: ignore[assignment]
    try:
        with pytest.raises(RuntimeError, match="changed 1 partition"):
            flows.verify_rerun("W1")
    finally:
        flows.run_cli = original  # type: ignore[assignment]


def test_main_returns_an_error_code_when_the_flow_fails(cli: Path) -> None:
    (cli / "failures.json").write_text(json.dumps({"run --window W9": -1}))
    assert flows.main(["run", "--window", "W9"]) == 1
