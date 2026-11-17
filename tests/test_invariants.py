"""The two kinds of acceptance checks: invariants, which hold on any data
(`make verify`), and the baseline, the exact numbers of the provided dataset
(`make verify-v1/v2/v3`), which applies only to that dataset."""

from __future__ import annotations

import shutil
from collections.abc import Callable
from datetime import timedelta
from pathlib import Path

import pytest
from fakes import Harness
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from arkham.acceptance import baseline_differences
from arkham.config import DEFAULT_CONFIG_PATH, Window, WindowCalendar, load_config
from arkham.invariants import run_invariant_checks

Derived = tuple[Harness, dict]
LEGACY_ROOT = Path(__file__).resolve().parents[1] / "legacy_sources"


def failing(h: Harness) -> list[str]:
    return [check.label for check in run_invariant_checks(h.pipeline) if not check.passed]


def tamper(
    monkeypatch: pytest.MonkeyPatch,
    h: Harness,
    table: str,
    change: Callable[[DataFrame], DataFrame],
) -> None:
    """Reads of `table` return a changed copy for the rest of the test; nothing is written."""
    lake = h.pipeline.lakehouse
    original = lake.read
    monkeypatch.setattr(
        lake, "read", lambda name: change(original(name)) if name == table else original(name)
    )


def test_invariants_hold_after_the_three_windows(derived: Derived) -> None:
    assert failing(derived[0]) == []


def test_invariants_catch_an_unexplained_row(
    derived: Derived, monkeypatch: pytest.MonkeyPatch
) -> None:
    h = derived[0]
    tamper(
        monkeypatch,
        h,
        "gold.reconciliation",
        lambda df: df.withColumn("unexplained_rows", F.lit(1).cast("bigint")),
    )
    assert "reconciliation rows with unexplained_rows <> 0" in failing(h)


def test_invariants_catch_a_dangling_edge_and_a_sale_out_of_place(
    derived: Derived, monkeypatch: pytest.MonkeyPatch
) -> None:
    h = derived[0]
    tamper(
        monkeypatch,
        h,
        "graph.edges",
        lambda df: df.unionByName(
            df.limit(1).withColumn("dst_id", F.lit("NOT_A_NODE")), allowMissingColumns=False
        ),
    )
    tamper(
        monkeypatch,
        h,
        "gold.pos_sales_daily",
        lambda df: df.withColumn("sale_lines", F.col("sale_lines") + 1),
    )
    found = failing(h)
    assert "edge endpoints that are not nodes" in found
    assert "pos_sales_daily rows that differ from silver's active lines" in found
    assert "stores and currencies whose promotion totals differ from their sales" in found


def test_the_baseline_applies_to_the_provided_dataset() -> None:
    calendar = load_config(DEFAULT_CONFIG_PATH).calendar
    assert baseline_differences(calendar, LEGACY_ROOT) == []


def test_the_baseline_does_not_apply_to_another_calendar() -> None:
    calendar = load_config(DEFAULT_CONFIG_PATH).calendar
    last = calendar.windows[-1]
    longer = WindowCalendar(
        [*calendar.windows, Window("2026-10-W4", last.end, last.end + timedelta(days=5))]
    )
    [reason] = baseline_differences(longer, LEGACY_ROOT)
    assert "window calendar" in reason


def test_the_baseline_does_not_apply_to_other_sources(tmp_path: Path) -> None:
    calendar = load_config(DEFAULT_CONFIG_PATH).calendar
    copy = tmp_path / "legacy_sources"
    shutil.copytree(LEGACY_ROOT, copy)
    with (copy / "partner_api" / "main.py").open("a") as handle:
        handle.write("\n# more events\n")
    assert baseline_differences(calendar, copy) == [
        "legacy_sources/partner_api/main.py is not the version provided with the assessment"
    ]
    [reason, _] = baseline_differences(calendar, tmp_path / "missing")
    assert reason.startswith("legacy_sources/erp_db/init_erp.sql cannot be read")
