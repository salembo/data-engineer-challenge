"""Bronze end to end with in-memory sources: window order, cursor hand-off, re-runs,
interruptions, public-source fallbacks, empty windows and the reference load."""

from __future__ import annotations

import gzip
import json
import shutil
from datetime import UTC, datetime
from pathlib import Path

import pytest
from fakes import FALLBACK, W1, W2, W3, harness, make_config
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from arkham.bronze import partner_events
from arkham.config import Window
from arkham.pipeline import PipelineError
from arkham.runlog import FAILED, RUNNING, SUCCESS, SUCCESS_WITH_FALLBACK, RunRecord


def test_three_windows_then_a_rerun_of_the_first(spark: SparkSession, tmp_path: Path) -> None:
    h = harness(spark, tmp_path)
    for window_id in (W1, W2, W3):
        assert h.pipeline.run_window(window_id).status == SUCCESS

    # Partner events: each window starts at the previous window's end cursor.
    assert [call["since_cursor"] for call in h.partner.calls][:1] == [0]
    ends = [
        h.pipeline.landing.read_manifest(partner_events.SOURCE, "window_id", w)["end_cursor"]
        for w in (W1, W2, W3)
    ]
    assert ends == [10, 16, 21]
    assert [h.rows("bronze.partner_events", w) for w in (W1, W2, W3)] == [10, 6, 5]

    # The ledger is assigned by arrival time; the sandbox enters bronze unfiltered.
    assert [h.rows("bronze.erp_inventory_ledger", w) for w in (W1, W2, W3)] == [2, 1, 2]
    ledger = h.pipeline.lakehouse.read("bronze.erp_inventory_ledger")
    assert ledger.filter(F.col("tenant_id") == "TENANT_SBX").count() == 2

    # Dimensions are a full snapshot per window.
    assert [h.rows("bronze.erp_products", w) for w in (W1, W2, W3)] == [3, 3, 3]

    # Public sources: two distinct ERP barcodes; prices created inside each window
    # (W1 also holds a price marked as a duplicate: bronze keeps it).
    assert [h.rows("bronze.off_product_api", w) for w in (W1, W2, W3)] == [2, 2, 2]
    assert [h.rows("bronze.open_prices", w) for w in (W1, W2, W3)] == [2, 1, 1]

    # raw_json keeps the event exactly as delivered.
    event = (
        h.pipeline.lakehouse.read("bronze.partner_events")
        .filter(F.col("sequence_cursor") == 3)
        .first()
    )
    assert json.loads(event.raw_json) == h.partner.event(3)
    assert event.event_id == "EVT-0000003"

    before = h.pipeline.fingerprints()
    partner_calls, public_calls = len(h.partner.calls), len(h.public.calls)

    rerun = h.pipeline.run_window(W1)

    assert rerun.status == SUCCESS
    assert h.pipeline.fingerprints() == before
    assert (len(h.partner.calls), len(h.public.calls)) == (partner_calls, public_calls)


def test_windows_must_run_in_order(spark: SparkSession, tmp_path: Path) -> None:
    h = harness(spark, tmp_path)
    with pytest.raises(PipelineError, match="2026-10-W1"):
        h.pipeline.run_window(W2)
    assert not h.pipeline.lakehouse.exists("bronze.erp_tenants")


def test_interrupted_run_leaves_the_window_incomplete_until_rerun(
    spark: SparkSession, tmp_path: Path
) -> None:
    h = harness(spark, tmp_path / "interrupted")
    h.partner.fail = True
    with pytest.raises(RuntimeError, match="partner API unavailable"):
        h.pipeline.run_window(W1)

    assert h.pipeline.runlog.latest_runs()[W1].status == FAILED
    assert h.pipeline.pending_windows() == [W1, W2, W3]
    with pytest.raises(PipelineError):
        h.pipeline.run_window(W2)

    h.partner.fail = False
    assert h.pipeline.run_window(W1).status == SUCCESS
    assert h.pipeline.pending_windows() == [W2, W3]

    clean = harness(spark, tmp_path / "clean")
    clean.pipeline.run_window(W1)
    assert h.pipeline.fingerprints() == clean.pipeline.fingerprints()


def test_a_killed_run_stays_running_and_the_window_incomplete(
    spark: SparkSession, tmp_path: Path
) -> None:
    h = harness(spark, tmp_path)
    record = RunRecord("killed-run", "run", W1, datetime.now(UTC))
    h.pipeline.runlog.start(record)  # the process dies before finish()
    assert h.pipeline.runlog.latest_runs()[W1].status == RUNNING
    assert not h.pipeline.runlog.is_complete(W1)


def test_public_source_failure_uses_the_committed_fixture(
    spark: SparkSession, tmp_path: Path
) -> None:
    h = harness(spark, tmp_path)
    h.public.fail = True
    record = h.pipeline.run_window(W1)

    assert record.status == SUCCESS_WITH_FALLBACK
    assert any("off_product_api" in f and "committed fixture" in f for f in record.fallback_sources)
    assert any("open_prices" in f and "committed fixture" in f for f in record.fallback_sources)
    fixture_products = (FALLBACK / "off_product_api" / "responses.jsonl").read_text().splitlines()
    assert h.rows("bronze.off_product_api", W1) == len(fixture_products)
    fixture_prices = json.loads((FALLBACK / "open_prices" / "prices.json").read_text())["items"]
    window = make_config().calendar.get(W1)
    expected = [p for p in fixture_prices if window.contains(datetime.fromisoformat(p["created"]))]
    assert h.rows("bronze.open_prices", W1) == len(expected)

    # The fallback was landed, so a re-run is identical and still reports the fallback.
    before = h.pipeline.fingerprints()
    assert h.pipeline.run_window(W1).status == SUCCESS_WITH_FALLBACK
    assert h.pipeline.fingerprints() == before


def test_a_window_without_data_succeeds_with_zero_rows(spark: SparkSession, tmp_path: Path) -> None:
    # Before every ledger row, partner event and windowed price of the fakes.
    september = make_config(
        [Window("2026-09-W1", datetime(2026, 9, 1, tzinfo=UTC), datetime(2026, 9, 6, tzinfo=UTC))]
    )
    h = harness(spark, tmp_path, config=september)
    record = h.pipeline.run_window("2026-09-W1")

    assert record.status == SUCCESS
    assert record.metrics[("bronze.erp_inventory_ledger", "written")][0] == 0
    assert record.metrics[("bronze.partner_events", "written")][0] == 0
    assert record.metrics[("bronze.open_prices", "written")][0] == 0
    manifest = h.pipeline.landing.read_manifest(partner_events.SOURCE, "window_id", "2026-09-W1")
    assert (manifest["records"], manifest["end_cursor"]) == (0, 0)


def gzipped_fixture(tmp_path: Path) -> Path:
    path = tmp_path / "dump.csv.gz"
    with (
        (FALLBACK / "open_food_facts" / "off_18.tsv").open("rb") as src,
        gzip.open(path, "wb") as dst,
    ):
        shutil.copyfileobj(src, dst)
    return path


def test_reference_load_keeps_only_erp_barcodes_from_the_dump(
    spark: SparkSession, tmp_path: Path
) -> None:
    h = harness(spark, tmp_path, dump_file=gzipped_fixture(tmp_path))
    record = h.pipeline.load_reference()

    assert record.status == SUCCESS
    assert record.metrics[("bronze.off_products", "read")][0] == 18
    assert record.metrics[("bronze.off_products", "written")][0] == 2
    assert record.metrics[("bronze.off_products", "filtered")][0] == 16
    products = h.pipeline.lakehouse.read("bronze.off_products")
    assert sorted(r.code for r in products.collect()) == ["3017620422003", "5449000000996"]
    assert {f.name: f.dataType.simpleString() for f in products.schema.fields}["code"] == "string"
    assert str(products.first()._snapshot_date) == "2026-11-14"
    # Baseline prices are dated in the six months before the first window.
    assert h.rows("bronze.open_prices", "baseline") == 3

    # A second load reuses the landed dump instead of downloading it again.
    h.pipeline.load_reference()
    assert h.public.downloads == 1


def test_reference_load_can_skip_the_download(spark: SparkSession, tmp_path: Path) -> None:
    h = harness(spark, tmp_path)
    record = h.pipeline.load_reference(skip_download=True)
    assert record.status == SUCCESS_WITH_FALLBACK
    assert record.metrics[("bronze.off_products", "written")][0] == 2
    assert h.public.downloads == 0
