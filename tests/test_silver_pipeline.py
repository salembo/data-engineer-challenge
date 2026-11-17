"""Silver end to end with in-memory sources (see tests/fakes.py for the scenario).

After W1, W2 and W3 the fakes deliver 21 partner events: one from the sandbox, a
correction of event 3, event 7 corrected into a negative price, event 5 corrected
out of one, and a revision 2 with no revision 1. So 17 events are current: 15 sales,
1 shelf audit and 1 in quarantine, with 3 superseded revisions.
"""

from __future__ import annotations

import gzip
import shutil
from decimal import Decimal
from pathlib import Path

import pytest
from fakes import FALLBACK, W1, W2, W3, Harness, harness
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from arkham.lakehouse import TABLES
from arkham.runlog import SUCCESS

SALES, AUDITS = "silver.fact_pos_sale", "silver.fact_shelf_audit"
MOVEMENTS, QUARANTINE = "silver.fact_inventory_movement", "platform.quarantine"


def by_key(h: Harness, table: str, key: str) -> dict[str, dict[str, object]]:
    return {row[key]: row.asDict() for row in h.table(table).collect()}


@pytest.fixture(scope="module")
def three_windows(spark: SparkSession, tmp_path_factory: pytest.TempPathFactory) -> Harness:
    h = harness(spark, tmp_path_factory.mktemp("silver"))
    for window_id in (W1, W2, W3):
        assert h.pipeline.run_window(window_id).status == SUCCESS
    return h


def test_dimensions_hold_the_latest_snapshot(three_windows: Harness) -> None:
    h = three_windows
    tenants = by_key(h, "silver.dim_tenant", "tenant_id")
    assert {t: row["is_sandbox"] for t, row in tenants.items()} == {
        "TENANT_A": False,
        "TENANT_B": False,
        "TENANT_SBX": True,
    }
    assert {row["_snapshot_window_id"] for row in tenants.values()} == {W3}

    locations = by_key(h, "silver.dim_location", "location_id")
    assert locations["DC_1"]["parent_exists"] is True
    assert locations["STORE_2"]["parent_exists"] is False  # parent DC_9 does not exist
    assert locations["PLANT_1"]["parent_exists"] is None

    products = by_key(h, "silver.dim_product", "product_id")
    assert {p: row["gtin14"] for p, row in products.items()} == {
        "PROD_1": "03017620422003",
        "PROD_2": "05449000000996",
        "PROD_3": "03017620422003",
    }
    assert products["PROD_1"]["pack_size_units"] == 12
    assert products["PROD_2"]["pack_size_units"] == 12
    assert products["PROD_3"]["pack_size_units"] is None  # object shape: raw value only
    assert products["PROD_2"]["promo_eligible"] is True
    assert products["PROD_2"]["shelf_life_days"] == 180
    assert products["PROD_1"]["primary_supplier_location_id"] == "PLANT_1"
    assert products["PROD_1"]["currency"] is None  # the ERP declares no currency


def test_inventory_movements(three_windows: Harness) -> None:
    movements = by_key(three_windows, MOVEMENTS, "ledger_entry_id")
    assert sorted(movements) == [1, 3, 4]  # entries 2 and 5 belong to the sandbox
    assert movements[1]["is_posted"] and movements[1]["movement_status"] == "POSTED"
    assert movements[3]["movement_status"] == "VOIDED" and not movements[3]["is_posted"]
    assert movements[3]["is_late_arriving"] and not movements[1]["is_late_arriving"]
    # DOC-1 is shared by entries 1 and 4; DOC-3 by entry 3 and a sandbox entry.
    assert {k: v["is_reference_doc_reused"] for k, v in movements.items()} == {
        1: True,
        3: True,
        4: True,
    }
    # The fact's tenant is kept; the owners of the store and product come from the dimensions.
    assert movements[4]["tenant_id"] == "TENANT_B"
    assert movements[4]["location_owner_tenant_id"] == "TENANT_B"
    assert movements[4]["product_owner_tenant_id"] == "TENANT_A"
    assert movements[1]["workflow_run_id"] == "wf_1"


def test_partner_events_follow_the_newest_revision(three_windows: Harness) -> None:
    h = three_windows
    sales = by_key(h, SALES, "event_id")
    audits = by_key(h, AUDITS, "event_id")
    quarantined = by_key(h, QUARANTINE, "record_key")

    assert len(sales) == 15
    assert list(audits) == ["EVT-0000004"]
    assert list(quarantined) == ["EVT-0000007"]
    assert "EVT-0000002" not in sales  # sandbox

    # Event 3: the W2 correction changed the store.
    assert (sales["EVT-0000003"]["revision"], sales["EVT-0000003"]["location_id"]) == (2, "STORE_2")
    # Event 7: the current revision has a negative price, so the whole event is quarantined
    # and revision 1 is not restored.
    assert "EVT-0000007" not in sales
    assert quarantined["EVT-0000007"]["revision"] == 2
    assert "negative unit_price_cents" in quarantined["EVT-0000007"]["reason"]
    # Event 5: quarantined in W1, released when its valid revision 2 arrived in W3.
    assert sales["EVT-0000005"]["revision"] == 2
    assert sales["EVT-0000005"]["unit_price_cents"] == 500
    # A revision 2 with no revision 1 is valid.
    assert sales["EVT-0000099"]["revision"] == 2

    assert sales["EVT-0000006"]["is_cancelled"] is True
    assert sum(1 for row in sales.values() if row["is_cancelled"]) == 1
    assert sales["EVT-0000001"]["discount_rate"] == Decimal("0.1500")
    assert sales["EVT-0000010"]["discount_rate"] == Decimal("0.1000")
    assert sales["EVT-0000001"]["promo_campaign_id"] == "PRM-101"
    assert sales["EVT-0000001"]["promo_co_funded_ratio"] == Decimal("0.3000")
    # The product barcode comes from the master; the event's own is kept for audit.
    assert sales["EVT-0000009"]["product_gtin14"] == "03017620422003"
    assert sales["EVT-0000009"]["event_gtin_raw"] == "003017620422003"
    assert sales["EVT-0000001"]["location_owner_tenant_id"] == "TENANT_B"
    assert audits["EVT-0000004"]["units_observed"] == 2


def test_local_format_dates_land_on_the_same_utc_instant(three_windows: Harness) -> None:
    h = three_windows
    sale = h.table(SALES).filter(F.col("event_id") == "EVT-0000008").first()
    expected = h.partner.occurred_at(8).replace(tzinfo=None)  # collect() returns naive UTC
    assert sale.event_occurred_at == expected
    assert h.table(SALES).filter(F.col("event_occurred_at").isNull()).count() == 0


def test_superseded_and_quarantined_counts_in_the_run_log(three_windows: Harness) -> None:
    metrics = (
        three_windows.table("platform.run_metrics")
        .filter((F.col("table_name") == SALES) & F.col("outcome").isin("superseded", "filtered"))
        .groupBy("outcome")
        .agg(F.sum("rows").alias("rows"))
        .collect()
    )
    # Superseded: event 3 and event 7 replaced in W2, event 5 in W3. Filtered: the sandbox event.
    assert {row.outcome: row.rows for row in metrics} == {"superseded": 3, "filtered": 1}


def test_public_references(three_windows: Harness) -> None:
    h = three_windows
    products = by_key(h, "silver.ref_product_public", "gtin14")
    assert sorted(products) == ["03017620422003", "05449000000996"]
    # Only the API has been loaded: its empty brand_owner falls back to the first brand.
    assert products["03017620422003"]["brand_owner_resolved"] == "Brand A"
    assert products["03017620422003"]["nova_group"] == 4
    assert products["03017620422003"]["source"] == "api"

    prices = by_key(h, "silver.ref_public_price", "id")
    assert sorted(prices) == [4, 5, 6]  # 8 is a duplicate of 4
    assert prices[4]["price_cents"] == 445
    assert prices[4]["location_country_code"] == "FR"
    columns = h.table("silver.ref_public_price").columns
    assert not [c for c in columns if "owner" in c]


def test_tenant_scoped_silver_tables_always_carry_a_tenant(three_windows: Harness) -> None:
    h = three_windows
    sandbox = [r.tenant_id for r in h.table("silver.dim_tenant").filter("is_sandbox").collect()]
    for name, spec in TABLES.items():
        if spec.layer != "silver" or spec.scope != "TENANT":
            continue
        table = h.table(name)
        assert table.filter(F.col("tenant_id").isNull()).count() == 0, name
        if name.startswith("silver.fact_"):
            assert table.filter(F.col("tenant_id").isin(sandbox)).count() == 0, name
            for tenant in ("TENANT_A", "TENANT_B"):
                rows = table.filter(F.col("tenant_id") == tenant)
                assert rows.filter(F.col("tenant_id") != tenant).count() == 0


def test_rerun_and_rebuild_leave_silver_identical(three_windows: Harness) -> None:
    h = three_windows
    before = h.pipeline.fingerprints()
    assert h.pipeline.run_window(W1).status == SUCCESS
    assert h.pipeline.fingerprints() == before
    # A later correction is never reverted by re-running an earlier window.
    assert h.table(SALES).filter(F.col("event_id") == "EVT-0000003").first().revision == 2

    assert h.pipeline.rebuild_silver().status == SUCCESS
    assert h.pipeline.fingerprints() == before
    # A rebuild does not count as a window run.
    assert h.pipeline.pending_windows() == []


def test_after_only_the_first_window(spark: SparkSession, tmp_path: Path) -> None:
    h = harness(spark, tmp_path)
    h.pipeline.run_window(W1)
    assert h.rows(SALES) == 7
    assert list(by_key(h, QUARANTINE, "record_key")) == ["EVT-0000005"]
    # DOC-1 is not shared yet: its second use arrives in W3.
    assert by_key(h, MOVEMENTS, "ledger_entry_id")[1]["is_reference_doc_reused"] is False


def test_reference_load_prefers_the_newer_dump_version(spark: SparkSession, tmp_path: Path) -> None:
    dump = tmp_path / "dump.csv.gz"
    with (
        (FALLBACK / "open_food_facts" / "off_18.tsv").open("rb") as src,
        gzip.open(dump, "wb") as dst,
    ):
        shutil.copyfileobj(src, dst)
    h = harness(spark, tmp_path, dump_file=dump)
    h.pipeline.load_reference()
    h.pipeline.run_window(W1)

    products = by_key(h, "silver.ref_product_public", "gtin14")
    nutella = products["03017620422003"]
    # The real dump row was edited in 2026, after the API fake's 2023 last_modified_t.
    assert nutella["source"] == "dump"
    assert nutella["last_modified_at"].year == 2026
    assert nutella["brands"].startswith("Ferrero")
    # Dump tags are comma-separated text turned into an array; OFF prefixes unknown-language
    # tags with "xx:".
    assert any(tag.endswith("ferrero") for tag in nutella["brands_tags"])
    prices = by_key(h, "silver.ref_public_price", "id")
    assert sorted(prices) == [1, 2, 3, 4]  # baseline plus W1, without the duplicate
