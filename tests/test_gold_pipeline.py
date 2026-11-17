"""Gold end to end with in-memory sources (see tests/fakes.py and test_silver_pipeline.py).

After W1, W2 and W3 silver holds 15 sales (14 active), 1 shelf audit, 1 quarantined event
and 3 superseded revisions, plus 3 real ledger entries (2 posted). Every active sale is
2 units in USD; odd deliveries carry a 15% discount and promotion PRM-101 (30% co-funded),
even ones a 10% discount and no promotion; EVT-5's current revision costs 500 cents.
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest
from fakes import W1, W2, W3, Harness
from pyspark.sql import functions as F

from arkham.catalog.stage import CATALOG_TABLES, FRESHNESS
from arkham.gold.health import HEALTH
from arkham.gold.stage import GOLD_TABLES
from arkham.graph.stage import GRAPH_TABLES
from arkham.runlog import SUCCESS

POSITION, DAILY = "gold.inventory_position", "gold.inventory_daily"
SALES_DAILY, PROMO = "gold.pos_sales_daily", "gold.pos_promo_performance"
RECONCILIATION = "gold.reconciliation"
SALE_KEY = ["tenant_id", "location_id", "product_id", "currency", "sale_date"]


def sale_lines_by_key(h: Harness) -> dict[tuple[object, ...], int]:
    return {
        tuple(row[k] for k in SALE_KEY): row.sale_lines for row in h.table(SALES_DAILY).collect()
    }


@pytest.fixture(scope="module")
def gold(derived: tuple[Harness, dict]) -> tuple[Harness, dict]:
    """The three windows of conftest.derived (gold, graph and catalog included)."""
    return derived


def test_inventory_position_and_daily_velocity(gold: tuple[Harness, dict]) -> None:
    h, _ = gold
    positions = {
        (row.tenant_id, row.location_id, row.product_id): (
            row.category_code,
            row.inbound_units,
            row.outbound_units,
            row.net_position_units,
            row.operating_status,
        )
        for row in h.table(POSITION).collect()
    }
    # Entry 3 is voided and the sandbox's entries 2 and 5 never reach gold.
    assert positions == {
        ("TENANT_A", "STORE_1", "PROD_1"): ("SNACKS", 10, 0, 10, "OPEN"),
        ("TENANT_B", "STORE_1", "PROD_3"): ("SNACKS", 0, 1, -1, "OPEN"),
    }
    daily = {
        (row.tenant_id, row.movement_date): (row.period_id, row.velocity_units_per_day)
        for row in h.table(DAILY).collect()
    }
    assert daily == {
        ("TENANT_A", date(2026, 10, 1)): (W1, Decimal("0.0000")),
        ("TENANT_B", date(2026, 10, 12)): (W3, Decimal("0.2000")),  # 1 unit over 5 days
    }


def test_sales_totals_and_promotions(gold: tuple[Harness, dict]) -> None:
    h, _ = gold
    totals = h.table(SALES_DAILY).agg(
        *(
            F.sum(c).alias(c)
            for c in (
                "sale_lines",
                "units_sold",
                "gross_revenue_cents",
                "net_revenue_cents",
                "discount_cents",
                "co_funded_discount_cents",
            )
        )
    )
    assert tuple(totals.first()) == (14, 28, 8_800, 7_710, 1_090, 189)
    assert {row.price_match_level for row in h.table(SALES_DAILY).collect()} == {"NONE"}
    nutella = h.table(SALES_DAILY).filter(F.col("product_id") == "PROD_1").first()
    assert (nutella.brand_owner, nutella.nova_group) == ("Brand A", 4)

    promo = {
        (row.location_id, row.promo_campaign_id, row.promo_mechanic_type): (
            row.sale_lines,
            row.avg_discount_rate,
        )
        for row in h.table(PROMO).collect()
    }
    assert promo == {
        ("STORE_1", "PRM-101", "LOYALTY_TIER"): (7, Decimal("0.1500")),
        ("STORE_1", "NONE", "NONE"): (6, Decimal("0.1000")),
        ("STORE_2", "NONE", "NONE"): (1, Decimal("0.1000")),  # EVT-3, moved by its revision
    }


def test_a_superseded_sale_leaves_its_old_date(gold: tuple[Harness, dict]) -> None:
    h, after_w1 = gold
    old = ("TENANT_A", "STORE_1", "PROD_1", "USD", date(2026, 10, 1))
    new = ("TENANT_A", "STORE_2", "PROD_1", "USD", date(2026, 10, 6))
    assert after_w1[old] == 1  # EVT-3, revision 1
    now = sale_lines_by_key(h)
    assert old not in now
    assert now[new] == 1  # EVT-3, revision 2: another store and date


def test_business_tables_hold_no_sandbox_rows(gold: tuple[Harness, dict]) -> None:
    h, _ = gold
    for table in (POSITION, DAILY, SALES_DAILY, PROMO):
        assert h.table(table).filter(F.col("tenant_id") == "TENANT_SBX").count() == 0, table


def reconciled(h: Harness, source_object: str) -> dict[tuple[str, str], tuple[int, ...]]:
    return {
        (row.window_id, row.tenant_id): (
            row.source_rows,
            row.bronze_rows,
            row.silver_rows,
            row.quarantined,
            row.filtered,
            row.superseded,
            row.unexplained_rows,
        )
        for row in h.table(RECONCILIATION).filter(F.col("source_object") == source_object).collect()
    }


def test_reconciliation_explains_every_row(gold: tuple[Harness, dict]) -> None:
    h = gold[0]
    assert h.table(RECONCILIATION).filter("unexplained_rows <> 0").count() == 0
    # (source, bronze, silver, quarantined, filtered, superseded, unexplained); the three
    # revisions superseded later count in W1, where they arrived.
    assert reconciled(h, "partner_api.events") == {
        (W1, "TENANT_A"): (9, 9, 6, 0, 0, 3, 0),
        (W1, "TENANT_SBX"): (1, 1, 0, 0, 1, 0, 0),
        (W2, "TENANT_A"): (6, 6, 5, 1, 0, 0, 0),
        (W3, "TENANT_A"): (5, 5, 5, 0, 0, 0, 0),
    }
    assert reconciled(h, "erp.inventory_ledger") == {
        (W1, "TENANT_A"): (1, 1, 1, 0, 0, 0, 0),
        (W1, "TENANT_SBX"): (1, 1, 0, 0, 1, 0, 0),
        (W2, "TENANT_A"): (1, 1, 1, 0, 0, 0, 0),
        (W3, "TENANT_B"): (1, 1, 1, 0, 0, 0, 0),
        (W3, "TENANT_SBX"): (1, 1, 0, 0, 1, 0, 0),
    }
    # Earlier ERP snapshots are superseded by the latest one.
    tenants = reconciled(h, "erp.tenants")
    assert tenants[(W1, "TENANT_A")] == (1, 1, 0, 0, 0, 1, 0)
    assert tenants[(W3, "TENANT_SBX")] == (1, 1, 1, 0, 0, 0, 0)
    # Price 8 is a duplicate of price 4; the product API's latest snapshot is current.
    assert reconciled(h, "open_prices.prices") == {
        (W1, "GLOBAL"): (2, 2, 1, 0, 1, 0, 0),
        (W2, "GLOBAL"): (1, 1, 1, 0, 0, 0, 0),
        (W3, "GLOBAL"): (1, 1, 1, 0, 0, 0, 0),
    }
    assert reconciled(h, "open_food_facts.product_api") == {
        (W1, "GLOBAL"): (2, 2, 0, 0, 0, 2, 0),
        (W2, "GLOBAL"): (2, 2, 0, 0, 0, 2, 0),
        (W3, "GLOBAL"): (2, 2, 2, 0, 0, 0, 0),
    }


def test_pipeline_health_adds_up_to_reconciliation(gold: tuple[Harness, dict]) -> None:
    h = gold[0]
    health = h.table(HEALTH)
    sales_w1 = health.filter(
        (F.col("window_id") == W1)
        & (F.col("tenant_id") == "TENANT_A")
        & (F.col("table_name") == "silver.fact_pos_sale")
    ).first()
    # W1 sales: 1, 6 (cancelled), 8, 9 and 10 are current; 3, 5 and 7 were superseded.
    assert (sales_w1.written, sales_w1.superseded, sales_w1.tenant_mismatch_rows) == (5, 3, 5)
    assert (sales_w1.sales_with_public_price, sales_w1.sales_without_public_price) == (0, 4)
    assert sales_w1.public_price_coverage == Decimal("0.0000")
    assert sales_w1.run_status == SUCCESS and sales_w1.run_duration_seconds is not None
    sales_w2 = health.filter(
        (F.col("window_id") == W2) & (F.col("table_name") == "silver.fact_pos_sale")
    ).first()
    assert [(r.reason, r.rows) for r in sales_w2.quarantine_reasons] == [
        ("negative unit_price_cents", 1)
    ]
    sums = {
        (row.window_id, row.tenant_id, row.source_object): tuple(row)[3:]
        for row in health.groupBy("window_id", "tenant_id", "source_object")
        .agg(*(F.sum(c) for c in ("written", "quarantined", "filtered", "superseded")))
        .collect()
    }
    expected = {
        (row.window_id, row.tenant_id, row.source_object): (
            row.silver_rows,
            row.quarantined,
            row.filtered,
            row.superseded,
        )
        for row in h.table(RECONCILIATION).collect()
    }
    assert sums == expected


def test_rerun_leaves_derived_tables_identical_and_writes_nothing(
    gold: tuple[Harness, dict],
) -> None:
    h = gold[0]
    lake = h.pipeline.lakehouse
    derived = (*GOLD_TABLES, *GRAPH_TABLES, *CATALOG_TABLES)
    before = h.pipeline.fingerprints()
    versions = {table: lake.version(table) for table in derived}
    assert h.pipeline.run_window(W1).status == SUCCESS
    assert h.pipeline.fingerprints() == before
    commits = {table: lake.commits_since(table, versions[table]) for table in derived}
    # Only the run columns and load times of the window that ran: written during the run,
    # then refreshed with the final duration once the run is finished.
    assert {table for table, n in commits.items() if n} <= {HEALTH, FRESHNESS}
    latest_run = lake.read("platform.run_log").agg(F.max("run_id")).first()[0]
    for table in (HEALTH, FRESHNESS):
        rewritten = lake.read(table).filter(F.col("_run_id") == latest_run)
        assert {row.window_id for row in rewritten.select("window_id").distinct().collect()} <= {
            W1
        }, table

    versions = {table: lake.version(table) for table in derived}
    assert h.pipeline.rebuild_gold().status == SUCCESS
    assert h.pipeline.fingerprints() == before
    assert {t: n for t in derived if (n := lake.commits_since(t, versions[t]))} == {}
