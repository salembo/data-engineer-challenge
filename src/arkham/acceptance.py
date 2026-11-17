"""Baseline checks: the exact numbers of the provided dataset, W1, W2 and W3 of the legacy
sources as delivered with the assessment.

- v1 (docs/prd.md, acceptance criteria v1): `make verify-v1`.
- v2 (docs/PRD v2-v4 Arkham Data Platform.md, v2 acceptance criteria): `make verify-v2`.
- v3 (same document, v3 acceptance criteria: catalog and graph): `make verify-v3`.

The expected values were computed from the generation logic of the legacy sources, which
is deterministic, before the pipeline existed; they are an independent reference for this
dataset and a regression test for the pipeline. They mean nothing for other data, so the
checks run only when the environment is the baseline: the legacy sources match the
provided files (and the ERP was seeded from them; `make clean` re-seeds it) and the window
calendar is the baseline's. Otherwise they report "not applicable".

This is the only module that names values of the dataset (tenants, nodes, windows). The
checks that hold on any data are in invariants.py (`make verify`); re-run identity is
`make verify-rerun`. Values that depend on live public data are reported, not checked.
"""

from __future__ import annotations

import hashlib
from pathlib import Path

from pyspark.sql import functions as F

from arkham.config import WindowCalendar
from arkham.invariants import Check, real_deliveries, sandbox_tenants, totals
from arkham.pipeline import Pipeline
from arkham.silver import partner
from arkham.silver.dimensions import latest_window

__all__ = ["Check", "baseline_differences", "run_checks", "run_v2_checks", "run_v3_checks"]

# The provided legacy sources (SHA-256), relative to legacy_sources/.
BASELINE_SOURCES = {
    "erp_db/init_erp.sql": "63f57d9274b07409a03ba0b26693d912bc359aa5ed0216cfdc841013e7c84bc7",
    "partner_api/main.py": "8a610dafeed0757583f3b5005cc092be7edd47596481803b9d199130da2a0ee3",
}
# The baseline's window calendar: ID, start (included) and end (excluded), in UTC.
BASELINE_CALENDAR = (
    ("2026-10-W1", "2026-10-01T00:00:00+00:00", "2026-10-06T00:00:00+00:00"),
    ("2026-10-W2", "2026-10-06T00:00:00+00:00", "2026-10-11T00:00:00+00:00"),
    ("2026-10-W3", "2026-10-11T00:00:00+00:00", "2026-10-16T00:00:00+00:00"),
)


def baseline_differences(calendar: WindowCalendar, legacy_root: Path) -> list[str]:
    """Why this environment is not the dataset the expected values describe; empty when
    it is."""
    reasons = []
    windows = tuple((w.id, w.start.isoformat(), w.end.isoformat()) for w in calendar.windows)
    if windows != BASELINE_CALENDAR:
        reasons.append(
            "the window calendar in config/pipeline.toml is not the baseline's: "
            + ", ".join(w[0] for w in BASELINE_CALENDAR)
        )
    for name, digest in BASELINE_SOURCES.items():
        path = legacy_root / name
        if not path.is_file():
            reasons.append(f"legacy_sources/{name} cannot be read at {path}")
        elif hashlib.sha256(path.read_bytes()).hexdigest() != digest:
            reasons.append(f"legacy_sources/{name} is not the version provided with the assessment")
    return reasons


def _windows_complete(pipeline: Pipeline) -> Check:
    windows = [w.id for w in pipeline.config.calendar.windows]
    complete = [w for w in windows if pipeline.runlog.is_complete(w)]
    return Check("windows complete", complete, windows)


def run_checks(pipeline: Pipeline) -> list[Check]:
    lake = pipeline.lakehouse
    read = lake.read
    calendar = pipeline.config.calendar
    checks = [_windows_complete(pipeline)]

    snapshot = latest_window(lake, calendar)
    for table, expected in (("erp_tenants", 5), ("erp_locations", 128), ("erp_products", 200)):
        rows = read(f"bronze.{table}").filter(F.col("_window_id") == snapshot).count()
        checks.append(Check(f"ERP {table} rows (snapshot {snapshot})", rows, expected))
    checks.append(
        Check("ERP ledger rows, all windows", read("bronze.erp_inventory_ledger").count(), 250_000)
    )

    products = read("silver.dim_product")
    checks.append(
        Check("distinct GTIN-14 in dim_product", products.select("gtin14").distinct().count(), 18)
    )
    checks.append(
        Check(
            "barcodes longer than 14 digits", products.filter("gtin_status = 'TOO_LONG'").count(), 0
        )
    )

    real = real_deliveries(pipeline)
    local = real.filter(F.col("event_timestamp").rlike(r"^\d{4}/\d{2}/\d{2} "))
    checks.append(Check("real deliveries with a local-format date", local.count(), 3_000))
    quarantine = read("platform.quarantine").filter(F.col("source_table") == partner.SOURCE)
    unparseable = quarantine.filter(F.col("reason").contains("unparseable")).count()
    checks.append(Check("events quarantined for an unparseable date", unparseable, 0))
    sales = read("silver.fact_pos_sale")
    event5 = sales.filter(F.col("event_id") == "EVT-0000005").select(
        F.date_format("event_occurred_at", "yyyy-MM-dd'T'HH:mm:ss'Z'").alias("t")
    )
    checks.append(
        Check(
            "event 5 occurred at (UTC)", [r.t for r in event5.collect()], ["2026-10-01T00:01:20Z"]
        )
    )

    audits = read("silver.fact_shelf_audit")
    checks.append(Check("fact_pos_sale rows", sales.count(), 23_554))
    checks.append(Check("fact_pos_sale active", sales.filter("NOT is_cancelled").count(), 22_796))
    checks.append(Check("fact_pos_sale cancelled", sales.filter("is_cancelled").count(), 758))
    checks.append(Check("fact_shelf_audit rows", audits.count(), 5_888))
    negative = quarantine.filter(F.col("reason").contains("negative unit_price_cents")).count()
    checks.append(Check("quarantined events with a negative unit price", negative, 308))
    current = sales.count() + audits.count() + quarantine.count()
    checks.append(
        Check(
            "superseded revisions (real deliveries - current events)", real.count() - current, 250
        )
    )

    movements = read("silver.fact_inventory_movement")
    checks.append(Check("fact_inventory_movement rows", movements.count(), 100_000))
    checks.append(Check("posted movements", movements.filter("is_posted").count(), 88_408))
    checks.append(
        Check(
            "movements sharing a reference_doc_id",
            movements.filter("is_reference_doc_reused").count(),
            1_600,
            "both movements of a shared document are flagged",
        )
    )
    checks.append(
        Check("late-arriving movements", movements.filter("is_late_arriving").count(), 3_555)
    )
    checks.append(Check("sandbox tenants flagged in dim_tenant", len(sandbox_tenants(pipeline)), 1))
    return checks


# The v2 PRD's sales totals: tenant -> (currency, lines, units, gross cents, net cents).
EXPECTED_SALES = {
    "TENANT_CPG_002": ("MXN", 8_548, 51_288, 51_413_752, 46_110_496),
    "TENANT_RTL_003": ("EUR", 5_700, 39_900, 39_957_344, 35_176_072),
    "TENANT_RTL_004": ("USD", 8_548, 68_404, 68_358_608, 60_044_458),
}
EXPECTED_NO_PROMO = {"TENANT_CPG_002": 8_548, "TENANT_RTL_003": 672, "TENANT_RTL_004": 1_006}


def run_v2_checks(pipeline: Pipeline) -> list[Check]:
    read = pipeline.lakehouse.read
    calendar = pipeline.config.calendar
    checks = [_windows_complete(pipeline)]

    # Inventory.
    position = read("gold.inventory_position")
    checks.append(Check("inventory_position rows", position.count(), 240))
    checks.append(
        Check("inventory_position stores", position.select("location_id").distinct().count(), 48)
    )
    checks.append(
        Check(
            "inventory_position rows with a negative net position",
            position.filter("net_position_units < 0").count(),
            180,
        )
    )
    checks.append(
        Check(
            "inbound, outbound and net units",
            totals(position, "inbound_units", "outbound_units", "net_position_units"),
            (1_259_667, 906_172, 353_495),
        )
    )

    # Sales.
    sales_daily = read("gold.pos_sales_daily")
    measured = {
        row.tenant_id: (row.currency, row.lines, row.units, row.gross, row.net)
        for row in sales_daily.groupBy("tenant_id", "currency")
        .agg(
            F.sum("sale_lines").alias("lines"),
            F.sum("units_sold").alias("units"),
            F.sum("gross_revenue_cents").alias("gross"),
            F.sum("net_revenue_cents").alias("net"),
        )
        .collect()
    }
    for tenant, expected in EXPECTED_SALES.items():
        checks.append(
            Check(
                f"sales {tenant} (currency, lines, units, gross, net)",
                measured.get(tenant),
                expected,
            )
        )
    checks.append(Check("tenants with sales", sorted(measured), sorted(EXPECTED_SALES)))
    checks.append(
        Check("sale lines in pos_sales_daily", totals(sales_daily, "sale_lines")[0], 22_796)
    )
    levels = {
        row.price_match_level: int(row.lines)
        for row in sales_daily.groupBy("price_match_level")
        .agg(F.sum("sale_lines").alias("lines"))
        .collect()
    }
    checks.append(
        Check(
            "sale lines by price_match_level", levels, levels, "informative: depends on live prices"
        )
    )
    no_promo = {
        row.tenant_id: int(row.lines)
        for row in read("gold.pos_promo_performance")
        .filter("promo_campaign_id = 'NONE'")
        .groupBy("tenant_id")
        .agg(F.sum("sale_lines").alias("lines"))
        .collect()
    }
    checks.append(Check("sale lines under NONE per tenant", no_promo, EXPECTED_NO_PROMO))

    # Reconciliation.
    reconciliation = read("gold.reconciliation")
    outcomes = ("bronze_rows", "silver_rows", "quarantined", "filtered", "superseded")
    events = reconciliation.filter(F.col("source_object") == "partner_api.events")
    checks.append(
        Check(
            "partner events: bronze, silver, quarantined, filtered, superseded",
            totals(events, *outcomes),
            (60_000, 29_442, 308, 30_000, 250),
        )
    )
    superseded_by_window = {
        row.window_id: int(row.n)
        for row in events.groupBy("window_id").agg(F.sum("superseded").alias("n")).collect()
        if row.n
    }
    checks.append(
        Check(
            "partner events superseded, by arrival window",
            superseded_by_window,
            {calendar.first.id: 250},
        )
    )
    ledger = reconciliation.filter(F.col("source_object") == "erp.inventory_ledger")
    checks.append(
        Check(
            "ledger: bronze, silver, quarantined, filtered, superseded",
            totals(ledger, *outcomes),
            (250_000, 100_000, 0, 150_000, 0),
        )
    )
    return checks


def run_v3_checks(pipeline: Pipeline) -> list[Check]:
    read = pipeline.lakehouse.read
    checks = [_windows_complete(pipeline)]

    personal = sorted(
        (row.table_name, tuple(row.personal_data_columns))
        for row in read("catalog.datasets").filter("contains_personal_data").collect()
    )
    checks.append(
        Check("tables flagged as personal data", personal, [("bronze.open_prices", ("raw_json",))])
    )

    nodes, graph_edges = read("graph.nodes"), read("graph.edges")
    node_types = {r.node_type: r["count"] for r in nodes.groupBy("node_type").count().collect()}
    checks.append(Check("graph.nodes by type", node_types, {"LOCATION": 127, "PRODUCT": 200}))
    edge_types = {
        r.edge_type: r["count"] for r in graph_edges.groupBy("edge_type").count().collect()
    }
    checks.append(
        Check(
            "graph.edges by type",
            edge_types,
            {"SUPPLIED_BY": 124, "SOURCED_FROM": 200, "SUBSTITUTED_BY": 199},
        )
    )
    upstream = read("graph.upstream_dependencies")
    central = upstream.filter(F.col("upstream_node_id") == "DC_CENTRAL_HUB_01")
    checks.append(
        Check(
            "stores depending on DC_CENTRAL_HUB_01 (total, direct, through DC_WEST_HUB_04)",
            (
                central.count(),
                central.filter("hops = 1").count(),
                central.join(
                    upstream.filter(F.col("upstream_node_id") == "DC_WEST_HUB_04").select(
                        "store_id"
                    ),
                    "store_id",
                    "left_semi",
                ).count(),
            ),
            (72, 48, 24),
        )
    )
    deepest = upstream.groupBy("store_id").agg(F.max("hops").alias("depth"))
    checks.append(
        Check(
            "upstream_dependencies rows, and stores by depth",
            (
                upstream.count(),
                {r.depth: r["count"] for r in deepest.groupBy("depth").count().collect()},
            ),
            (264, {2: 96, 3: 24}),
        )
    )
    flagged = sorted(
        (r.node_id, str(r.store_share), r.dependent_tenants)
        for r in read("graph.bottlenecks").filter("is_bottleneck").collect()
    )
    checks.append(
        Check(
            "bottlenecks flagged (node, store share, dependent tenants)",
            flagged,
            [("DC_CENTRAL_HUB_01", "0.6000", 4), ("PLANT_NA_01", "0.6000", 4)],
        )
    )
    checks.append(Check("graph.bottlenecks rows", read("graph.bottlenecks").count(), 7))
    cross = read("graph.cross_tenant_dependencies")
    checks.append(
        Check(
            "cross-tenant dependencies by type",
            (
                cross.count(),
                {r.edge_type: r["count"] for r in cross.groupBy("edge_type").count().collect()},
            ),
            (368, {"SUPPLIED_BY": 19, "SOURCED_FROM": 150, "SUBSTITUTED_BY": 199}),
        )
    )
    paths = read("graph.substitution_paths")
    longest = paths.agg(F.max("hops")).first()[0]
    checks.append(
        Check(
            "substitution paths, longest chain, products with it, rows for PROD_0001",
            (
                paths.count(),
                longest,
                paths.filter(F.col("hops") == longest).count(),
                paths.filter(F.col("product_id") == "PROD_0001").count(),
            ),
            (2_080, 20, 9, 0),
        )
    )
    return checks
