"""The gold stage of a run: the business tables, then reconciliation and pipeline health.

Every table is recomputed in full from silver (reconciliation also reads bronze) and
synchronised with its result, so the same silver always gives the same gold and a run
with no changes writes nothing. A table whose silver inputs do not exist yet, as in a
reference load before any window, is skipped.
"""

from __future__ import annotations

from arkham.bronze.context import RunContext
from arkham.gold import health, inventory, sales
from arkham.gold.common import publish, stable
from arkham.lakehouse import TABLES, with_lineage
from arkham.runlog import FLAGGED

GOLD_TABLES = tuple(name for name, spec in TABLES.items() if spec.layer == "gold")


def run(ctx: RunContext) -> None:
    lake = ctx.lakehouse

    if all(lake.exists(table) for table in inventory.INPUTS):
        movements = inventory.posted_movements(lake)
        locations = lake.read("silver.dim_location")
        publish(
            ctx,
            inventory.POSITION,
            inventory.position(movements, locations),
            inventory.POSITION_KEYS,
        )
        periods = inventory.periods(ctx.spark, ctx.config.calendar)
        publish(ctx, inventory.DAILY, inventory.daily(movements, periods), inventory.DAILY_KEYS)

    lines = None
    if all(lake.exists(table) for table in sales.INPUTS):
        max_age = ctx.config.gold.public_price_max_age_months
        lines = stable(ctx, sales.priced_lines(lake, max_age), "gold_sale_lines")
        publish(ctx, sales.SALES_DAILY, sales.sales_daily(lines, lake), sales.DAILY_KEYS)
        publish(ctx, sales.PROMO, sales.promo_performance(lines), sales.PROMO_KEYS)

    runs = health.chosen_runs(ctx)
    counts = health.source_counts(ctx, runs)
    rows = stable(ctx, health.classified_rows(ctx, lines, counts), "gold_classified_rows")
    reconciliation = stable(ctx, health.reconciliation(rows, counts), "gold_reconciliation_result")
    publish(ctx, health.RECONCILIATION, reconciliation, health.RECONCILIATION_KEYS)
    unexplained = reconciliation.filter("unexplained_rows <> 0").count()
    if unexplained:
        ctx.record.count(
            health.RECONCILIATION, FLAGGED, unexplained, "rows with unexplained_rows <> 0"
        )
    window_ids = [window.id for window in ctx.config.calendar.windows]
    publish(
        ctx,
        health.HEALTH,
        health.pipeline_health(rows, health.run_attributes(ctx, runs), window_ids),
        health.HEALTH_KEYS,
    )


def refresh_run_attributes(ctx: RunContext) -> None:
    """Runs after the run is recorded as finished: replaces the provisional status and
    duration of the run in gold.pipeline_health with the final ones from the run log, so
    a later run recomputes the same values and rewrites only its own window's rows."""
    lake = ctx.lakehouse
    if not lake.exists(health.HEALTH):
        return
    runs = health.run_attributes(ctx, health.chosen_runs(ctx, include_current=False))
    window_ids = [window.id for window in ctx.config.calendar.windows]
    current = lake.read(health.HEALTH).drop("_row_hash", "_run_id", "_loaded_at")
    refreshed = health.with_final_run_attributes(current, runs, window_ids)
    rows = stable(ctx, with_lineage(refreshed, ctx.run_id, ctx.loaded_at), "gold_health_refresh")
    lake.sync(health.HEALTH, rows, health.HEALTH_KEYS)
