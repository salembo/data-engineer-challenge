"""ERP to bronze: dimensions as a full snapshot per window, the ledger by arrival range.

Bronze keeps the types the ERP declares. A re-run reads the source again for
the same snapshot or range (docs/prd.md, extraction contract).
"""

from __future__ import annotations

from pyspark.sql import DataFrame

from arkham.bronze.context import RunContext
from arkham.config import Window
from arkham.erp import ErpSource
from arkham.runlog import READ, WRITTEN

# bronze table -> ERP table
DIMENSIONS = {
    "bronze.erp_tenants": "arkham_erp.tenants",
    "bronze.erp_locations": "arkham_erp.locations",
    "bronze.erp_products": "arkham_erp.products",
}
LEDGER_TABLE = "bronze.erp_inventory_ledger"
LEDGER_SOURCE = "arkham_erp.inventory_ledger"


def rows_per_tenant(source: DataFrame) -> dict[str, int]:
    """The source count per tenant, taken from the source itself at extraction time, so
    gold reconciliation can compare it with bronze without calling the source again."""
    return {row.tenant_id: row["count"] for row in source.groupBy("tenant_id").count().collect()}


def load_dimensions(ctx: RunContext, erp: ErpSource, window: Window) -> None:
    for table, source_table in DIMENSIONS.items():
        source = erp.read_table(source_table)
        ctx.record.count_by_tenant(table, READ, rows_per_tenant(source))
        rows = ctx.write_bronze(table, source, window.id, f"erp:{source_table}", window.id)
        ctx.record.count(table, WRITTEN, rows)


def load_ledger(ctx: RunContext, erp: ErpSource, window: Window) -> None:
    source = erp.read_ledger(window.start, window.end)
    ctx.record.count_by_tenant(LEDGER_TABLE, READ, rows_per_tenant(source))
    rows = ctx.write_bronze(LEDGER_TABLE, source, window.id, f"erp:{LEDGER_SOURCE}", window.id)
    ctx.record.count(LEDGER_TABLE, WRITTEN, rows)
