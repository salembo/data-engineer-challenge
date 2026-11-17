"""fact_inventory_movement: one row per ledger entry of the real tenants.

- Sandbox entries are excluded (counted as filtered); entries of a tenant missing
  from dim_tenant go to quarantine.
- Every status is kept; `is_posted` marks the only one that counts in gold.
- `is_late_arriving`: the business date is earlier than the window the entry arrived in.
- `is_reference_doc_reused`: another entry of the ledger carries the same document,
  in any window and of any tenant. Both entries are kept and flagged. It is computed
  against every bronze window, so re-running any window gives the same flags.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from arkham.bronze.context import RunContext
from arkham.config import Window
from arkham.lakehouse import HASH_EXCLUDED, with_lineage
from arkham.runlog import FILTERED, FLAGGED, QUARANTINED, READ, WRITTEN
from arkham.silver import dimensions, quarantine

TABLE = "silver.fact_inventory_movement"
SOURCE = "bronze.erp_inventory_ledger"

STATUSES = {
    "POSTED_00": "POSTED",
    "VOID_CANCELLED_99": "VOIDED",
    "PENDING_APPROVAL": "PENDING",
    "DUPLICATE_REPLAY": "DUPLICATE_REPLAY",
}


def movement_status(raw: F.Column) -> F.Column:
    status = F.lit("UNKNOWN")
    for source, normalized in reversed(STATUSES.items()):
        status = F.when(raw == source, F.lit(normalized)).otherwise(status)
    return status


def shared_documents(ctx: RunContext) -> DataFrame:
    """Documents carried by more than one ledger entry across all landed windows."""
    return (
        ctx.lakehouse.read(SOURCE)
        .groupBy("reference_doc_id")
        .agg(F.countDistinct("ledger_entry_id").alias("entries"))
        .filter(F.col("entries") > 1)
        .select("reference_doc_id", F.lit(True).alias("_shared"))
    )


def load(ctx: RunContext, window: Window) -> None:
    lake = ctx.lakehouse
    ledger = lake.read(SOURCE).filter(F.col("_window_id") == window.id)
    tenants = dimensions.tenants(dimensions.snapshot(lake, "bronze.erp_tenants", window.id))
    locations = dimensions.locations(dimensions.snapshot(lake, "bronze.erp_locations", window.id))
    products = dimensions.products(dimensions.snapshot(lake, "bronze.erp_products", window.id))

    entries = ledger.join(
        tenants.select("tenant_id", "is_sandbox", F.lit(True).alias("_tenant_known")),
        "tenant_id",
        "left",
    )
    unknown = entries.filter(F.col("_tenant_known").isNull())
    real = entries.filter(F.col("_tenant_known") & ~F.col("is_sandbox"))

    facts = (
        real.join(
            locations.select("location_id", F.col("tenant_id").alias("location_owner_tenant_id")),
            "location_id",
            "left",
        )
        .join(
            products.select("product_id", F.col("tenant_id").alias("product_owner_tenant_id")),
            "product_id",
            "left",
        )
        .join(shared_documents(ctx), "reference_doc_id", "left")
        .select(
            "ledger_entry_id",
            "tenant_id",
            "location_id",
            "location_owner_tenant_id",
            "product_id",
            "product_owner_tenant_id",
            "movement_type",
            "quantity_delta",
            "entry_status",
            movement_status(F.col("entry_status")).alias("movement_status"),
            (F.col("entry_status") == "POSTED_00").alias("is_posted"),
            "reference_doc_id",
            F.coalesce(F.col("_shared"), F.lit(False)).alias("is_reference_doc_reused"),
            "is_deleted",
            "event_occurred_at",
            "ingested_at",
            (F.col("event_occurred_at") < F.lit(window.start)).alias("is_late_arriving"),
            F.get_json_object("lowcode_workflow_meta", "$.workflow_run_id").alias(
                "workflow_run_id"
            ),
            F.get_json_object("lowcode_workflow_meta", "$.schema_version").alias(
                "workflow_schema_version"
            ),
            "lowcode_workflow_meta",
            F.col("_window_id"),
            F.lit(SOURCE).alias("_source"),
        )
    )
    lake.merge(TABLE, with_lineage(facts, ctx.run_id, ctx.loaded_at), ["ledger_entry_id"])
    refresh_shared_flags(ctx, window)

    quarantined = unknown.count()
    if quarantined:
        quarantine.write(
            ctx,
            quarantine.quarantine_rows(
                unknown,
                source_table=SOURCE,
                target_table=TABLE,
                record_key=F.col("ledger_entry_id"),
                reason=F.lit("tenant not in dim_tenant"),
                raw_record=F.to_json(F.struct(*ledger.columns)),
                tenant_id=F.col("tenant_id"),
                window_id=F.col("_window_id"),
            ),
        )
    current = lake.read(TABLE).filter(F.col("_window_id") == window.id)
    ctx.record.count(TABLE, READ, ledger.count())
    ctx.record.count(TABLE, FILTERED, entries.filter(F.col("is_sandbox")).count(), "sandbox tenant")
    ctx.record.count(TABLE, QUARANTINED, quarantined)
    ctx.record.count(TABLE, WRITTEN, current.count())
    late = current.filter("is_late_arriving").count()
    reused = current.filter("is_reference_doc_reused").count()
    ctx.record.count(
        TABLE,
        FLAGGED,
        current.filter("is_late_arriving OR is_reference_doc_reused").count(),
        f"late arriving {late}; reused document {reused}",
    )


def refresh_shared_flags(ctx: RunContext, window: Window) -> None:
    """Entries of other windows whose document this window now shares get flagged too.

    The window's documents come from bronze, sandbox included: a sandbox entry that reuses
    a real entry's document flags the real one, even though the sandbox entry is not in silver.
    """
    lake = ctx.lakehouse
    window_docs = (
        lake.read(SOURCE)
        .filter(F.col("_window_id") == window.id)
        .select("reference_doc_id")
        .distinct()
    )
    others = (
        lake.read(TABLE)
        .filter(F.col("_window_id") != window.id)
        .join(window_docs, "reference_doc_id", "left_semi")
    )
    refreshed = (
        others.join(shared_documents(ctx), "reference_doc_id", "left")
        .withColumn("is_reference_doc_reused", F.coalesce(F.col("_shared"), F.lit(False)))
        .drop("_shared", *HASH_EXCLUDED)
    )
    lake.merge(TABLE, with_lineage(refreshed, ctx.run_id, ctx.loaded_at), ["ledger_entry_id"])
