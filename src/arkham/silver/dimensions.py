"""ERP dimensions: dim_tenant, dim_location and dim_product.

Silver holds the latest snapshot: the bronze partition of the latest window,
recomputed whole on every run, so re-running an earlier window never brings
back an older snapshot. Facts look up the snapshot of their own window through
the same transformations, so a fact's owner columns never depend on when it ran.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from arkham.bronze.context import RunContext
from arkham.config import WindowCalendar
from arkham.gtin import INVALID, gtin14_column, gtin_status_column
from arkham.lakehouse import Lakehouse, with_lineage
from arkham.runlog import FLAGGED, QUARANTINED, READ, WRITTEN
from arkham.silver import quarantine
from arkham.silver.normalize import lowcode_attributes

DIM_TENANT = "silver.dim_tenant"
DIM_LOCATION = "silver.dim_location"
DIM_PRODUCT = "silver.dim_product"

BRONZE = {
    DIM_TENANT: "bronze.erp_tenants",
    DIM_LOCATION: "bronze.erp_locations",
    DIM_PRODUCT: "bronze.erp_products",
}


def snapshot(lakehouse: Lakehouse, bronze_table: str, window_id: str) -> DataFrame:
    return lakehouse.read(bronze_table).filter(F.col("_window_id") == window_id)


def latest_window(lakehouse: Lakehouse, calendar: WindowCalendar) -> str | None:
    """The latest window, in calendar order, whose ERP snapshot is in bronze."""
    if not lakehouse.exists("bronze.erp_tenants"):
        return None
    landed = {
        row._window_id
        for row in lakehouse.read("bronze.erp_tenants").select("_window_id").distinct().collect()
    }
    candidates = [w.id for w in calendar.windows if w.id in landed]
    return candidates[-1] if candidates else None


def tenants(bronze: DataFrame) -> DataFrame:
    """The sandbox is identified by its workspace environment, never by its ID."""
    return bronze.select(
        "tenant_id",
        "tenant_slug",
        "company_name",
        "tier",
        "workspace_env",
        "region",
        "is_active",
        F.upper(F.col("workspace_env")).startswith("SANDBOX").alias("is_sandbox"),
        "created_at",
        "updated_at",
        F.col("_window_id").alias("_snapshot_window_id"),
        "_source",
    )


def locations(bronze: DataFrame) -> DataFrame:
    """parent_exists: whether the parent supply node is itself a known location."""
    parents = bronze.select(F.col("location_id").alias("_parent_id"), F.lit(True).alias("_known"))
    return bronze.join(
        parents, bronze["parent_supply_node_id"] == parents["_parent_id"], "left"
    ).select(
        "location_id",
        "tenant_id",
        "location_type",
        "parent_supply_node_id",
        F.when(F.col("parent_supply_node_id").isNull(), F.lit(None).cast("boolean"))
        .otherwise(F.coalesce(F.col("_known"), F.lit(False)))
        .alias("parent_exists"),
        "banner_name",
        "country_code",
        "postal_code",
        "operating_status",
        "updated_at",
        F.col("_window_id").alias("_snapshot_window_id"),
        "_source",
    )


def products(bronze: DataFrame) -> DataFrame:
    """Barcodes in GTIN-14 and low-code attributes typed by the shape of each value.

    The ERP declares no currency for its prices, so `currency` is null rather than guessed.
    """
    attributes = lowcode_attributes(F.col("lowcode_attributes"))
    return bronze.select(
        "product_id",
        "tenant_id",
        "internal_sku",
        F.col("gtin_barcode").alias("gtin_raw"),
        gtin14_column(F.col("gtin_barcode")).alias("gtin14"),
        gtin_status_column(F.col("gtin_barcode")).alias("gtin_status"),
        "product_name",
        "category_code",
        F.col("primary_supplier_id").alias("primary_supplier_location_id"),
        "substitute_product_id",
        "unit_cost_cents",
        "retail_price_cents",
        F.lit(None).cast("string").alias("currency"),
        "lowcode_attributes",
        *(column.alias(name) for name, column in attributes.items()),
        "is_discontinued",
        "updated_at",
        F.col("_window_id").alias("_snapshot_window_id"),
        "_source",
    )


def load(ctx: RunContext) -> None:
    window_id = latest_window(ctx.lakehouse, ctx.config.calendar)
    if window_id is None:
        return
    for table, transform in ((DIM_TENANT, tenants), (DIM_LOCATION, locations)):
        rows = transform(snapshot(ctx.lakehouse, BRONZE[table], window_id))
        ctx.lakehouse.merge(
            table, with_lineage(rows, ctx.run_id, ctx.loaded_at), [rows.columns[0]], True
        )
        written = ctx.lakehouse.read(table).count()
        ctx.record.count(table, READ, written, f"snapshot {window_id}")
        ctx.record.count(table, WRITTEN, written)

    all_products = products(snapshot(ctx.lakehouse, BRONZE[DIM_PRODUCT], window_id))
    invalid = all_products.filter(F.col("gtin_status") == INVALID)
    valid = all_products.filter(F.col("gtin_status") != INVALID)
    ctx.lakehouse.merge(
        DIM_PRODUCT, with_lineage(valid, ctx.run_id, ctx.loaded_at), ["product_id"], True
    )
    quarantined = invalid.count()
    if quarantined:
        quarantine.write(
            ctx,
            quarantine.quarantine_rows(
                invalid,
                source_table=BRONZE[DIM_PRODUCT],
                target_table=DIM_PRODUCT,
                record_key=F.col("product_id"),
                reason=F.lit("barcode is empty or not numeric"),
                raw_record=F.to_json(F.struct(*invalid.columns)),
                tenant_id=F.col("tenant_id"),
                window_id=F.col("_snapshot_window_id"),
            ),
        )
    product_rows = ctx.lakehouse.read(DIM_PRODUCT)
    ctx.record.count(DIM_PRODUCT, READ, product_rows.count() + quarantined, f"snapshot {window_id}")
    ctx.record.count(DIM_PRODUCT, WRITTEN, product_rows.count())
    ctx.record.count(DIM_PRODUCT, QUARANTINED, quarantined)
    check_digit = product_rows.filter(F.col("gtin_status") != "VALID").count()
    if check_digit:
        ctx.record.count(DIM_PRODUCT, FLAGGED, check_digit, "barcode check digit or length")
