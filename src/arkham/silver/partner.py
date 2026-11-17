"""Partner events to fact_pos_sale and fact_shelf_audit (docs/prd.md, silver design).

- One row per business event (`event_id`); its current version is the highest
  (`revision`, `sequence_cursor`) seen so far in any window.
- The current version is compared with what silver already holds, in both fact tables
  and in quarantine, so re-running an earlier window never reverts a later revision.
- Rules are checked on the current version. If it fails, the whole event goes to
  quarantine and an earlier revision is not restored; if it passes, any quarantined
  version of the event is released.
- Sandbox events are excluded; events of a tenant missing from dim_tenant are quarantined.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window as SparkWindow

from arkham.bronze.context import RunContext
from arkham.config import Window
from arkham.gtin import INVALID, gtin14_column, gtin_status_column
from arkham.lakehouse import with_lineage
from arkham.runlog import FILTERED, FLAGGED, QUARANTINED, READ, SUPERSEDED, WRITTEN
from arkham.silver import dimensions, quarantine
from arkham.silver.normalize import parse_fraction, parse_timestamp

SOURCE = "bronze.partner_events"
SALES = "silver.fact_pos_sale"
AUDITS = "silver.fact_shelf_audit"
POS_CHECKOUT = "POS_CHECKOUT"
SHELF_AUDIT = "SHELF_AUDIT_SCAN"

EVENT_SCHEMA = (
    "event_id STRING, sequence_cursor BIGINT, revision INT, tenant_id STRING, "
    "store_id STRING, product_id STRING, gtin_barcode STRING, event_type STRING, "
    "units_sold INT, unit_price_cents BIGINT, discount_rate STRING, currency STRING, "
    "record_state STRING, is_deleted BOOLEAN, event_timestamp STRING, ingested_at STRING, "
    "batch_window STRING, "
    "promo_mechanics STRUCT<campaign_id: STRING, mechanic_type: STRING, "
    "co_funded_cpg_ratio: STRING>"
)


def version() -> F.Column:
    """The ordering of an event's versions: revision first, then delivery cursor."""
    return F.struct(F.col("revision"), F.col("sequence_cursor"))


def parsed_deliveries(ctx: RunContext, window: Window) -> DataFrame:
    deliveries = ctx.lakehouse.read(SOURCE).filter(F.col("_window_id") == window.id)
    return deliveries.select(
        F.from_json("raw_json", EVENT_SCHEMA).alias("e"), "raw_json", "_window_id"
    ).select("e.*", "raw_json", "_window_id")


def typed(events: DataFrame, window: Window) -> DataFrame:
    occurred = parse_timestamp(F.col("event_timestamp"))
    return events.select(
        "*",
        occurred.alias("event_occurred_at"),
        parse_timestamp(F.col("ingested_at")).alias("ingested_at_utc"),
        parse_fraction(F.col("discount_rate")).alias("discount_fraction"),
        parse_fraction(F.col("promo_mechanics.co_funded_cpg_ratio")).alias("promo_co_funded_ratio"),
        gtin14_column(F.col("gtin_barcode")).alias("event_gtin14"),
        gtin_status_column(F.col("gtin_barcode")).alias("event_gtin_status"),
        (occurred < F.lit(window.start)).alias("is_late_arriving"),
    )


def with_reasons(events: DataFrame) -> DataFrame:
    return events.withColumn(
        "reason",
        quarantine.reasons(
            (
                F.col("event_id").isNull() | F.col("revision").isNull(),
                "missing event_id or revision",
            ),
            (F.col("unit_price_cents").isNull(), "missing unit_price_cents"),
            (F.col("units_sold").isNull(), "missing units_sold"),
            (F.col("unit_price_cents") < 0, "negative unit_price_cents"),
            (F.col("event_occurred_at").isNull(), "unparseable event_timestamp"),
            (F.col("ingested_at_utc").isNull(), "unparseable ingested_at"),
            # isNull first: isin() on a null is null, which would skip the rule silently.
            (
                F.col("event_type").isNull() | ~F.col("event_type").isin(POS_CHECKOUT, SHELF_AUDIT),
                "unknown event_type",
            ),
            (
                (F.col("event_type") == POS_CHECKOUT) & F.col("discount_fraction").isNull(),
                "discount_rate is not a fraction between 0 and 1",
            ),
            (F.col("event_gtin_status") == INVALID, "event barcode is empty or not numeric"),
        ),
    )


def existing_versions(ctx: RunContext) -> DataFrame | None:
    """The version of each event that silver holds now, wherever it is."""
    parts = []
    for table in (SALES, AUDITS):
        if ctx.lakehouse.exists(table):
            parts.append(
                ctx.lakehouse.read(table).select("event_id", "revision", "sequence_cursor")
            )
    if ctx.lakehouse.exists(quarantine.TABLE):
        parts.append(
            ctx.lakehouse.read(quarantine.TABLE)
            .filter(F.col("source_table") == SOURCE)
            .select(F.col("record_key").alias("event_id"), "revision", "sequence_cursor")
        )
    if not parts:
        return None
    union = parts[0]
    for part in parts[1:]:
        union = union.unionByName(part)
    return union.groupBy("event_id").agg(F.max(version()).alias("held"))


def owners(ctx: RunContext, window: Window) -> tuple[DataFrame, DataFrame]:
    lake = ctx.lakehouse
    locations = dimensions.locations(dimensions.snapshot(lake, "bronze.erp_locations", window.id))
    products = dimensions.products(dimensions.snapshot(lake, "bronze.erp_products", window.id))
    return (
        locations.select(
            F.col("location_id").alias("store_id"),
            F.col("tenant_id").alias("location_owner_tenant_id"),
        ),
        products.select(
            "product_id",
            F.col("tenant_id").alias("product_owner_tenant_id"),
            F.col("gtin14").alias("product_gtin14"),
        ),
    )


def sale_rows(events: DataFrame) -> DataFrame:
    return events.select(
        "event_id",
        "revision",
        "sequence_cursor",
        "tenant_id",
        F.col("store_id").alias("location_id"),
        "location_owner_tenant_id",
        "product_id",
        "product_owner_tenant_id",
        "product_gtin14",
        F.col("gtin_barcode").alias("event_gtin_raw"),
        "event_gtin14",
        "event_gtin_status",
        "units_sold",
        "unit_price_cents",
        "currency",
        F.col("discount_fraction").alias("discount_rate"),
        F.col("promo_mechanics.campaign_id").alias("promo_campaign_id"),
        F.col("promo_mechanics.mechanic_type").alias("promo_mechanic_type"),
        "promo_co_funded_ratio",
        "record_state",
        (F.col("record_state") == "CANCELLED_VOID").alias("is_cancelled"),
        "is_deleted",
        "event_occurred_at",
        F.col("ingested_at_utc").alias("ingested_at"),
        "is_late_arriving",
        F.col("batch_window").alias("source_batch_window"),
        "_window_id",
        F.lit(SOURCE).alias("_source"),
    )


def audit_rows(events: DataFrame) -> DataFrame:
    """In a shelf audit, units_sold means units observed and the price is the shelf price."""
    return events.select(
        "event_id",
        "revision",
        "sequence_cursor",
        "tenant_id",
        F.col("store_id").alias("location_id"),
        "location_owner_tenant_id",
        "product_id",
        "product_owner_tenant_id",
        "product_gtin14",
        F.col("gtin_barcode").alias("event_gtin_raw"),
        "event_gtin14",
        "event_gtin_status",
        F.col("units_sold").alias("units_observed"),
        F.col("unit_price_cents").alias("shelf_price_cents"),
        "currency",
        "record_state",
        (F.col("record_state") == "CANCELLED_VOID").alias("is_cancelled"),
        "is_deleted",
        "event_occurred_at",
        F.col("ingested_at_utc").alias("ingested_at"),
        "is_late_arriving",
        F.col("batch_window").alias("source_batch_window"),
        "_window_id",
        F.lit(SOURCE).alias("_source"),
    )


def load(ctx: RunContext, window: Window) -> None:
    lake = ctx.lakehouse
    deliveries = parsed_deliveries(ctx, window)
    tenants = dimensions.tenants(dimensions.snapshot(lake, "bronze.erp_tenants", window.id))
    with_tenant = deliveries.join(
        tenants.select("tenant_id", "is_sandbox", F.lit(True).alias("_tenant_known")),
        "tenant_id",
        "left",
    )
    sandbox = with_tenant.filter(F.col("is_sandbox")).count()
    unknown_tenant = F.col("_tenant_known").isNull()
    real = with_tenant.filter(unknown_tenant | ~F.col("is_sandbox"))

    location_owner, product_owner = owners(ctx, window)
    checked = with_reasons(typed(real, window)).withColumn(
        "reason",
        F.when(
            unknown_tenant,
            F.concat_ws(
                "; ", F.lit("tenant not in dim_tenant"), F.nullif(F.col("reason"), F.lit(""))
            ),
        ).otherwise(F.col("reason")),
    )
    checked = checked.join(location_owner, "store_id", "left").join(
        product_owner, "product_id", "left"
    )

    # Current version within this window's deliveries.
    latest_first = SparkWindow.partitionBy("event_id").orderBy(
        F.col("revision").desc(), F.col("sequence_cursor").desc()
    )
    ranked = checked.withColumn("_rank", F.row_number().over(latest_first))
    superseded_in_window = ranked.filter(F.col("_rank") > 1).count()
    incoming = ranked.filter(F.col("_rank") == 1).drop("_rank")

    # Compare with the version silver already holds. Equal versions win too, so a re-run
    # with the same deliveries rewrites nothing; older ones lose.
    held = existing_versions(ctx)
    if held is None:
        winners = incoming.withColumn("_replaces", F.lit(False))
        lost_to_later = 0
    else:
        compared = incoming.join(held, "event_id", "left")
        wins = F.col("held").isNull() | (version() >= F.col("held"))
        winners = compared.filter(wins).withColumn(
            "_replaces", F.col("held").isNotNull() & (version() > F.col("held"))
        )
        lost_to_later = compared.filter(~wins).count()
        winners = winners.drop("held")
    # Fixed once on disk, so the writes below do not change which deliveries won, and
    # nothing stays in memory after the window: the run removes its scratch folder.
    scratch = lake.scratch_path(ctx.run_id, f"partner_winners_{window.id}")
    winners.write.mode("overwrite").parquet(scratch)
    winners = ctx.spark.read.parquet(scratch)
    replaced = winners.filter("_replaces").count()

    valid = F.col("reason") == ""
    is_sale = valid & (F.col("event_type") == POS_CHECKOUT)
    is_audit = valid & (F.col("event_type") == SHELF_AUDIT)
    sales = winners.filter(is_sale)
    audits = winners.filter(is_audit)
    rejected = winners.filter(~valid)

    lake.merge(SALES, with_lineage(sale_rows(sales), ctx.run_id, ctx.loaded_at), ["event_id"])
    lake.merge(AUDITS, with_lineage(audit_rows(audits), ctx.run_id, ctx.loaded_at), ["event_id"])
    quarantine.write(
        ctx,
        quarantine.quarantine_rows(
            rejected,
            source_table=SOURCE,
            target_table=F.when(F.col("event_type") == SHELF_AUDIT, F.lit(AUDITS)).otherwise(
                F.lit(SALES)
            ),
            record_key=F.col("event_id"),
            reason=F.col("reason"),
            raw_record=F.col("raw_json"),
            tenant_id=F.col("tenant_id"),
            window_id=F.col("_window_id"),
            revision=F.col("revision"),
            sequence_cursor=F.col("sequence_cursor"),
        ),
    )
    # The winning version lives in exactly one place: remove it from the other two.
    lake.delete_keys(SALES, winners.filter(~is_sale).select("event_id"), ["event_id"])
    lake.delete_keys(AUDITS, winners.filter(~is_audit).select("event_id"), ["event_id"])
    quarantine.release(ctx, SOURCE, winners.filter(valid).select("event_id"), "event_id")

    record = ctx.record
    record.count(SALES, READ, deliveries.count(), "partner deliveries, sales and audits")
    record.count(SALES, FILTERED, sandbox, "sandbox tenant")
    record.count(SALES, SUPERSEDED, superseded_in_window + lost_to_later + replaced)
    record.count(SALES, QUARANTINED, rejected.count())
    record.count(SALES, WRITTEN, sales.count())
    record.count(AUDITS, WRITTEN, audits.count())
    late = winners.filter(valid & F.col("is_late_arriving")).count()
    record.count(SALES, FLAGGED, late, "late arriving (sales and audits)")
