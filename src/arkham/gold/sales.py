"""POS sales gold: daily sales per store and product, and promotion performance.

Rules (the v2-v4 PRD, sales and public price reference):
- only ACTIVE rows of fact_pos_sale count; shelf audits are in another table;
- gross = units x unit price; net = gross x (1 - discount_rate), rounded half up to the
  cent on each line; discount = gross - net; co-funded discount = discount x the
  promotion's co-funding ratio, rounded half up per line, 0 without a promotion;
- every amount is grouped by currency and never added across currencies;
- `sale_date` is the UTC date of the event, not of its ingestion;
- the public price is a reference only: the latest price for the same GTIN-14 and
  currency, dated on or before the sale date and within the configured age. A price
  from the store's country (COUNTRY) beats a more recent one from elsewhere (CURRENCY);
  ties on the date go to the latest `updated_at`, then the highest `id`.
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window as SparkWindow

from arkham.gold.common import NONE, UNKNOWN, without_sandbox
from arkham.lakehouse import Lakehouse

SALES_DAILY = "gold.pos_sales_daily"
PROMO = "gold.pos_promo_performance"
DAILY_KEYS = ["tenant_id", "location_id", "product_id", "currency", "sale_date"]
PROMO_KEYS = ["tenant_id", "location_id", "currency", "promo_campaign_id", "promo_mechanic_type"]
INPUTS = ("silver.fact_pos_sale", "silver.dim_product", "silver.dim_location")

COUNTRY, CURRENCY = "COUNTRY", "CURRENCY"
PRICE_KEYS = ["gtin14", "currency", "country_code", "sale_date"]
PUBLIC_ATTRIBUTES = ("brand_owner", "main_category", "nutriscore_grade", "nova_group")


def _half_up(amount: Column) -> Column:
    """Rounds a decimal amount half up to whole cents (Spark rounds decimals HALF_UP)."""
    return F.round(amount, 0).cast("bigint")


def line_amounts(lines: DataFrame) -> DataFrame:
    """Adds the per-line amounts in cents to rows with units, price, rate and promotion."""
    gross = F.col("units_sold").cast("bigint") * F.col("unit_price_cents").cast("bigint")
    rate = F.coalesce(F.col("discount_rate"), F.lit(0).cast("decimal(5,4)"))
    has_promo = F.col("promo_campaign_id").isNotNull()
    return (
        lines.withColumn("gross_cents", gross)
        .withColumn("net_cents", _half_up(F.col("gross_cents") * (F.lit(1) - rate)))
        .withColumn("discount_cents", F.col("gross_cents") - F.col("net_cents"))
        .withColumn("has_promo", has_promo)
        .withColumn(
            "co_funded_discount_cents",
            F.when(
                has_promo,
                _half_up(
                    F.col("discount_cents")
                    * F.coalesce(F.col("promo_co_funded_ratio"), F.lit(0).cast("decimal(5,4)"))
                ),
            ).otherwise(F.lit(0).cast("bigint")),
        )
    )


def active_lines(lakehouse: Lakehouse) -> DataFrame:
    """Active sale lines of the real tenants, with the master barcode and store country."""
    sales = lakehouse.read("silver.fact_pos_sale").filter(F.col("record_state") == "ACTIVE")
    products = lakehouse.read("silver.dim_product").select("product_id", "gtin14")
    countries = lakehouse.read("silver.dim_location").select("location_id", "country_code")
    lines = (
        without_sandbox(sales, lakehouse)
        .join(products, "product_id", "left")
        .join(countries, "location_id", "left")
        .select(
            "event_id",
            "_window_id",
            "tenant_id",
            F.coalesce(F.col("location_id"), F.lit(UNKNOWN)).alias("location_id"),
            F.coalesce(F.col("product_id"), F.lit(UNKNOWN)).alias("product_id"),
            F.coalesce(F.col("currency"), F.lit(UNKNOWN)).alias("currency"),
            F.to_date("event_occurred_at").alias("sale_date"),
            # The ERP master's barcode, as decided for v1; the event's own is in silver.
            "gtin14",
            "country_code",
            "units_sold",
            "unit_price_cents",
            "discount_rate",
            "promo_campaign_id",
            "promo_mechanic_type",
            "promo_co_funded_ratio",
        )
    )
    return line_amounts(lines)


def price_reference(keys: DataFrame, prices: DataFrame, max_age_months: int) -> DataFrame:
    """The public price reference for each (gtin14, currency, country_code, sale_date)."""
    candidates = prices.filter(
        F.col("gtin14").isNotNull()
        & F.col("price_cents").isNotNull()
        & F.col("price_date").isNotNull()
    ).select(
        F.col("gtin14").alias("_p_gtin14"),
        F.col("currency").alias("_p_currency"),
        F.col("location_country_code").alias("_p_country"),
        F.col("price_date").alias("public_price_date"),
        F.col("updated_at").alias("_p_updated_at"),
        F.col("id").alias("_p_id"),
        F.col("price_cents").alias("public_price_cents"),
    )
    joined = keys.join(
        candidates,
        (F.col("gtin14") == F.col("_p_gtin14"))
        & (F.col("currency") == F.col("_p_currency"))
        & (F.col("public_price_date") <= F.col("sale_date"))
        & (F.col("public_price_date") >= F.add_months(F.col("sale_date"), -max_age_months)),
        "left",
    )
    level = (
        F.when(F.col("_p_id").isNull(), F.lit(NONE))
        .when(F.col("_p_country") == F.col("country_code"), F.lit(COUNTRY))
        .otherwise(F.lit(CURRENCY))
    )
    level_rank = F.when(level == COUNTRY, 0).when(level == CURRENCY, 1).otherwise(2)
    best_first = SparkWindow.partitionBy(*PRICE_KEYS).orderBy(
        level_rank,
        F.col("public_price_date").desc_nulls_last(),
        F.col("_p_updated_at").desc_nulls_last(),
        F.col("_p_id").desc_nulls_last(),
    )
    return (
        joined.withColumn("price_match_level", level)
        .withColumn("_rank", F.row_number().over(best_first))
        .filter(F.col("_rank") == 1)
        .select(*PRICE_KEYS, "public_price_cents", "public_price_date", "price_match_level")
    )


def priced_lines(lakehouse: Lakehouse, max_age_months: int) -> DataFrame:
    """Active sale lines with their public price reference."""
    lines = active_lines(lakehouse)
    keys = lines.select(*PRICE_KEYS).distinct()
    if lakehouse.exists("silver.ref_public_price"):
        reference = price_reference(keys, lakehouse.read("silver.ref_public_price"), max_age_months)
    else:
        reference = keys.select(
            *PRICE_KEYS,
            F.lit(None).cast("bigint").alias("public_price_cents"),
            F.lit(None).cast("date").alias("public_price_date"),
            F.lit(NONE).alias("price_match_level"),
        )
    # The reference is derived from `lines`; renaming its keys avoids an ambiguous self-join.
    reference = reference.select(
        *(F.col(key).alias(f"_ref_{key}") for key in PRICE_KEYS),
        "public_price_cents",
        "public_price_date",
        "price_match_level",
    )
    on_keys = [F.col(key).eqNullSafe(F.col(f"_ref_{key}")) for key in PRICE_KEYS]
    return lines.join(reference, on_keys, "left").drop(*(f"_ref_{key}" for key in PRICE_KEYS))


def _public_attributes(lakehouse: Lakehouse) -> DataFrame | None:
    if not lakehouse.exists("silver.ref_product_public"):
        return None
    return lakehouse.read("silver.ref_product_public").select(
        F.col("gtin14").alias("_public_gtin14"),
        F.col("brand_owner_resolved").alias("brand_owner"),
        "main_category",
        "nutriscore_grade",
        "nova_group",
    )


def sales_daily(lines: DataFrame, lakehouse: Lakehouse) -> DataFrame:
    """One row per tenant, store, product, currency and sale date."""
    days = lines.groupBy(*DAILY_KEYS).agg(
        F.count(F.lit(1)).alias("sale_lines"),
        F.sum(F.col("units_sold").cast("bigint")).alias("units_sold"),
        F.sum("gross_cents").alias("gross_revenue_cents"),
        F.sum("discount_cents").alias("discount_cents"),
        F.sum("net_cents").alias("net_revenue_cents"),
        F.sum(F.col("has_promo").cast("bigint")).alias("promo_sale_lines"),
        F.sum(F.when(F.col("has_promo"), F.col("discount_cents")).otherwise(0)).alias(
            "promo_discount_cents"
        ),
        F.sum("co_funded_discount_cents").alias("co_funded_discount_cents"),
        # Constant within the grain: same product (one master barcode) and store.
        F.first("gtin14", ignorenulls=True).alias("gtin14"),
        F.first("public_price_cents", ignorenulls=True).alias("public_price_cents"),
        F.first("public_price_date", ignorenulls=True).alias("public_price_date"),
        F.first("price_match_level", ignorenulls=True).alias("price_match_level"),
    )
    attributes = _public_attributes(lakehouse)
    if attributes is None:
        enriched = days.select(
            "*",
            *(F.lit(None).cast("string").alias(name) for name in PUBLIC_ATTRIBUTES[:3]),
            F.lit(None).cast("int").alias("nova_group"),
        )
    else:
        enriched = days.join(
            attributes, F.col("gtin14").eqNullSafe(F.col("_public_gtin14")), "left"
        ).drop("_public_gtin14")
    return enriched.select(
        *DAILY_KEYS,
        "sale_lines",
        "units_sold",
        "gross_revenue_cents",
        "discount_cents",
        "net_revenue_cents",
        "promo_sale_lines",
        "promo_discount_cents",
        "co_funded_discount_cents",
        "gtin14",
        *PUBLIC_ATTRIBUTES,
        "public_price_cents",
        "public_price_date",
        F.coalesce(F.col("price_match_level"), F.lit(NONE)).alias("price_match_level"),
    )


def promo_performance(lines: DataFrame) -> DataFrame:
    """One row per tenant, store, currency and promotion; NONE for lines without one."""
    grouped = lines.select(
        "*",
        F.coalesce(F.col("promo_campaign_id"), F.lit(NONE)).alias("_campaign"),
        F.coalesce(F.col("promo_mechanic_type"), F.lit(NONE)).alias("_mechanic"),
    ).groupBy("tenant_id", "location_id", "currency", "_campaign", "_mechanic")
    totals = grouped.agg(
        F.count(F.lit(1)).alias("sale_lines"),
        F.sum(F.col("units_sold").cast("bigint")).alias("units_sold"),
        F.sum("gross_cents").alias("gross_revenue_cents"),
        F.sum("net_cents").alias("net_revenue_cents"),
        F.sum("discount_cents").alias("discount_cents"),
        F.sum("co_funded_discount_cents").alias("co_funded_discount_cents"),
    )
    # Weighted: total discount over total gross revenue, half up to four places.
    average = F.when(
        F.col("gross_revenue_cents") != 0,
        F.round(
            F.col("discount_cents").cast("decimal(20,4)")
            / F.col("gross_revenue_cents").cast("decimal(20,4)"),
            4,
        ),
    ).cast("decimal(9,4)")
    return totals.select(
        "tenant_id",
        "location_id",
        "currency",
        F.col("_campaign").alias("promo_campaign_id"),
        F.col("_mechanic").alias("promo_mechanic_type"),
        "sale_lines",
        "units_sold",
        "gross_revenue_cents",
        "net_revenue_cents",
        "discount_cents",
        average.alias("avg_discount_rate"),
        "co_funded_discount_cents",
    )
