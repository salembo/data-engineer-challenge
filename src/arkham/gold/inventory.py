"""Inventory gold: net position per store and product, and daily movements with velocity.

Rules (the v2-v4 PRD, inventory):
- only posted ledger entries count; entries flagged for a reused document count too;
- `quantity_delta` keeps its sign: positive is inbound, negative is outbound;
- the net position is the change since the first window, so it can be negative;
- velocity is per period, a period being the date range of a calendar window: the
  outbound units of the period divided by its days, read from the calendar. A movement
  counts in the period of its business date, not of its arrival;
- stores in RENOVATION_HOLD are included, with `operating_status` as a column.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, StringType, StructField, StructType, TimestampType

from arkham.config import WindowCalendar
from arkham.gold.common import UNKNOWN, without_sandbox
from arkham.lakehouse import Lakehouse, jvm_frame

POSITION = "gold.inventory_position"
DAILY = "gold.inventory_daily"
POSITION_KEYS = ["tenant_id", "location_id", "product_id"]
DAILY_KEYS = ["tenant_id", "location_id", "category_code", "movement_date"]
INPUTS = ("silver.fact_inventory_movement", "silver.dim_product", "silver.dim_location")

PERIOD_SCHEMA = StructType(
    [
        StructField("period_id", StringType()),
        StructField("period_start", TimestampType()),
        StructField("period_end", TimestampType()),
        StructField("period_days", DecimalType(10, 4)),
    ]
)


def posted_movements(lakehouse: Lakehouse) -> DataFrame:
    """Posted entries of the real tenants, with the product's category."""
    movements = lakehouse.read("silver.fact_inventory_movement").filter(F.col("is_posted"))
    categories = lakehouse.read("silver.dim_product").select("product_id", "category_code")
    return (
        without_sandbox(movements, lakehouse)
        .join(categories, "product_id", "left")
        .withColumn("category_code", F.coalesce(F.col("category_code"), F.lit(UNKNOWN)))
        .withColumn("movement_date", F.to_date("event_occurred_at"))
    )


def _inbound(quantity: F.Column) -> F.Column:
    return F.when(quantity > 0, quantity).otherwise(F.lit(0))


def _outbound(quantity: F.Column) -> F.Column:
    return F.when(quantity < 0, -quantity).otherwise(F.lit(0))


def position(movements: DataFrame, locations: DataFrame) -> DataFrame:
    quantity = F.col("quantity_delta").cast("bigint")
    totals = movements.groupBy(*POSITION_KEYS, "category_code").agg(
        F.sum(_inbound(quantity)).alias("inbound_units"),
        F.sum(_outbound(quantity)).alias("outbound_units"),
        F.sum(quantity).alias("net_position_units"),
        F.count(F.lit(1)).alias("movement_count"),
        F.max("event_occurred_at").alias("last_movement_at"),
    )
    return totals.join(
        locations.select("location_id", "operating_status"), "location_id", "left"
    ).select(
        *POSITION_KEYS,
        "category_code",
        "operating_status",
        "inbound_units",
        "outbound_units",
        "net_position_units",
        "movement_count",
        "last_movement_at",
    )


def periods(spark: SparkSession, calendar: WindowCalendar) -> DataFrame:
    """One period per calendar window; its length in days comes from the calendar."""
    rows = [
        (
            window.id,
            window.start,
            window.end,
            round((window.end - window.start).total_seconds() / 86_400, 4),
        )
        for window in calendar.windows
    ]
    return jvm_frame(spark, rows, PERIOD_SCHEMA)


def daily(movements: DataFrame, calendar_periods: DataFrame) -> DataFrame:
    """Movements per business day, each day carrying its period's velocity. A day belongs
    to the period that contains its first instant; outside every period both are null."""
    day_start = F.col("movement_date").cast("timestamp")
    with_period = movements.join(
        F.broadcast(calendar_periods),
        (day_start >= F.col("period_start")) & (day_start < F.col("period_end")),
        "left",
    )
    quantity = F.col("quantity_delta").cast("bigint")
    days = with_period.groupBy(*DAILY_KEYS, "period_id", "period_days").agg(
        F.sum(_inbound(quantity)).alias("inbound_units"),
        F.sum(_outbound(quantity)).alias("outbound_units"),
        F.sum(quantity).alias("net_units"),
        F.count(F.lit(1)).alias("movement_count"),
    )
    velocity = (
        days.filter(F.col("period_id").isNotNull())
        .groupBy("tenant_id", "location_id", "category_code", "period_id", "period_days")
        .agg(F.sum("outbound_units").alias("_period_outbound"))
        .select(
            "tenant_id",
            "location_id",
            "category_code",
            "period_id",
            # Decimal division, rounded half up to four places.
            F.round(F.col("_period_outbound").cast("decimal(18,4)") / F.col("period_days"), 4)
            .cast("decimal(18,4)")
            .alias("velocity_units_per_day"),
        )
    )
    return days.join(
        velocity, ["tenant_id", "location_id", "category_code", "period_id"], "left"
    ).select(
        *DAILY_KEYS,
        "inbound_units",
        "outbound_units",
        "net_units",
        "movement_count",
        "period_id",
        "velocity_units_per_day",
    )
