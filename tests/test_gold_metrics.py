"""Gold metric rules on small hand-made inputs (the v2-v4 PRD, v2 metric rules)."""

from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal

from pyspark.sql import SparkSession

from arkham.config import Window, WindowCalendar
from arkham.gold import inventory, sales


def test_line_amounts_round_half_up_per_line(spark: SparkSession) -> None:
    lines = spark.createDataFrame(
        [
            ("half", 1, 301, Decimal("0.5"), None, None),  # 150.5 -> 151, not 150
            ("promo", 2, 300, Decimal("0.15"), "PRM-1", Decimal("0.3")),
            ("cents", 1, 333, Decimal("0.15"), "PRM-2", Decimal("0.5")),  # 283.05 -> 283
            ("cofund", 1, 101, Decimal("0.15"), "PRM-3", Decimal("0.5")),  # 7.5 -> 8
        ],
        "line STRING, units_sold INT, unit_price_cents BIGINT, discount_rate DECIMAL(5,4), "
        "promo_campaign_id STRING, promo_co_funded_ratio DECIMAL(5,4)",
    )
    result = {
        row.line: (row.gross_cents, row.net_cents, row.discount_cents, row.co_funded_discount_cents)
        for row in sales.line_amounts(lines).collect()
    }
    assert result == {
        "half": (301, 151, 150, 0),
        "promo": (600, 510, 90, 27),
        "cents": (333, 283, 50, 25),
        "cofund": (101, 86, 15, 8),
    }


def test_price_reference_prefers_country_then_recency_then_tie_breaks(
    spark: SparkSession,
) -> None:
    sale = date(2026, 10, 10)
    keys = spark.createDataFrame(
        [
            ("G1", "EUR", "FR", sale),
            ("G1", "EUR", "ES", sale),
            ("G1", "USD", "US", sale),
            ("G2", "EUR", "FR", sale),
            ("G3", "EUR", "FR", sale),
        ],
        "gtin14 STRING, currency STRING, country_code STRING, sale_date DATE",
    )

    def at(month: int, day: int) -> datetime:
        return datetime(2026, month, day, tzinfo=UTC)

    prices = spark.createDataFrame(
        [
            (1, "G1", "EUR", "FR", date(2026, 9, 1), at(9, 1), 400),  # same country
            (2, "G1", "EUR", "DE", date(2026, 10, 5), at(10, 5), 450),  # newer, other country
            (3, "G1", "EUR", "FR", date(2026, 3, 1), at(3, 1), 300),  # older than six months
            (4, "G1", "EUR", "FR", date(2026, 10, 12), at(10, 12), 500),  # after the sale
            (5, "G2", "EUR", "FR", date(2026, 10, 1), at(10, 3), 100),  # same day, updated later
            (6, "G2", "EUR", "FR", date(2026, 10, 1), at(10, 2), 200),
            (7, "G3", "EUR", "FR", date(2026, 10, 1), at(10, 1), 700),  # full tie: highest id
            (8, "G3", "EUR", "FR", date(2026, 10, 1), at(10, 1), 800),
        ],
        "id BIGINT, gtin14 STRING, currency STRING, location_country_code STRING, "
        "price_date DATE, updated_at TIMESTAMP, price_cents BIGINT",
    )
    result = {
        (row.gtin14, row.currency, row.country_code): (
            row.price_match_level,
            row.public_price_cents,
            row.public_price_date,
        )
        for row in sales.price_reference(keys, prices, 6).collect()
    }
    assert result == {
        ("G1", "EUR", "FR"): ("COUNTRY", 400, date(2026, 9, 1)),
        ("G1", "EUR", "ES"): ("CURRENCY", 450, date(2026, 10, 5)),
        ("G1", "USD", "US"): ("NONE", None, None),
        ("G2", "EUR", "FR"): ("COUNTRY", 100, date(2026, 10, 1)),
        ("G3", "EUR", "FR"): ("COUNTRY", 800, date(2026, 10, 1)),
    }


def test_velocity_is_per_calendar_period_and_uses_its_length(spark: SparkSession) -> None:
    calendar = WindowCalendar(
        [
            Window("P1", datetime(2026, 10, 1, tzinfo=UTC), datetime(2026, 10, 3, tzinfo=UTC)),
            Window("P2", datetime(2026, 10, 3, tzinfo=UTC), datetime(2026, 10, 6, tzinfo=UTC)),
        ]
    )

    def movement(day: int, month: int, quantity: int) -> tuple[object, ...]:
        occurred = datetime(2026, month, day, 12, tzinfo=UTC)
        return ("T", "S", "SNACKS", "P", quantity, occurred, occurred.date())

    movements = spark.createDataFrame(
        [
            movement(1, 10, -4),
            movement(2, 10, -2),  # P1: 6 outbound units over 2 days
            movement(4, 10, -3),  # P2: 3 outbound units over 3 days
            movement(5, 10, 5),  # inbound, not part of velocity
            movement(30, 9, -1),  # before the calendar: no period
        ],
        "tenant_id STRING, location_id STRING, category_code STRING, product_id STRING, "
        "quantity_delta INT, event_occurred_at TIMESTAMP, movement_date DATE",
    )
    days = {
        row.movement_date.day: (row.period_id, row.velocity_units_per_day, row.net_units)
        for row in inventory.daily(movements, inventory.periods(spark, calendar)).collect()
    }
    assert days == {
        1: ("P1", Decimal("3.0000"), -4),
        2: ("P1", Decimal("3.0000"), -2),
        4: ("P2", Decimal("1.0000"), -3),
        5: ("P2", Decimal("1.0000"), 5),
        30: (None, None, -1),
    }
