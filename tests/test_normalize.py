from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from arkham.silver.normalize import lowcode_attributes, parse_fraction, parse_timestamp


def _utc(row_value: datetime | None) -> datetime | None:
    # collect() returns naive datetimes in the session time zone, which is UTC.
    return row_value.replace(tzinfo=UTC) if row_value else None


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        # Event 5 of the partner API: the local format must land on the same UTC instant.
        ("2026/09/30 18:01:20-0600", datetime(2026, 10, 1, 0, 1, 20, tzinfo=UTC)),
        ("2026-10-01T00:01:25Z", datetime(2026, 10, 1, 0, 1, 25, tzinfo=UTC)),
        ("2026-10-01T02:01:25+02:00", datetime(2026, 10, 1, 0, 1, 25, tzinfo=UTC)),
        (" 2026-10-01T00:01:25Z ", datetime(2026, 10, 1, 0, 1, 25, tzinfo=UTC)),
        ("01/10/2026 00:01", None),
        ("", None),
        (None, None),
    ],
)
def test_parse_timestamp(spark: SparkSession, text: str | None, expected: datetime | None) -> None:
    df = spark.createDataFrame([(text,)], "t STRING")
    assert _utc(df.select(parse_timestamp(F.col("t")).alias("ts")).first().ts) == expected


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("15%", Decimal("0.1500")),
        ("0%", Decimal("0.0000")),
        ("12.5 %", Decimal("0.1250")),
        ("0.19", Decimal("0.1900")),
        ("0", Decimal("0.0000")),
        ("1", Decimal("1.0000")),
        ("150%", None),
        ("-0.1", None),
        ("abc", None),
        (None, None),
    ],
)
def test_parse_fraction(spark: SparkSession, text: str | None, expected: Decimal | None) -> None:
    df = spark.createDataFrame([(text,)], "t STRING")
    assert df.select(parse_fraction(F.col("t")).alias("v")).first().v == expected


def test_lowcode_attributes_by_value_shape(spark: SparkSession) -> None:
    # One shape per ERP tenant (docs/prd.md: 4 variants), plus an unknown key.
    rows = [
        ('{"uom": "EA", "pack_size": 12, "promo_eligible": true, "shelf_life_days": 180}',),
        (
            '{"uom": "CASE", "pack_size": "12x500ml", "promo_eligible": "yes", '
            '"shelf_life_days": "180d"}',
        ),
        (
            '{"uom": "PALLET_LAYER", "pack_size": {"inner": 6, "outer": 24}, '
            '"temp_zone": "AMBIENT"}',
        ),
        ('{"pack_size": null, "custom_tags": ["fast-mover", "endcap"], "new_key": 1}',),
    ]
    df = spark.createDataFrame(rows, "attrs STRING")
    columns = lowcode_attributes(F.col("attrs"))
    result = [
        row.asDict() for row in df.select(*(c.alias(n) for n, c in columns.items())).collect()
    ]

    assert [r["pack_size_units"] for r in result] == [12, 12, None, None]
    assert [r["pack_size_raw"] for r in result] == [
        "12",
        "12x500ml",
        '{"inner":6,"outer":24}',
        None,
    ]
    assert [r["uom"] for r in result] == ["EA", "CASE", "PALLET_LAYER", None]
    assert [r["shelf_life_days"] for r in result] == [180, 180, None, None]
    assert [r["promo_eligible"] for r in result] == [True, True, None, None]
    assert [r["temp_zone"] for r in result] == [None, None, "AMBIENT", None]
    assert [r["custom_tags"] for r in result] == [None, None, None, ["fast-mover", "endcap"]]
