from __future__ import annotations

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from arkham.gtin import (
    INVALID,
    INVALID_CHECK_DIGIT,
    TOO_LONG,
    VALID,
    gtin14_column,
    gtin_status,
    to_gtin14,
    to_off_code,
)

# Barcode variants found in the ERP and the partner API (docs/prd.md).
CASES = [
    ("3017620422003", "03017620422003", VALID),
    ("03017620422003", "03017620422003", VALID),
    (" 5449000000996 ", "05449000000996", VALID),
    ("GTIN:7622210449283", "07622210449283", VALID),
    ("0028400090896", "00028400090896", VALID),
    ("028400090896", "00028400090896", VALID),
    ("00049000028904", "00049000028904", VALID),
    ("003017620422003", "03017620422003", VALID),  # 15 characters, only extra zeros
    ("3017620422004", "03017620422004", INVALID_CHECK_DIGIT),
    ("123456789012345", None, TOO_LONG),
    ("", None, INVALID),
    ("GTIN:", None, INVALID),
    (None, None, INVALID),
]


@pytest.mark.parametrize(("raw", "gtin14", "status"), CASES)
def test_python_normalization(raw: str | None, gtin14: str | None, status: str) -> None:
    assert to_gtin14(raw) == gtin14
    assert gtin_status(raw) == status


def test_spark_expression_matches_python(spark: SparkSession) -> None:
    df = spark.createDataFrame([(raw,) for raw, _, _ in CASES], "raw STRING")
    result = [
        row.gtin14 for row in df.select(gtin14_column(F.col("raw")).alias("gtin14")).collect()
    ]
    assert result == [gtin14 for _, gtin14, _ in CASES]


def test_off_code_is_ean13_for_short_codes() -> None:
    assert to_off_code("00028400090896") == "0028400090896"
    assert to_off_code("03017620422003") == "3017620422003"
    assert to_off_code("12345678901234") == "12345678901234"
