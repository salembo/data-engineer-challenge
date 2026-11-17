"""Barcode normalization to GTIN-14, in plain Python and as Spark column expressions.

Rules (docs/prd.md, silver column rules):
- keep digits only;
- strip leading zeros to measure the significant length;
- 14 significant digits or fewer: left-pad with zeros to exactly 14;
- more than 14: no GTIN-14 (status TOO_LONG); the raw value is kept elsewhere;
- empty or no digits at all: INVALID;
- the check digit is validated and reported, never used to reject.
"""

from __future__ import annotations

import re

from pyspark.sql import Column
from pyspark.sql import functions as F

VALID = "VALID"
INVALID_CHECK_DIGIT = "INVALID_CHECK_DIGIT"
TOO_LONG = "TOO_LONG"
INVALID = "INVALID"

_NON_DIGITS = re.compile(r"\D")


def _significant_digits(raw: str | None) -> str | None:
    if raw is None:
        return None
    digits = _NON_DIGITS.sub("", raw)
    if not digits:
        return None
    return digits.lstrip("0") or "0"


def to_gtin14(raw: str | None) -> str | None:
    significant = _significant_digits(raw)
    if significant is None or len(significant) > 14:
        return None
    return significant.zfill(14)


def has_valid_check_digit(gtin14: str) -> bool:
    """GS1 mod-10: weights 3 and 1 alternate from the digit next to the check digit."""
    body, check = gtin14[:-1], int(gtin14[-1])
    total = sum(int(digit) * (3 if index % 2 == 0 else 1) for index, digit in enumerate(body))
    return (10 - total % 10) % 10 == check


def gtin_status(raw: str | None) -> str:
    significant = _significant_digits(raw)
    if significant is None:
        return INVALID
    if len(significant) > 14:
        return TOO_LONG
    return VALID if has_valid_check_digit(significant.zfill(14)) else INVALID_CHECK_DIGIT


def to_off_code(gtin14: str) -> str:
    """Code format used by the Open Food Facts and Open Prices APIs: EAN-13, or GTIN-14
    when the code really has 14 significant digits."""
    significant = gtin14.lstrip("0") or "0"
    return significant.zfill(13) if len(significant) <= 13 else significant


def gtin_status_column(raw: Column) -> Column:
    """Spark equivalent of gtin_status."""
    digits = F.regexp_replace(raw, r"\D", "")
    significant = F.regexp_replace(digits, r"^0+(?=\d)", "")
    padded = F.lpad(significant, 14, "0")
    # GS1 mod-10 over the 13 body digits: weight 3 on even positions, 1 on odd ones.
    weighted = sum(
        (F.substring(padded, position + 1, 1).cast("int") * (3 if position % 2 == 0 else 1))
        for position in range(13)
    )
    check = F.substring(padded, 14, 1).cast("int")
    return (
        F.when(raw.isNull() | (digits == ""), F.lit(INVALID))
        .when(F.length(significant) > 14, F.lit(TOO_LONG))
        .when((10 - weighted % 10) % 10 == check, F.lit(VALID))
        .otherwise(F.lit(INVALID_CHECK_DIGIT))
    )


def gtin14_column(raw: Column) -> Column:
    """Spark equivalent of to_gtin14."""
    digits = F.regexp_replace(raw, r"\D", "")
    significant = F.regexp_replace(digits, r"^0+(?=\d)", "")
    return F.when(
        (digits == "") | (F.length(significant) > 14), F.lit(None).cast("string")
    ).otherwise(F.lpad(significant, 14, "0"))
