"""Column normalizers for silver, as Spark expressions (docs/prd.md, silver column rules).

Every parser returns null instead of failing when a value does not match, so the
caller can send the record to quarantine with a reason. Spark 4 runs in ANSI mode,
where a plain cast of a bad value raises; these use try_* functions instead.
"""

from __future__ import annotations

from pyspark.sql import Column
from pyspark.sql import functions as F

# Partner API event timestamps arrive in two formats:
#   ISO 8601 in UTC        2026-10-01T00:01:25Z
#   local with an offset   2026/09/30 18:01:20-0600
ISO_TIMESTAMP = "yyyy-MM-dd'T'HH:mm:ssXXX"
LOCAL_OFFSET_TIMESTAMP = "yyyy/MM/dd HH:mm:ssZ"

FRACTION = "decimal(5,4)"


def parse_timestamp(text: Column) -> Column:
    """Both source formats to the same UTC instant; null when neither matches."""
    trimmed = F.trim(text)
    return F.coalesce(
        F.try_to_timestamp(trimmed, F.lit(ISO_TIMESTAMP)),
        F.try_to_timestamp(trimmed, F.lit(LOCAL_OFFSET_TIMESTAMP)),
    )


def parse_fraction(text: Column) -> Column:
    """`"15%"` -> 0.15 and `0.15` -> 0.15, as an exact decimal between 0 and 1.
    Null when the value is not a number or falls outside that range."""
    trimmed = F.trim(text)
    percent = F.regexp_extract(trimmed, r"^(\d+(?:\.\d+)?)\s*%$", 1)
    value = F.when(percent != "", percent.try_cast("decimal(9,4)") / 100).otherwise(
        trimmed.try_cast("decimal(9,4)")
    )
    return F.when((value >= 0) & (value <= 1), value.cast(FRACTION))


def json_field(json_text: Column, field: str) -> Column:
    """A top-level JSON field as text: scalars as their value, objects and arrays as JSON."""
    return F.get_json_object(json_text, f"$.{field}")


def lowcode_attributes(json_text: Column) -> dict[str, Column]:
    """Typed columns from the low-code attributes, chosen by the shape of each value and
    never by tenant. The JSON itself is kept as text, so unknown keys are not lost.

    pack_size: 12 -> 12 units; "12x500ml" -> 12 units; an object such as
    {"inner": 6, "outer": 24} has no single unit count, so only the raw value is kept.
    """
    pack_size = json_field(json_text, "pack_size")
    shelf_life = F.trim(json_field(json_text, "shelf_life_days"))
    promo = F.lower(F.trim(json_field(json_text, "promo_eligible")))
    units = F.regexp_extract(pack_size, r"^\s*(\d+)\s*(?:[xX]|$)", 1)
    shelf_days = F.regexp_extract(shelf_life, r"^(\d+)\s*d?$", 1)
    return {
        "pack_size_units": F.when(units != "", units.try_cast("int")),
        "pack_size_raw": pack_size,
        "uom": json_field(json_text, "uom"),
        "shelf_life_days": F.when(shelf_days != "", shelf_days.try_cast("int")),
        "promo_eligible": F.when(promo.isin("true", "yes", "y", "1"), F.lit(True)).when(
            promo.isin("false", "no", "n", "0"), F.lit(False)
        ),
        "temp_zone": json_field(json_text, "temp_zone"),
        "custom_tags": F.from_json(json_field(json_text, "custom_tags"), "ARRAY<STRING>"),
    }
