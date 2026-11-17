"""What gold, and the graph built after it, share: grain placeholders and the sandbox filter.
Tables are written with arkham.publish (recompute, then synchronise)."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from arkham.lakehouse import Lakehouse
from arkham.publish import publish, stable

__all__ = ["GLOBAL", "NONE", "UNKNOWN", "publish", "sandbox_tenants", "stable", "without_sandbox"]

# A grain column never holds a null (the v2-v4 PRD): a sale line with no promotion is
# NONE, and a value the dimensions do not know is UNKNOWN.
NONE = "NONE"
UNKNOWN = "UNKNOWN"
# The tenant of the health rows that describe a public source.
GLOBAL = "GLOBAL"


def sandbox_tenants(lakehouse: Lakehouse) -> DataFrame:
    """Tenants whose workspace is a sandbox, found by `workspace_env` and never by ID."""
    return lakehouse.read("silver.dim_tenant").filter(F.col("is_sandbox")).select("tenant_id")


def without_sandbox(df: DataFrame, lakehouse: Lakehouse) -> DataFrame:
    """Silver facts already exclude the sandbox; business gold enforces it again."""
    return df.join(sandbox_tenants(lakehouse), "tenant_id", "left_anti")
