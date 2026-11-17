"""Public reference data: ref_product_public and ref_public_price (scope GLOBAL, no tenant).

Both are recomputed whole from every bronze partition on each run, so the result does
not depend on which window ran last.

- ref_product_public, one row per GTIN-14: the version with the highest
  `last_modified_t` between the dump and the product API (the API on a tie), with each
  missing attribute filled from the other source. The brand owner is `brand_owner`,
  or the first value of `brands` when it is empty.
- ref_public_price, one row per Open Prices `id`: the version with the highest
  `updated`; prices marked as a duplicate of another are excluded. Amounts in cents.
  Only the listed fields are read, so no `owner*` field reaches silver.
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window as SparkWindow

from arkham.bronze.context import RunContext
from arkham.gtin import gtin14_column
from arkham.lakehouse import with_lineage
from arkham.runlog import FILTERED, READ, SUPERSEDED, WRITTEN

PRODUCTS = "silver.ref_product_public"
PRICES = "silver.ref_public_price"

ATTRIBUTES = (
    "product_name",
    "quantity",
    "brands",
    "brands_tags",
    "brand_owner",
    "categories_tags",
    "main_category",
    "nutriscore_grade",
    "nova_group",
)

PRODUCT_API_SCHEMA = (
    "status INT, product STRUCT<product_name: STRING, quantity: STRING, brands: STRING, "
    "brands_tags: ARRAY<STRING>, brand_owner: STRING, categories_tags: ARRAY<STRING>, "
    "main_category: STRING, nutriscore_grade: STRING, nova_group: STRING, "
    "last_modified_t: STRING>"
)

PRICE_SCHEMA = (
    "id BIGINT, product_code STRING, type STRING, price DECIMAL(12,4), currency STRING, "
    "price_is_discounted BOOLEAN, price_without_discount DECIMAL(12,4), discount_type STRING, "
    "price_per STRING, date STRING, created STRING, updated STRING, duplicate_of BIGINT, "
    "receipt_quantity DECIMAL(12,4), location_osm_id BIGINT, location_osm_type STRING, "
    "location STRUCT<osm_name: STRING, osm_address_city: STRING, osm_address_country: STRING, "
    "osm_address_country_code: STRING>, proof STRUCT<type: STRING>"
)


def _blank_to_null(column: Column) -> Column:
    return F.when(F.trim(column) != "", F.trim(column))


def _tags(column: Column) -> Column:
    """Dump tags are comma-separated text; the API sends arrays."""
    return F.when(_blank_to_null(column).isNotNull(), F.split(F.trim(column), r"\s*,\s*"))


def dump_candidates(ctx: RunContext) -> DataFrame | None:
    if not ctx.lakehouse.exists("bronze.off_products"):
        return None
    dump = ctx.lakehouse.read("bronze.off_products")

    def column(name: str) -> Column:
        # A column dropped from the configured dump columns is null, not an error.
        return F.col(name) if name in dump.columns else F.lit(None).cast("string")

    return dump.select(
        gtin14_column(F.col("code")).alias("gtin14"),
        _blank_to_null(column("product_name")).alias("product_name"),
        _blank_to_null(column("quantity")).alias("quantity"),
        _blank_to_null(column("brands")).alias("brands"),
        _tags(column("brands_tags")).alias("brands_tags"),
        _blank_to_null(column("brand_owner")).alias("brand_owner"),
        _tags(column("categories_tags")).alias("categories_tags"),
        _blank_to_null(column("main_category")).alias("main_category"),
        _blank_to_null(column("nutriscore_grade")).alias("nutriscore_grade"),
        column("nova_group").try_cast("int").alias("nova_group"),
        column("last_modified_t").try_cast("bigint").alias("last_modified_t"),
        F.lit("dump").alias("source"),
        F.col("_snapshot_date").cast("string").alias("_source_partition"),
    )


def api_candidates(ctx: RunContext) -> DataFrame | None:
    if not ctx.lakehouse.exists("bronze.off_product_api"):
        return None
    api = ctx.lakehouse.read("bronze.off_product_api")
    parsed = api.select(
        "code", F.from_json("raw_json", PRODUCT_API_SCHEMA).alias("r"), "_window_id"
    ).filter(F.col("r.status") == 1)
    product = F.col("r.product")
    return parsed.select(
        gtin14_column(F.col("code")).alias("gtin14"),
        _blank_to_null(product["product_name"]).alias("product_name"),
        _blank_to_null(product["quantity"]).alias("quantity"),
        _blank_to_null(product["brands"]).alias("brands"),
        product["brands_tags"].alias("brands_tags"),
        _blank_to_null(product["brand_owner"]).alias("brand_owner"),
        product["categories_tags"].alias("categories_tags"),
        _blank_to_null(product["main_category"]).alias("main_category"),
        _blank_to_null(product["nutriscore_grade"]).alias("nutriscore_grade"),
        product["nova_group"].try_cast("int").alias("nova_group"),
        product["last_modified_t"].try_cast("bigint").alias("last_modified_t"),
        F.lit("api").alias("source"),
        F.col("_window_id").alias("_source_partition"),
    )


def _best_per_gtin(candidates: DataFrame) -> DataFrame:
    order = SparkWindow.partitionBy("gtin14").orderBy(
        F.col("last_modified_t").desc_nulls_last(), F.col("_source_partition").desc()
    )
    return (
        candidates.withColumn("_rank", F.row_number().over(order)).filter("_rank = 1").drop("_rank")
    )


def products(ctx: RunContext) -> DataFrame | None:
    dump, api = dump_candidates(ctx), api_candidates(ctx)
    if dump is None and api is None:
        return None
    sides = {}
    for name, candidates in (("d", dump), ("a", api)):
        if candidates is not None:
            sides[name] = _best_per_gtin(candidates.filter(F.col("gtin14").isNotNull()))
    if len(sides) == 1:
        (only,) = sides.values()
        joined = only.select(
            "gtin14",
            *(F.col(c).alias(f"w_{c}") for c in (*ATTRIBUTES, "last_modified_t", "source")),
            *(F.lit(None).alias(f"l_{c}") for c in ATTRIBUTES),
        )
    else:
        d, a = sides["d"].alias("d"), sides["a"].alias("a")
        both = d.join(a, "gtin14", "full_outer")
        api_wins = F.col("d.last_modified_t").isNull() | (
            F.col("a.last_modified_t").isNotNull()
            & (F.col("a.last_modified_t") >= F.col("d.last_modified_t"))
        )

        def pick(column: str, winner: bool) -> Column:
            """The winning source's value, or the other source's for filling nulls."""
            first, second = ("a", "d") if winner else ("d", "a")
            return F.when(api_wins, F.col(f"{first}.{column}")).otherwise(
                F.col(f"{second}.{column}")
            )

        joined = both.select(
            "gtin14",
            *(pick(c, True).alias(f"w_{c}") for c in (*ATTRIBUTES, "last_modified_t", "source")),
            *(pick(c, False).alias(f"l_{c}") for c in ATTRIBUTES),
        )
    filled = {c: F.coalesce(F.col(f"w_{c}"), F.col(f"l_{c}")) for c in ATTRIBUTES}
    return joined.select(
        "gtin14",
        *(column.alias(name) for name, column in filled.items()),
        F.coalesce(filled["brand_owner"], F.trim(F.split(filled["brands"], ",")[0])).alias(
            "brand_owner_resolved"
        ),
        F.timestamp_seconds(F.col("w_last_modified_t")).alias("last_modified_at"),
        F.col("w_source").alias("source"),
        F.lit("bronze.off_products, bronze.off_product_api").alias("_source"),
    )


def prices(ctx: RunContext) -> tuple[DataFrame, int, int] | None:
    if not ctx.lakehouse.exists("bronze.open_prices"):
        return None
    raw = ctx.lakehouse.read("bronze.open_prices")
    parsed = raw.select(F.from_json("raw_json", PRICE_SCHEMA).alias("p"), "_window_id").select(
        "p.*", "_window_id"
    )
    latest = SparkWindow.partitionBy("id").orderBy(
        F.col("updated").desc(), F.col("_window_id").desc()
    )
    ranked = parsed.withColumn("_rank", F.row_number().over(latest))
    current = ranked.filter("_rank = 1")
    superseded = ranked.filter("_rank > 1").count()
    duplicates = current.filter(F.col("duplicate_of").isNotNull()).count()
    rows = current.filter(F.col("duplicate_of").isNull()).select(
        "id",
        "product_code",
        gtin14_column(F.col("product_code")).alias("gtin14"),
        F.col("type").alias("price_type"),
        F.round(F.col("price") * 100).cast("bigint").alias("price_cents"),
        F.round(F.col("price_without_discount") * 100)
        .cast("bigint")
        .alias("price_without_discount_cents"),
        "currency",
        "price_is_discounted",
        "discount_type",
        "price_per",
        "receipt_quantity",
        F.try_to_timestamp(F.col("date"), F.lit("yyyy-MM-dd")).cast("date").alias("price_date"),
        F.try_to_timestamp(F.col("created")).alias("created_at"),
        F.try_to_timestamp(F.col("updated")).alias("updated_at"),
        "location_osm_id",
        "location_osm_type",
        F.col("location.osm_name").alias("location_name"),
        F.col("location.osm_address_city").alias("location_city"),
        F.col("location.osm_address_country").alias("location_country"),
        F.upper(F.col("location.osm_address_country_code")).alias("location_country_code"),
        F.col("proof.type").alias("proof_type"),
        F.col("_window_id").alias("source_partition"),
        F.lit("bronze.open_prices").alias("_source"),
    )
    return rows, superseded, duplicates


def load(ctx: RunContext) -> None:
    product_rows = products(ctx)
    if product_rows is not None:
        ctx.lakehouse.merge(
            PRODUCTS, with_lineage(product_rows, ctx.run_id, ctx.loaded_at), ["gtin14"], True
        )
        written = ctx.lakehouse.read(PRODUCTS).count()
        ctx.record.count(PRODUCTS, WRITTEN, written)
    price_result = prices(ctx)
    if price_result is not None:
        rows, superseded, duplicates = price_result
        ctx.lakehouse.merge(PRICES, with_lineage(rows, ctx.run_id, ctx.loaded_at), ["id"], True)
        written = ctx.lakehouse.read(PRICES).count()
        ctx.record.count(PRICES, READ, written + superseded + duplicates)
        ctx.record.count(PRICES, SUPERSEDED, superseded, "older version of the same price id")
        ctx.record.count(PRICES, FILTERED, duplicates, "marked duplicate_of another price")
        ctx.record.count(PRICES, WRITTEN, written)
