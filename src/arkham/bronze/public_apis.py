"""Open Food Facts product API and Open Prices to bronze.

- Product API: a snapshot of the ERP barcodes per window, one request per code.
- Open Prices: prices of the ERP barcodes created within the window, plus a
  baseline partition with the prices dated in the months before the first window.

Both go through the public HTTP client (custom User-Agent, one request per
second, three retries). If the source still fails, the run falls back to an
earlier landed response, then to the fixture committed under fallback/, then
to an empty partition, and records which one it used (docs/prd.md, NFR-7).
A landed partition is reused on re-runs, so a re-run makes no HTTP calls.
"""

from __future__ import annotations

import json
from datetime import UTC, date, datetime
from typing import Any

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from arkham.bronze.context import RunContext
from arkham.config import Window
from arkham.gtin import gtin14_column, to_off_code
from arkham.http import HttpClient, HttpError
from arkham.landing import RESPONSES
from arkham.runlog import READ, WRITTEN

PARTITION_KEY = "window_id"
BASELINE = "baseline"

PRODUCT_SOURCE = "off_product_api"
PRODUCT_TABLE = "bronze.off_product_api"
PRICES_SOURCE = "open_prices"
PRICES_TABLE = "bronze.open_prices"


def erp_barcodes(products: DataFrame) -> list[str]:
    """Distinct ERP barcodes in the format the public APIs use (EAN-13). Small by
    construction: one value per real product code."""
    codes = (
        products.select(gtin14_column(F.col("gtin_barcode")).alias("gtin14"))
        .where(F.col("gtin14").isNotNull())
        .distinct()
        .collect()
    )
    return sorted(to_off_code(row.gtin14) for row in codes)


def _now() -> str:
    return datetime.now(UTC).isoformat()


def _land_with_fallback(
    ctx: RunContext,
    source: str,
    partition: str,
    fetch: Any,
    fallback_responses: Any,
) -> dict[str, Any]:
    """Reuses a landed partition, or fetches it; on failure lands the fallback instead."""
    manifest = ctx.landing.read_manifest(source, PARTITION_KEY, partition)
    if manifest is None:
        try:
            responses, manifest = fetch()
            manifest["fallback"] = None
        except HttpError as error:
            responses, fallback = fallback_responses()
            manifest = {"fallback": fallback, "error": str(error), "extracted_at": _now()}
        ctx.landing.write_partition(source, PARTITION_KEY, partition, manifest, responses=responses)
    if manifest.get("fallback"):
        ctx.record.fallback(f"{source}[{partition}]: {manifest['fallback']}")
    return manifest


# ---- Open Food Facts product API ----


def fetch_products(
    ctx: RunContext, client: HttpClient, codes: list[str]
) -> tuple[list[bytes], dict[str, Any]]:
    settings = ctx.config.product_api
    params = {"fields": ",".join(settings.fields)}
    responses = [client.get_json(f"{settings.url}/{code}.json", params)[0] for code in codes]
    manifest = {
        "source": f"GET {settings.url}/{{code}}.json",
        "codes": codes,
        "fields": list(settings.fields),
        "responses": len(responses),
        "extracted_at": _now(),
    }
    return responses, manifest


def product_fallback(ctx: RunContext, window: Window) -> tuple[list[bytes], str]:
    for earlier in reversed(ctx.config.calendar.before(window.id)):
        manifest = ctx.landing.read_manifest(PRODUCT_SOURCE, PARTITION_KEY, earlier.id)
        if manifest is not None and not manifest.get("fallback"):
            responses = ctx.landing.read_responses(PRODUCT_SOURCE, PARTITION_KEY, earlier.id)
            return responses, f"landed snapshot of {earlier.id} ({manifest['extracted_at']})"
    fixture = ctx.fallback_root / PRODUCT_SOURCE
    if (fixture / RESPONSES).exists():
        captured = json.loads((fixture / "_manifest.json").read_text())["captured_at"]
        lines = (fixture / RESPONSES).read_bytes().splitlines()
        return [line for line in lines if line], f"committed fixture (captured {captured})"
    return [], "empty: no landed snapshot and no fixture"


def products_from_responses(ctx: RunContext, partition: str) -> DataFrame:
    path = ctx.landing.partition_dir(PRODUCT_SOURCE, PARTITION_KEY, partition) / RESPONSES
    raw = F.col("raw_json")
    return ctx.spark.read.text(str(path)).select(
        F.col("value").alias("raw_json"),
        F.get_json_object(raw, "$.code").alias("code"),
        F.get_json_object(raw, "$.status").cast("int").alias("status"),
        F.get_json_object(raw, "$.product.created_t").alias("created_t"),
        F.get_json_object(raw, "$.product.last_modified_t").alias("last_modified_t"),
    )


def load_products(ctx: RunContext, client: HttpClient, window: Window, codes: list[str]) -> None:
    manifest = _land_with_fallback(
        ctx,
        PRODUCT_SOURCE,
        window.id,
        lambda: fetch_products(ctx, client, codes),
        lambda: product_fallback(ctx, window),
    )
    rows = ctx.write_bronze(
        PRODUCT_TABLE,
        products_from_responses(ctx, window.id),
        window.id,
        f"open_food_facts:{manifest.get('source', 'fallback')}",
        window.id,
    )
    ctx.record.count(PRODUCT_TABLE, READ, rows)
    ctx.record.count(PRODUCT_TABLE, WRITTEN, rows)


# ---- Open Prices ----


def fetch_prices(
    ctx: RunContext, client: HttpClient, codes: list[str], filters: dict[str, str]
) -> tuple[list[bytes], dict[str, Any]]:
    settings = ctx.config.open_prices
    responses: list[bytes] = []
    page = 1
    total = 0
    while True:
        body, payload = client.get_json(
            settings.url,
            {
                "product_code__in": ",".join(codes),
                "type": settings.price_type,
                "order_by": "id",  # stable order across pages
                "size": settings.page_size,
                "page": page,
                **filters,
            },
        )
        responses.append(body)
        total = int(payload.get("total") or 0)
        if not payload.get("items") or page >= int(payload.get("pages") or 1):
            break
        page += 1
    manifest = {
        "source": f"GET {settings.url}",
        "codes": codes,
        "filters": filters,
        "total": total,
        "responses": len(responses),
        "extracted_at": _now(),
    }
    return responses, manifest


def prices_fallback(ctx: RunContext, keep: Any) -> tuple[list[bytes], str]:
    fixture = ctx.fallback_root / PRICES_SOURCE / "prices.json"
    if not fixture.exists():
        return [], "empty: no fixture"
    content = json.loads(fixture.read_text())
    items = [item for item in content["items"] if keep(item)]
    body = json.dumps({"items": items, "total": len(items), "page": 1, "pages": 1}).encode()
    return [body], f"committed fixture (captured {content['captured_at']})"


def prices_from_responses(ctx: RunContext, partition: str) -> DataFrame:
    path = ctx.landing.partition_dir(PRICES_SOURCE, PARTITION_KEY, partition) / RESPONSES
    raw = F.col("raw_json")
    return (
        ctx.spark.read.text(str(path))
        .select(F.explode(F.from_json("value", "items ARRAY<STRING>")["items"]).alias("raw_json"))
        .select(
            "raw_json",
            F.get_json_object(raw, "$.id").cast("bigint").alias("id"),
            F.get_json_object(raw, "$.created").alias("created"),
        )
    )


def _load_prices(
    ctx: RunContext,
    client: HttpClient,
    partition: str,
    window_id: str,
    codes: list[str],
    filters: dict[str, str],
    keep: Any,
) -> None:
    manifest = _land_with_fallback(
        ctx,
        PRICES_SOURCE,
        partition,
        lambda: fetch_prices(ctx, client, codes, filters),
        lambda: prices_fallback(ctx, keep),
    )
    rows = ctx.write_bronze(
        PRICES_TABLE,
        prices_from_responses(ctx, partition),
        partition,
        f"open_prices:{manifest.get('source', 'fallback')}",
        window_id,
    )
    ctx.record.count(PRICES_TABLE, READ, rows)
    ctx.record.count(PRICES_TABLE, WRITTEN, rows)


def _utc_text(instant: datetime) -> str:
    return instant.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def load_window_prices(
    ctx: RunContext, client: HttpClient, window: Window, codes: list[str]
) -> None:
    """Prices created within the window."""
    _load_prices(
        ctx,
        client,
        window.id,
        window.id,
        codes,
        {"created__gte": _utc_text(window.start), "created__lt": _utc_text(window.end)},
        lambda item: window.contains(datetime.fromisoformat(item["created"])),
    )


def load_baseline_prices(ctx: RunContext, client: HttpClient, codes: list[str]) -> None:
    """Prices dated in the configured months before the first window."""
    start, end = ctx.config.baseline_range()
    _load_prices(
        ctx,
        client,
        BASELINE,
        BASELINE,
        codes,
        {"date__gte": start.isoformat(), "date__lt": end.isoformat()},
        lambda item: start <= date.fromisoformat(item["date"]) < end,
    )
