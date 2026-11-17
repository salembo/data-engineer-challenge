"""Captures the committed fallback fixtures for the public APIs.

The pipeline uses them when Open Food Facts or Open Prices fail after every retry
and no earlier landed response exists (docs/prd.md, NFR-7). Run it inside the
pipeline container, with the ERP up, to refresh them:

    make capture-fallback

Every `owner*` field (the Open Prices username, also nested in `proof`, and the
user's free-text comments) is removed before writing: personal data never goes
into the repository.
"""

from __future__ import annotations

import json
import os
from datetime import UTC, datetime
from pathlib import Path

from arkham.bronze.public_apis import erp_barcodes
from arkham.config import load_config
from arkham.erp import JdbcErpSource
from arkham.http import HttpClient
from arkham.landing import one_line
from arkham.settings import ErpConnection
from arkham.spark import build_spark_session

PERSONAL_FIELD_PREFIX = "owner"


def without_personal_fields(value: object) -> object:
    """Drops every key starting with `owner`, at any depth."""
    if isinstance(value, dict):
        return {
            key: without_personal_fields(item)
            for key, item in value.items()
            if not key.startswith(PERSONAL_FIELD_PREFIX)
        }
    if isinstance(value, list):
        return [without_personal_fields(item) for item in value]
    return value


def main() -> None:
    config = load_config()
    root = Path(os.environ.get("ARKHAM_FALLBACK_ROOT", "fallback"))
    http = config.public_http
    client = HttpClient(
        user_agent=http.user_agent,
        timeout_seconds=http.timeout_seconds,
        max_retries=http.max_retries,
        min_interval_seconds=http.min_interval_seconds,
    )
    spark = build_spark_session("arkham-capture-fallback")
    try:
        products = JdbcErpSource(spark, ErpConnection.from_env()).read_table("arkham_erp.products")
        codes = erp_barcodes(products)
    finally:
        spark.stop()
    captured_at = datetime.now(UTC).isoformat()

    product_api = config.product_api
    bodies = [
        client.get_json(f"{product_api.url}/{code}.json", {"fields": ",".join(product_api.fields)})[
            0
        ]
        for code in codes
    ]
    product_dir = root / "off_product_api"
    product_dir.mkdir(parents=True, exist_ok=True)
    (product_dir / "responses.jsonl").write_bytes(b"".join(one_line(b) + b"\n" for b in bodies))
    (product_dir / "_manifest.json").write_text(
        json.dumps(
            {"captured_at": captured_at, "codes": codes, "source": product_api.url}, indent=2
        )
    )

    # Prices for the baseline (by price date) and for the whole calendar (by creation time).
    prices = config.open_prices
    baseline_start, baseline_end = config.baseline_range()
    calendar = config.calendar
    filters = [
        {"date__gte": baseline_start.isoformat(), "date__lt": baseline_end.isoformat()},
        {
            "created__gte": calendar.first.start.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "created__lt": calendar.windows[-1].end.strftime("%Y-%m-%dT%H:%M:%SZ"),
        },
    ]
    items: dict[int, dict[str, object]] = {}
    for extra in filters:
        page = 1
        while True:
            _, payload = client.get_json(
                prices.url,
                {
                    "product_code__in": ",".join(codes),
                    "type": prices.price_type,
                    "order_by": "id",
                    "size": prices.page_size,
                    "page": page,
                    **extra,
                },
            )
            for item in payload["items"]:
                items[item["id"]] = without_personal_fields(item)
            if not payload["items"] or page >= payload["pages"]:
                break
            page += 1
    prices_dir = root / "open_prices"
    prices_dir.mkdir(parents=True, exist_ok=True)
    (prices_dir / "prices.json").write_text(
        json.dumps(
            {
                "captured_at": captured_at,
                "codes": codes,
                "filters": filters,
                "removed_fields": f"keys starting with '{PERSONAL_FIELD_PREFIX}', at any depth",
                "items": [items[key] for key in sorted(items)],
            },
            indent=1,
        )
    )
    print(f"Captured {len(bodies)} product responses and {len(items)} prices into {root}.")


if __name__ == "__main__":
    main()
