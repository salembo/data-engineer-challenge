"""In-memory stand-ins for the sources, so tests never touch the network.

They follow the real contracts profiled in docs/prd.md: the partner API pages by
since_cursor, the product API answers one code per request, and Open Prices
filters by creation time or price date and pages by page/size.
"""

from __future__ import annotations

import json
import shutil
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from arkham.config import (
    DumpSettings,
    HttpSettings,
    OpenPricesSettings,
    PipelineConfig,
    ProductApiSettings,
    Window,
    WindowCalendar,
)
from arkham.http import HttpError
from arkham.pipeline import Pipeline, Sources

UTC_START = datetime(2026, 10, 1, tzinfo=UTC)


def utc(day: int, hour: int = 0) -> datetime:
    return datetime(2026, 10, day, hour, tzinfo=UTC)


def make_config(windows: list[Window] | None = None) -> PipelineConfig:
    windows = windows or [
        Window("2026-10-W1", utc(1), utc(6)),
        Window("2026-10-W2", utc(6), utc(11)),
        Window("2026-10-W3", utc(11), utc(16)),
    ]
    return PipelineConfig(
        calendar=WindowCalendar(windows),
        public_http=HttpSettings("test-agent", 0.0, 0, 5),
        partner_page_size=4,
        open_prices=OpenPricesSettings("https://prices.test/api/v1/prices", 2, "PRODUCT", 6),
        product_api=ProductApiSettings("https://products.test/api/v2/product", ("code",)),
        dump=DumpSettings(
            "https://dump.test/products.csv.gz",
            "products.csv.gz",
            (
                "code",
                "product_name",
                "quantity",
                "brands",
                "brands_tags",
                "brand_owner",
                "categories_tags",
                "main_category",
                "nutriscore_grade",
                "nova_group",
                "last_modified_t",
            ),
        ),
    )


SEPT = datetime(2026, 9, 1, tzinfo=UTC)

TENANTS = [
    ("TENANT_A", "a", "A Corp", "ENTERPRISE", "PRODUCTION", "NA", True, SEPT, SEPT),
    ("TENANT_B", "b", "B Corp", "GROWTH", "PRODUCTION", "EU", True, SEPT, SEPT),
    ("TENANT_SBX", "sbx", "Sandbox", "INTERNAL", "SANDBOX_LOADTEST", "GLOBAL", True, SEPT, SEPT),
]
LOCATIONS = [
    ("PLANT_1", "TENANT_A", "SUPPLIER_PLANT", None, "Plant", "US", "1", "OPEN", SEPT),
    ("DC_1", "TENANT_A", "REGIONAL_DC", "PLANT_1", "DC", "US", "2", "OPEN", SEPT),
    ("STORE_1", "TENANT_B", "RETAIL_STORE", "DC_1", "Store 1", "US", "3", "OPEN", SEPT),
    ("STORE_2", "TENANT_A", "RETAIL_STORE", "DC_9", "Store 2", "US", "4", "RENOVATION_HOLD", SEPT),
]
PRODUCTS = [
    ("PROD_1", "TENANT_A", "SKU-1", "3017620422003", "Item 1", "SNACKS", "PLANT_1", "PROD_2",
     150, 299, '{"uom": "EA", "pack_size": 12, "promo_eligible": true, "shelf_life_days": 180}',
     False, SEPT),
    ("PROD_2", "TENANT_B", "SKU-2", " 5449000000996 ", "Item 2", "BEVERAGES", "PLANT_1", None,
     160, 310, '{"pack_size": "12x500ml", "promo_eligible": "yes", "shelf_life_days": "180d"}',
     False, SEPT),
    ("PROD_3", "TENANT_A", "SKU-3", "GTIN:3017620422003", "Item 3", "SNACKS", "PLANT_1", None,
     170, 320, '{"pack_size": {"inner": 6, "outer": 24}, "temp_zone": "AMBIENT"}', True, SEPT),
]  # fmt: skip
# id, tenant, location, product, movement, quantity, status, document, occurred, ingested
LEDGER = [
    (1, "TENANT_A", "STORE_1", "PROD_1", "RECEIPT", 10, "POSTED_00", "DOC-1", utc(1), utc(1, 1)),
    (2, "TENANT_SBX", "STORE_1", "PROD_1", "RECEIPT", 5, "POSTED_00", "DOC-2", utc(2), utc(2, 1)),
    # Arrives in W2 with a business date in W1: late arriving.
    (3, "TENANT_A", "STORE_2", "PROD_2", "SHRINKAGE", -2, "VOID_CANCELLED_99", "DOC-3", utc(3),
     utc(7)),
    # Reuses DOC-1 of entry 1, two windows later.
    (4, "TENANT_B", "STORE_1", "PROD_3", "ADJUSTMENT", -1, "POSTED_00", "DOC-1", utc(12),
     utc(12, 1)),
    # A sandbox entry reusing DOC-3: entry 3 is flagged too.
    (5, "TENANT_SBX", "STORE_1", "PROD_1", "RECEIPT", 7, "PENDING_APPROVAL", "DOC-3", utc(12),
     utc(12, 2)),
]  # fmt: skip


class FakeErp:
    """ERP fixture with the real column set: two production tenants and the sandbox,
    two real barcodes written three different ways, and ledger entries in each window
    that exercise the silver rules."""

    def __init__(self, spark: SparkSession) -> None:
        self.spark = spark
        self.reads: list[str] = []

    def read_table(self, table: str) -> DataFrame:
        self.reads.append(table)
        if table == "arkham_erp.tenants":
            return self.spark.createDataFrame(
                TENANTS,
                "tenant_id STRING, tenant_slug STRING, company_name STRING, tier STRING, "
                "workspace_env STRING, region STRING, is_active BOOLEAN, "
                "created_at TIMESTAMP, updated_at TIMESTAMP",
            )
        if table == "arkham_erp.locations":
            return self.spark.createDataFrame(
                LOCATIONS,
                "location_id STRING, tenant_id STRING, location_type STRING, "
                "parent_supply_node_id STRING, banner_name STRING, country_code STRING, "
                "postal_code STRING, operating_status STRING, updated_at TIMESTAMP",
            )
        if table == "arkham_erp.products":
            return self.spark.createDataFrame(
                PRODUCTS,
                "product_id STRING, tenant_id STRING, internal_sku STRING, gtin_barcode STRING, "
                "product_name STRING, category_code STRING, primary_supplier_id STRING, "
                "substitute_product_id STRING, unit_cost_cents INT, retail_price_cents INT, "
                "lowcode_attributes STRING, is_discontinued BOOLEAN, updated_at TIMESTAMP",
            )
        raise KeyError(table)

    def read_ledger(self, start: datetime, end: datetime) -> DataFrame:
        self.reads.append("arkham_erp.inventory_ledger")
        rows = [
            (*row, False, json.dumps({"workflow_run_id": f"wf_{row[0]}", "schema_version": "1.0"}))
            for row in LEDGER
        ]
        df = self.spark.createDataFrame(
            rows,
            "ledger_entry_id BIGINT, tenant_id STRING, location_id STRING, product_id STRING, "
            "movement_type STRING, quantity_delta INT, entry_status STRING, "
            "reference_doc_id STRING, event_occurred_at TIMESTAMP, ingested_at TIMESTAMP, "
            "is_deleted BOOLEAN, lowcode_workflow_meta STRING",
        )
        return df.filter(
            (F.col("ingested_at") >= F.lit(start)) & (F.col("ingested_at") < F.lit(end))
        )


# Deliveries that are revisions or fail a rule (sequence -> changed fields).
EVENT_OVERRIDES: dict[int, dict[str, Any]] = {
    5: {"unit_price_cents": -300},  # W1: negative price, to quarantine
    12: {"event_id": "EVT-0000003", "revision": 2, "store_id": "STORE_2"},  # W2 corrects event 3
    13: {"event_id": "EVT-0000007", "revision": 2, "unit_price_cents": -100},  # W2: now invalid
    18: {"event_id": "EVT-0000005", "revision": 2, "unit_price_cents": 500},  # W3: now valid
    19: {"event_id": "EVT-0000099", "revision": 2},  # W3: revision 2 with no revision 1
}


class FakePartnerApi:
    """GET /v1/events by since_cursor. Event N arrives at `arrivals[N - 1]`."""

    def __init__(
        self, arrivals: list[datetime], overrides: dict[int, dict[str, Any]] | None = None
    ) -> None:
        self.arrivals = arrivals
        self.overrides = EVENT_OVERRIDES if overrides is None else overrides
        self.calls: list[dict[str, Any]] = []
        self.fail = False

    def occurred_at(self, sequence: int) -> datetime:
        return self.arrivals[sequence - 1] - timedelta(seconds=10)

    def event(self, sequence: int) -> dict[str, Any]:
        """Event 2 is the sandbox's, event 4 a shelf audit, event 6 cancelled and event 8
        dated in the local -06:00 format; odd events carry a promotion and a "15%" discount."""
        occurred = self.occurred_at(sequence)
        if sequence == 8:
            local = occurred.astimezone(timezone(timedelta(hours=-6)))
            timestamp = local.strftime("%Y/%m/%d %H:%M:%S-0600")
        else:
            timestamp = occurred.isoformat().replace("+00:00", "Z")
        event: dict[str, Any] = {
            "event_id": f"EVT-{sequence:07d}",
            "sequence_cursor": sequence,
            "revision": 1,
            "tenant_id": "TENANT_SBX" if sequence == 2 else "TENANT_A",
            "store_id": "STORE_1",
            "product_id": "PROD_1",
            "gtin_barcode": "003017620422003" if sequence % 3 == 0 else "3017620422003",
            "event_type": "SHELF_AUDIT_SCAN" if sequence == 4 else "POS_CHECKOUT",
            "units_sold": 2,
            "unit_price_cents": 300,
            "discount_rate": "15%" if sequence % 2 else 0.1,
            "currency": "USD",
            "record_state": "CANCELLED_VOID" if sequence == 6 else "ACTIVE",
            "is_deleted": False,
            "event_timestamp": timestamp,
            "ingested_at": self.arrivals[sequence - 1].isoformat().replace("+00:00", "Z"),
            "batch_window": "unused-by-the-pipeline",
        }
        if sequence % 2:
            event["promo_mechanics"] = {
                "campaign_id": "PRM-101",
                "mechanic_type": "LOYALTY_TIER",
                "co_funded_cpg_ratio": 0.3,
            }
        event.update(self.overrides.get(sequence, {}))
        return event

    def get_json(self, url: str, params: dict[str, Any] | None = None) -> tuple[bytes, Any]:
        params = params or {}
        self.calls.append(dict(params))
        if self.fail:
            raise RuntimeError("partner API unavailable")
        assert url.endswith("/v1/events")
        assert params["page"] == 1, "page must never be combined with since_cursor"
        start = int(params["since_cursor"]) + 1
        end = min(len(self.arrivals), start + int(params["page_size"]) - 1)
        data = [self.event(sequence) for sequence in range(start, end + 1)]
        payload = {
            "page": 1,
            "page_size": params["page_size"],
            "count": len(data),
            "next_cursor": data[-1]["sequence_cursor"] if data else params["since_cursor"],
            "has_more": end < len(self.arrivals),
            "data": data,
        }
        body = json.dumps(payload).encode()
        return body, json.loads(body)


class FakePublicApi:
    """Product API and Open Prices. `fail = True` makes every call fail like a dead source."""

    def __init__(
        self, prices: list[dict[str, Any]] | None = None, dump_file: Path | None = None
    ) -> None:
        self.prices = prices if prices is not None else default_prices()
        self.dump_file = dump_file
        self.calls: list[str] = []
        self.downloads = 0
        self.fail = False

    def head(self, url: str) -> dict[str, str]:
        self.calls.append(url)
        if self.fail or self.dump_file is None:
            raise HttpError(f"HEAD {url} failed after 3 retries")
        return {
            "Last-Modified": "Sat, 14 Nov 2026 10:13:40 GMT",
            "Content-Length": str(self.dump_file.stat().st_size),
        }

    def download(
        self,
        url: str,
        destination: Path,
        expected_size: int | None = None,
        etag: str | None = None,
    ) -> None:
        self.calls.append(url)
        self.downloads += 1
        assert self.dump_file is not None
        shutil.copyfile(self.dump_file, destination)

    def get_json(self, url: str, params: dict[str, Any] | None = None) -> tuple[bytes, Any]:
        params = params or {}
        self.calls.append(url)
        if self.fail:
            raise HttpError(f"GET {url} failed after 3 retries: HTTP 503")
        if url.endswith(".json"):
            code = url.rsplit("/", 1)[1].removesuffix(".json")
            payload: dict[str, Any] = {
                "code": code,
                "status": 1,
                "product": {
                    "code": code,
                    "product_name": f"API product {code}",
                    "brands": "Brand A, Brand B",
                    "brands_tags": ["brand-a", "brand-b"],
                    "brand_owner": "",
                    "categories_tags": ["en:snacks"],
                    "main_category": "en:snacks",
                    "nutriscore_grade": "d",
                    "nova_group": 4,
                    "created_t": 1600000000,
                    "last_modified_t": 1700000000,
                },
            }
        else:
            payload = self._prices_page(params)
        body = json.dumps(payload).encode()
        return body, json.loads(body)

    def _prices_page(self, params: dict[str, Any]) -> dict[str, Any]:
        codes = set(params["product_code__in"].split(","))

        def keep(item: dict[str, Any]) -> bool:
            if item["product_code"] not in codes:
                return False
            if "created__gte" in params:
                created = datetime.fromisoformat(item["created"])
                return (
                    datetime.fromisoformat(params["created__gte"])
                    <= created
                    < datetime.fromisoformat(params["created__lt"])
                )
            return params["date__gte"] <= item["date"] < params["date__lt"]

        items = sorted((item for item in self.prices if keep(item)), key=lambda item: item["id"])
        size, page = int(params["size"]), int(params["page"])
        pages = max(1, -(-len(items) // size))
        return {
            "items": items[(page - 1) * size : page * size],
            "total": len(items),
            "page": page,
            "pages": pages,
            "size": size,
        }


def default_prices() -> list[dict[str, Any]]:
    """Three baseline prices, one per window, a duplicate in W1 and a code outside the ERP.
    Each carries the personal `owner` fields, which must never reach silver."""
    rows = [
        (1, "3017620422003", "2026-05-10", datetime(2026, 5, 11, tzinfo=UTC), None),
        (2, "3017620422003", "2026-09-20", datetime(2026, 9, 21, tzinfo=UTC), None),
        (3, "5449000000996", "2026-08-01", datetime(2026, 8, 2, tzinfo=UTC), None),
        (4, "3017620422003", "2026-10-02", utc(2, 10), None),
        (5, "5449000000996", "2026-10-07", utc(7, 10), None),
        (6, "3017620422003", "2026-10-12", utc(12, 10), None),
        (7, "9999999999999", "2026-10-12", utc(12, 11), None),  # not an ERP barcode
        (8, "3017620422003", "2026-10-02", utc(3, 10), 4),  # duplicate of price 4
    ]
    return [
        {
            "id": price_id,
            "product_code": code,
            "type": "PRODUCT",
            "date": price_date,
            "created": created.isoformat().replace("+00:00", "Z"),
            "updated": created.isoformat().replace("+00:00", "Z"),
            "price": 4.45,
            "currency": "EUR",
            "duplicate_of": duplicate_of,
            "owner": "someone",
            "owner_comment": "my local shop",
            "location": {"osm_name": "Shop", "osm_address_country_code": "fr"},
            "proof": {"type": "RECEIPT", "owner": "someone"},
        }
        for price_id, code, price_date, created, duplicate_of in rows
    ]


REPO = Path(__file__).resolve().parents[1]
FALLBACK = REPO / "fallback"
W1, W2, W3 = "2026-10-W1", "2026-10-W2", "2026-10-W3"


@dataclass
class Harness:
    pipeline: Pipeline
    erp: FakeErp
    partner: FakePartnerApi
    public: FakePublicApi

    def rows(self, table: str, partition: str | None = None) -> int:
        return {
            item["partition"]: item["rows"] for item in self.pipeline.lakehouse.fingerprint(table)
        }.get(partition, 0)

    def table(self, name: str) -> DataFrame:
        return self.pipeline.lakehouse.read(name)


def harness(
    spark: SparkSession,
    root: Path,
    config: PipelineConfig | None = None,
    dump_file: Path | None = None,
) -> Harness:
    """A pipeline over the in-memory sources, writing under `root`."""
    erp = FakeErp(spark)
    partner = FakePartnerApi(default_arrivals())
    public = FakePublicApi(dump_file=dump_file)
    sources = Sources(
        erp=erp, partner_api=partner, partner_api_url="http://partner.test", public_http=public
    )
    pipeline = Pipeline(
        spark, config or make_config(), sources, root / "lakehouse", root / "landing", FALLBACK
    )
    return Harness(pipeline, erp, partner, public)


def default_arrivals() -> list[datetime]:
    """Ten events in W1, six in W2, five in W3."""
    w1 = [UTC_START + timedelta(hours=5 * i) for i in range(10)]
    w2 = [utc(6) + timedelta(hours=7 * i) for i in range(6)]
    w3 = [utc(11) + timedelta(hours=9 * i) for i in range(5)]
    return w1 + w2 + w3
