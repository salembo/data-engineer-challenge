"""Partner API events to bronze, by cursor.

Extraction contract (docs/prd.md):
- the first window starts at cursor 0; each later window starts at the end cursor
  of the previous window, which is trusted only once that window is complete;
- pages are requested with since_cursor, page=1 and page_size <= 500, never
  combining page with since_cursor;
- extraction stops on has_more = false, on count = 0, or at the first event whose
  ingested_at is at or after the window end;
- a re-run reads the landed responses and never calls the API again.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from arkham.bronze.context import PipelineError, RunContext
from arkham.config import Window
from arkham.http import HttpClient
from arkham.landing import RESPONSES
from arkham.runlog import FLAGGED, READ, WRITTEN

SOURCE = "partner_events"
TABLE = "bronze.partner_events"
PARTITION_KEY = "window_id"


def parse_instant(text: str) -> datetime:
    return datetime.fromisoformat(text).astimezone(UTC)


def extract(
    client: HttpClient, base_url: str, window: Window, start_cursor: int, page_size: int
) -> tuple[list[bytes], dict[str, Any]]:
    """Fetches the window's events. Returns the response bodies and the manifest."""
    url = f"{base_url}/v1/events"
    responses: list[bytes] = []
    cursor = end_cursor = start_cursor
    records = 0
    stopped_at_window_end = False
    while True:
        body, payload = client.get_json(
            url, {"since_cursor": cursor, "page": 1, "page_size": page_size}
        )
        responses.append(body)
        events = payload.get("data") or []
        for event in events:
            if parse_instant(event["ingested_at"]) >= window.end:
                stopped_at_window_end = True
                break
            end_cursor = int(event["sequence_cursor"])
            records += 1
        if stopped_at_window_end or not events or not payload.get("has_more"):
            break
        cursor = int(events[-1]["sequence_cursor"])
    manifest = {
        "source": f"GET {url}",
        "window_id": window.id,
        "window_start": window.start.isoformat(),
        "window_end": window.end.isoformat(),
        "start_cursor": start_cursor,
        "end_cursor": end_cursor,
        "records": records,
        "responses": len(responses),
        "page_size": page_size,
        "stopped_at_window_end": stopped_at_window_end,
        "extracted_at": datetime.now(UTC).isoformat(),
    }
    return responses, manifest


def start_cursor(ctx: RunContext, window: Window) -> int:
    previous = ctx.config.calendar.previous(window.id)
    if previous is None:
        return 0
    manifest = ctx.landing.read_manifest(SOURCE, PARTITION_KEY, previous.id)
    if manifest is None:
        raise PipelineError(
            f"The landed partner events of {previous.id} are missing, so the start cursor of "
            f"{window.id} is unknown. Run {previous.id} again first."
        )
    return int(manifest["end_cursor"])


def ensure_landed(
    ctx: RunContext, client: HttpClient, base_url: str, window: Window
) -> dict[str, Any]:
    manifest = ctx.landing.read_manifest(SOURCE, PARTITION_KEY, window.id)
    if manifest is not None:
        return manifest
    responses, manifest = extract(
        client, base_url, window, start_cursor(ctx, window), ctx.config.partner_page_size
    )
    ctx.landing.write_partition(SOURCE, PARTITION_KEY, window.id, manifest, responses=responses)
    return manifest


def events_from_responses(ctx: RunContext, window: Window, manifest: dict[str, Any]) -> DataFrame:
    """One row per event of the window, from the landed responses. The last response can
    hold events of the next window; the cursor range of the manifest excludes them."""
    path = ctx.landing.partition_dir(SOURCE, PARTITION_KEY, window.id) / RESPONSES
    raw = F.col("raw_json")
    return (
        ctx.spark.read.text(str(path))
        .select(F.explode(F.from_json("value", "data ARRAY<STRING>")["data"]).alias("raw_json"))
        .select(
            "raw_json",
            F.get_json_object(raw, "$.sequence_cursor").cast("bigint").alias("sequence_cursor"),
            F.get_json_object(raw, "$.event_id").alias("event_id"),
            F.get_json_object(raw, "$.ingested_at").alias("ingested_at"),
            # The table is tenant-scoped: consumers filter it by this column (v3 catalog).
            F.get_json_object(raw, "$.tenant_id").alias("tenant_id"),
        )
        .filter(
            (F.col("sequence_cursor") > manifest["start_cursor"])
            & (F.col("sequence_cursor") <= manifest["end_cursor"])
        )
    )


def load(ctx: RunContext, client: HttpClient, base_url: str, window: Window) -> None:
    manifest = ensure_landed(ctx, client, base_url, window)
    events = events_from_responses(ctx, window, manifest)
    # The source count per tenant, from the responses as the API returned them; its total
    # is the manifest's record count.
    per_tenant = (
        events.groupBy(F.get_json_object("raw_json", "$.tenant_id").alias("tenant_id"))
        .count()
        .collect()
    )
    ctx.record.count_by_tenant(TABLE, READ, {row.tenant_id: row["count"] for row in per_tenant})
    rows = ctx.write_bronze(
        TABLE, events, window.id, f"partner_api:{manifest['source']}", window.id
    )
    ctx.record.count(TABLE, WRITTEN, rows)
    # Delivered in this window but arrived before it started: kept, and reported.
    early = (
        ctx.lakehouse.read(TABLE)
        .filter(F.col("_window_id") == window.id)
        .filter(F.try_to_timestamp(F.col("ingested_at")) < F.lit(window.start))
        .count()
    )
    if early:
        ctx.record.count(TABLE, FLAGGED, early, "ingested_at before the window start")
