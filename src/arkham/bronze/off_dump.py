"""Open Food Facts dump to bronze: one reference snapshot, not windowed.

- Downloaded once into the landing zone and reused; downloaded again only when forced.
- Read as a single streaming task (gzip is not splittable) with an explicit
  all-text schema taken from the file's own header line: nothing is inferred.
- Only the configured columns are kept, and only rows whose barcode exists in the
  ERP (the documented bronze exception). One pass counts rows read and kept.
"""

from __future__ import annotations

import gzip
import json
from datetime import UTC, date, datetime
from email.utils import parsedate_to_datetime
from pathlib import Path

from pyspark.sql import DataFrame, Observation
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from arkham.bronze.context import PipelineError, RunContext
from arkham.gtin import gtin14_column
from arkham.http import HttpClient, HttpError
from arkham.runlog import FILTERED, READ, WRITTEN

SOURCE = "off_dump"
TABLE = "bronze.off_products"
PARTITION_KEY = "snapshot_date"
FIXTURE = Path("open_food_facts") / "off_18.tsv"


def read_header(path: Path) -> list[str]:
    opener = gzip.open if path.suffix == ".gz" else open
    with opener(path, "rt", encoding="utf-8") as handle:
        return handle.readline().rstrip("\r\n").split("\t")


def ensure_dump(
    ctx: RunContext, client: HttpClient, force_download: bool, skip_download: bool
) -> tuple[Path, date]:
    """Returns the dump file to load and its snapshot date."""
    settings = ctx.config.dump
    landed = ctx.landing.partitions(SOURCE, PARTITION_KEY)

    def landed_file(snapshot: str) -> tuple[Path, date]:
        directory = ctx.landing.partition_dir(SOURCE, PARTITION_KEY, snapshot)
        return directory / settings.file_name, date.fromisoformat(snapshot)

    def fixture(reason: str) -> tuple[Path, date]:
        path = ctx.fallback_root / FIXTURE
        manifest = json.loads((path.parent / "_manifest.json").read_text())
        ctx.record.fallback(
            f"{SOURCE}: committed fixture (captured {manifest['captured_at']}); {reason}"
        )
        return path, date.fromisoformat(manifest["captured_at"][:10])

    if skip_download:
        return fixture("download skipped on request")
    if landed and not force_download:
        return landed_file(landed[-1])
    try:
        headers = {k.lower(): v for k, v in client.head(settings.url).items()}
        last_modified = headers.get("last-modified")
        snapshot = (
            parsedate_to_datetime(last_modified).astimezone(UTC).date()
            if last_modified
            else datetime.now(UTC).date()
        )
        size = int(headers["content-length"]) if "content-length" in headers else None
        manifest = {
            "source": settings.url,
            "last_modified": last_modified,
            "etag": headers.get("etag"),
            "size_bytes": size,
            "downloaded_at": datetime.now(UTC).isoformat(),
        }
        ctx.landing.write_partition(
            SOURCE,
            PARTITION_KEY,
            snapshot.isoformat(),
            manifest,
            populate=lambda staging: client.download(
                settings.url, staging / settings.file_name, size, headers.get("etag")
            ),
        )
        return landed_file(snapshot.isoformat())
    except HttpError as error:
        if landed:
            ctx.record.fallback(f"{SOURCE}: landed snapshot {landed[-1]}; download failed: {error}")
            return landed_file(landed[-1])
        return fixture(f"download failed: {error}")


def read_dump(ctx: RunContext, path: Path, codes14: list[str]) -> tuple[DataFrame, Observation]:
    """The configured columns, as text, for rows whose barcode is in `codes14`."""
    header = read_header(path)
    columns = list(ctx.config.dump.columns)
    missing = [column for column in columns if column not in header]
    if missing:
        raise PipelineError(f"The dump at {path} lacks the expected columns {missing}.")
    schema = StructType([StructField(name, StringType()) for name in header])
    observation = Observation("off_dump_read")
    df = (
        ctx.spark.read.schema(schema)
        .option("header", "true")
        .option("sep", "\t")
        .option("quote", "\u0000")  # the export has no quoting; quotes are literal text
        .option("mode", "PERMISSIVE")
        .csv(str(path))
        .select(*columns)
        .observe(observation, F.count(F.lit(1)).alias("rows_read"))
        .where(gtin14_column(F.col("code")).isin(codes14))
    )
    return df, observation


def load(ctx: RunContext, path: Path, snapshot: date, codes14: list[str]) -> None:
    df, observation = read_dump(ctx, path, codes14)
    df = df.withColumn("_snapshot_date", F.lit(snapshot))
    kept = ctx.write_bronze(TABLE, df, snapshot.isoformat(), f"open_food_facts:{path.name}", None)
    rows_read = int(observation.get["rows_read"])
    ctx.record.count(TABLE, READ, rows_read, f"snapshot {snapshot.isoformat()}")
    ctx.record.count(TABLE, WRITTEN, kept)
    ctx.record.count(TABLE, FILTERED, rows_read - kept, "barcode not in the ERP")
