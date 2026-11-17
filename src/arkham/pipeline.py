"""Orchestration: runs a window (bronze, silver, gold, graph, then catalog), the pending
windows, the reference data load, or a rebuild of silver from bronze, or of the derived
layers (gold, graph, catalog) from silver."""

from __future__ import annotations

import sys
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from arkham.bronze import erp as bronze_erp
from arkham.bronze import off_dump, partner_events, public_apis
from arkham.bronze.context import PipelineError, RunContext
from arkham.catalog import stage as catalog
from arkham.config import PipelineConfig
from arkham.erp import ErpSource
from arkham.gold import stage as gold
from arkham.graph import stage as graph
from arkham.http import HttpClient
from arkham.lakehouse import TABLES, Lakehouse
from arkham.landing import LandingZone
from arkham.runlog import RunLog, RunRecord
from arkham.silver import stage as silver
from arkham.silver.quarantine import TABLE as QUARANTINE

__all__ = ["Pipeline", "PipelineError", "Sources"]


@dataclass
class Sources:
    erp: ErpSource
    partner_api: HttpClient
    partner_api_url: str
    public_http: HttpClient


def derived_layers(ctx: RunContext) -> None:
    """Gold, the graph and the catalog, recomputed from silver; the catalog goes last so it
    sees every table the run wrote."""
    gold.run(ctx)
    graph.run(ctx)
    catalog.run(ctx)


def refresh_run_details(ctx: RunContext) -> None:
    """After the run is recorded as finished: its final status and duration replace the
    provisional ones in gold.pipeline_health and catalog.freshness."""
    gold.refresh_run_attributes(ctx)
    catalog.refresh_run_attributes(ctx)


def new_run_id(now: datetime) -> str:
    return f"{now.strftime('%Y%m%dT%H%M%SZ')}-{uuid.uuid4().hex[:8]}"


class Pipeline:
    def __init__(
        self,
        spark: SparkSession,
        config: PipelineConfig,
        sources: Sources,
        lakehouse_root: Path,
        landing_root: Path,
        fallback_root: Path,
    ) -> None:
        self.spark = spark
        self.config = config
        self.sources = sources
        self.lakehouse = Lakehouse(spark, lakehouse_root)
        self.landing = LandingZone(landing_root)
        self.fallback_root = fallback_root
        self.runlog = RunLog(spark, self.lakehouse)

    def _execute(
        self,
        command: str,
        window_id: str | None,
        stages: Callable[[RunContext], None],
        after_finish: Callable[[RunContext], None] | None = None,
    ) -> RunRecord:
        """Runs `stages` as one recorded run. `after_finish` runs once the run is recorded
        as finished; it only refines what the run wrote, so if it fails the run stays
        successful and a warning is printed (the next run repeats it)."""
        started = datetime.now(UTC)
        record = RunRecord(new_run_id(started), command, window_id, started)
        self.runlog.start(record)
        context = RunContext(
            self.spark,
            self.config,
            self.lakehouse,
            self.landing,
            self.fallback_root,
            record,
            loaded_at=started,
        )
        try:
            try:
                stages(context)
            except BaseException as error:  # record interruptions too, then re-raise
                self.runlog.finish(record, error)
                raise
            self.runlog.finish(record)
            if after_finish is not None:
                try:
                    after_finish(context)
                except Exception as error:  # noqa: BLE001 - the run itself succeeded
                    print(
                        f"warning: {command} finished, but refreshing its run details in gold "
                        f"failed ({type(error).__name__}: {error}); the next run refreshes them.",
                        file=sys.stderr,
                    )
        finally:
            self.lakehouse.clear_scratch(record.run_id)
        return record

    def run_window(self, window_id: str) -> RunRecord:
        window = self.config.calendar.get(window_id)
        incomplete = [w.id for w in self.config.calendar.before(window_id)]
        incomplete = [w for w in incomplete if not self.runlog.is_complete(w)]
        if incomplete:
            raise PipelineError(
                f"Windows run in chronological order: {', '.join(incomplete)} must complete "
                f"before {window_id}. Use `make run-pending`."
            )

        def stages(ctx: RunContext) -> None:
            bronze_erp.load_dimensions(ctx, self.sources.erp, window)
            bronze_erp.load_ledger(ctx, self.sources.erp, window)
            partner_events.load(ctx, self.sources.partner_api, self.sources.partner_api_url, window)
            products = self.lakehouse.read("bronze.erp_products").filter(
                F.col("_window_id") == window.id
            )
            codes = public_apis.erp_barcodes(products)
            public_apis.load_products(ctx, self.sources.public_http, window, codes)
            public_apis.load_window_prices(ctx, self.sources.public_http, window, codes)
            silver.run_window(ctx, window)
            derived_layers(ctx)

        return self._execute("run", window.id, stages, refresh_run_details)

    def load_reference(
        self, force_download: bool = False, skip_download: bool = False
    ) -> RunRecord:
        """Data that is not windowed: the Open Food Facts dump and the Open Prices baseline."""

        def stages(ctx: RunContext) -> None:
            products = self.sources.erp.read_table("arkham_erp.products")
            codes = public_apis.erp_barcodes(products)
            codes14 = sorted(code.zfill(14) for code in codes)
            path, snapshot = off_dump.ensure_dump(
                ctx, self.sources.public_http, force_download, skip_download
            )
            off_dump.load(ctx, path, snapshot, codes14)
            public_apis.load_baseline_prices(ctx, self.sources.public_http, codes)
            silver.run_reference(ctx)
            derived_layers(ctx)  # public attributes and prices feed the sales tables

        return self._execute("reference", None, stages, refresh_run_details)

    def rebuild_silver(self) -> RunRecord:
        """Recreates silver and its quarantine from bronze: drops them, then replays the
        silver stage of every complete window in order, and recomputes gold. Bronze and
        landing are untouched."""
        complete = [w for w in self.config.calendar.windows if self.runlog.is_complete(w.id)]

        def stages(ctx: RunContext) -> None:
            for table in silver.SILVER_TABLES:
                self.lakehouse.drop(table)
            silver.rebuild(ctx, complete)
            derived_layers(ctx)

        return self._execute("rebuild-silver", None, stages)

    def rebuild_gold(self) -> RunRecord:
        """Recomputes gold, the graph and the catalog from silver (and bronze, for
        reconciliation), touching nothing else. Derived tables are synchronised, so only
        rows that changed are written."""
        return self._execute("rebuild-gold", None, derived_layers)

    def pending_windows(self) -> list[str]:
        return [w.id for w in self.config.calendar.windows if not self.runlog.is_complete(w.id)]

    def run_pending(self) -> list[RunRecord]:
        """Loads the reference data if it never succeeded, then every pending window in order."""
        records = []
        if not self.runlog.reference_loaded():
            records.append(self.load_reference())
        for window_id in self.pending_windows():
            records.append(self.run_window(window_id))
        return records

    def fingerprints(
        self, layers: tuple[str, ...] = ("bronze", "silver", "gold", "graph", "catalog")
    ) -> list[dict[str, object]]:
        """Fingerprints of every data table in `layers`, plus the quarantine with silver.
        The run log and metrics are excluded: they record each run by design. Gold
        fingerprints leave out the volatile columns of gold.pipeline_health and
        catalog.freshness."""
        result: list[dict[str, object]] = []
        for qualified_name, spec in TABLES.items():
            wanted = spec.layer in layers or (qualified_name == QUARANTINE and "silver" in layers)
            if wanted and self.lakehouse.exists(qualified_name):
                result.extend(self.lakehouse.fingerprint(qualified_name))
        return result
