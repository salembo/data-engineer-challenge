# CLAUDE.md
 
## Project
 
- **Name**: arkham-process-delta-pipeline
- **What it is**: multi-tenant lakehouse for the Arkham Platform Engineering Assessment. Requirements are in the assessment statement: the challenge's `README.md`, one level above this repository (`../README.md`). The `README.md` inside this repository is the project README (how to run and inspect the platform), not the requirements. Product decisions are in two PRDs. `docs/prd.md` covers sources, bronze and silver (v1). `docs/PRD v2-v4 Arkham Data Platform.md` covers gold, catalog and graph (v2, v3) and the Prefect layer (v4), with every acceptance criterion and its evidence. If a request contradicts a PRD, say so before coding.
- **Stack**: Python 3.11, PySpark, open-source Delta Lake in local mode, Poetry. Versions are pinned in `pyproject.toml`; do not upgrade them unasked.
- **Versions**: v1 is environment, bronze and silver. v2 is gold. v3 is catalog and graph, and is the evaluable deliverable. v4 is the optional Prefect orchestration layer.
- **Current scope**: v1 to v4 are built and verified on the real data. Anything new is added only when asked, as an optional item that follows the v4 isolation rule below.
## Hard constraints
 
- The pipeline container must stay under 2 GB of memory, including the Open Food Facts dump load; each source service under 512 MB.
- Never modify anything under `legacy_sources/`.
- All timestamps are UTC: set it on the Spark session and on the JDBC connection.
- Batch only.
- Windows are a parameter. Never hardcode window IDs, window dates or tenant IDs.
- Re-running any window must leave every table identical. The only exception is the volatile columns of `gold.pipeline_health` and `catalog.freshness` (run status, duration, load times).
## Windows and extraction
 
- The window calendar is configuration: `window_id`, start (included) and end (excluded), in UTC. The internal ID (`2026-10-W1`) is the only one the pipeline uses.
- Keep three time fields apart: business time, arrival time at the source, and `_window_id`. A late record keeps its business date and belongs to the window in which it arrived.
- A re-run reads the API responses and the Open Food Facts dump from `landing/` and never calls those sources again. The ERP is read again for the same snapshot or `ingested_at` range (`docs/prd.md`, extraction contract).
| Source object | Window assignment | Notes |
| --- | --- | --- |
| ERP dimensions | Full snapshot per window | JDBC |
| ERP ledger | `ingested_at` within the window range | JDBC range predicate |
| Partner events (`GET /v1/events`) | `ingested_at` within the window range | By cursor; see below |
| Open Food Facts dump (CSV, gzip) | One reference snapshot, not windowed | Never downloaded again unless forced |
| Open Food Facts product API | Snapshot of the ERP codes per window | One lookup per code |
| Open Prices | `created` within the window range | Baseline by `date`, six months before the first window; the length is configuration |
 
- **Partner API cursor**: first window starts at cursor 0, later windows at the stored end cursor of the previous one. Use `since_cursor`, `page=1`, `page_size=500`; never combine `page` with `since_cursor`. Stop on `has_more = false`, on `count = 0`, or at the first event whose `ingested_at` is at or after the window end. Store the end cursor only after the bronze write succeeds.
- **Public APIs**: custom User-Agent, one request per second, at most three retries. On failure use the stored landing response, then the committed fixture, then leave the public tables empty for that window.
- **Dump**: read as a single streaming task with an explicit text schema, select the eleven validated columns, filter by code on read. No caching, no repartitioning of the full dataset.
## Layers
 
- **landing**: source files exactly as received (`.jsonl`, `.csv.gz`).
- **bronze**: keeps the types the source declares and infers none. ERP columns keep their types; JSON goes to a `raw_json` `STRING` column and CSV columns are `STRING`. Control columns: `_window_id`, `_run_id`, `_loaded_at`, `_source`. Partitioned by `_window_id` and written with `replaceWhere` on that partition. Never plain `append`.
- **bronze filters**: none. The sandbox and invalid records enter bronze. The one exception is `bronze.off_products`, which holds only codes present in the ERP; log rows read and rows kept.
- **silver**: the current state of each entity, typed and resolved by its own key. History stays in bronze. Rows that fail a rule go to quarantine with a reason; they are never dropped silently.
- **gold**: aggregates for the three business domains (inventory, POS sales, pipeline health), built from silver. Only `gold.reconciliation` also reads bronze. No gold table reads the sources.
- **graph**: nodes and edges from the silver dimensions, plus traversal results computed with GraphFrames. No graph database. Ad hoc graph questions run on the GraphFrame `g` that `make pyspark` builds (`arkham.graph.traversal.as_graph_frame`).
- **catalog**: generated from the registry `config/catalog.toml` at the end of every run; column names and types come from the Delta tables. A new table needs a registry entry, or the run fails.
## Tenancy and scope
 
- Every table declares a scope: tenant, global public (`GLOBAL`) or platform. Only tenant-scoped tables must carry `tenant_id`.
- In facts, `tenant_id` is the tenant that records the fact and defines isolation. Facts also carry `location_owner_tenant_id` and `product_owner_tenant_id`.
- Join facts to dimensions by ID only (`location_id`, `product_id`).
- The sandbox is found by joining `tenant_id` to `dim_tenant` and checking `workspace_env`, never by ID. It stays in the dimensions, flagged, and is excluded from silver facts. A fact whose tenant is unknown goes to quarantine.
## Silver rules
 
| Table | Key | Current row |
| --- | --- | --- |
| `dim_tenant`, `dim_location`, `dim_product` | Their ID | Latest snapshot |
| `fact_inventory_movement` | `ledger_entry_id` | One row per entry; reused `reference_doc_id` is kept and flagged |
| `fact_pos_sale`, `fact_shelf_audit` | `event_id` | Highest `revision`, then highest `sequence_cursor` |
| `ref_product_public` | `gtin14` | Highest `last_modified_t`; on a tie the API; nulls filled from the other source |
| `ref_public_price` | `id` | Highest `updated`; rows with `duplicate_of` excluded |
 
- **Revisions**: `MERGE` applies an incoming row only when its (`revision`, `sequence_cursor`) is higher than the stored one. A revision 2 with no revision 1 is valid. Check rules on the current revision; if it fails, the whole event goes to quarantine and the earlier revision is not restored.
- **Status**: silver keeps every status. Only `POSTED_00` ledger entries and `ACTIVE` sales count in gold.
- **Barcodes**: three columns. `gtin_raw` as received. `gtin14`: digits only, strip leading zeros to measure the significant length, left-pad to exactly 14 if it is 14 or less, otherwise null. `gtin_status`: `VALID`, `INVALID_CHECK_DIGIT`, `TOO_LONG` or `INVALID`. Only `INVALID` goes to quarantine.
- **Product link**: events join products by `product_id`. The product barcode comes from the ERP master; keep the event's own as `event_gtin14`.
- **Money**: integer cents in columns ending in `_cents`, always next to a currency column. Never add amounts across currencies; there is no conversion.
- **Discount**: `discount_rate` as a numeric fraction (`"15%"` becomes 0.15). `promo_mechanics` becomes `promo_campaign_id`, `promo_mechanic_type`, `promo_co_funded_ratio`.
- **Partner events**: `POS_CHECKOUT` and `SHELF_AUDIT_SCAN` go to separate tables. A negative unit price goes to quarantine. Unparseable timestamps go to quarantine.
- **Low-code attributes**: keep the JSON as text and extract typed columns by the shape of each value, never by tenant ID.
- **Public price**: a reference indicator; never use it to fill a sale.
- **Personal data**: `owner` from Open Prices must not appear in silver, gold, logs or the catalog.
## Gold rules

Metric formulas and expected values are in the v2-v4 PRD; these are the rules that shape the code.

- **Full recompute**: every run recomputes the business tables (`inventory_position`, `inventory_daily`, `pos_sales_daily`, `pos_promo_performance`) in full from silver. Do not track affected dates or periods.
- **Business date**: aggregate by the UTC date of the event, not of its ingestion. A late record lands on the day it happened.
- **Grain**: no nulls in a grain column. A sale line with no promotion uses `NONE`.
- **Sandbox**: business tables hold no sandbox rows. Health tables report the sandbox as filtered.
- **Money**: revenue and discounts are rounded half up to the cent on each sale line, then added, and always grouped by currency.
- **Reconciliation from state**: compute `gold.reconciliation` from bronze (keys, `_window_id`, tenant), silver and the quarantine, never from the run log counts, which depend on run order. Each bronze row counts in the window it arrived in.
- **Pipeline health**: row counts come from `gold.reconciliation`. The run log provides only run status, duration, fallback use and source counts.
- **Public price**: the reference is never used to fill a sale. Ties on the same date are broken by the latest `updated_at`, then the highest `id`.
## Runs
 
- Run status is `SUCCESS`, `SUCCESS_WITH_FALLBACK` (a public source failed) or `FAILED` (a legacy source or any stage failed).
- A window is complete only when all its stages finish. An interrupted run leaves it incomplete; running it again repeats the whole window.
- An empty window finishes with `SUCCESS` and zero rows.
- The run log counts rows per table and window by outcome: read, written, quarantined, filtered, superseded, flagged.
## Spark and Delta
 
- Use the DataFrame API with explicit schemas and type hints on every transformation function.
- No `collect()` or `toPandas()` on full tables, no caching of large DataFrames, keep shuffle partitions low.
- Do not run `OPTIMIZE`, `ZORDER` or `VACUUM`: tables are small and reviewers inspect Delta history.
- Never overwrite a whole table. The only write patterns are:
  - bronze: partition-scoped `replaceWhere`;
  - silver: `MERGE` on the key;
  - gold, graph and catalog: `MERGE` on the grain that inserts new rows, updates only changed rows and deletes rows that are no longer in the result.
- A re-run with no changes must leave no rewritten rows in the Delta history, except the window's rows in `gold.pipeline_health` and `catalog.freshness`.
- SQL uses UPPERCASE keywords and snake_case identifiers.
## Commands
 
Target interface; keep this section in sync with the Makefile.
 
- Start sources: `make up`
- Verify sources (ERP seed counts, read-only role, API contract): `make check-sources`
- ERP shell as the pipeline's read-only role: `make psql`
- Status and logs: `make ps`, `make logs`
- Stop sources: `make down` (keeps the ERP volume); `make clean` (deletes it, re-seeded on the next `make up`)
- Run one window: `make run WINDOW=2026-10-W1`
- Run pending windows in order (loads reference data first if it never succeeded): `make run-pending`
- Load reference data (Open Food Facts dump, Open Prices baseline): `make reference` (`SKIP_DOWNLOAD=1` uses the committed dump fixture, `FORCE=1` downloads again)
- Window state and bronze row counts: `make status`; row count and hash per partition: `make fingerprint`
- Re-run a window and check every table is unchanged and no derived table except `gold.pipeline_health` and `catalog.freshness` got a new commit: `make verify-rerun WINDOW=2026-10-W1`
- Check the invariants, which hold on any data and any window calendar: `make verify` (`src/arkham/invariants.py`)
- Check the baseline numbers of the provided dataset: `make verify-v1`, `make verify-v2`, `make verify-v3` (`src/arkham/acceptance.py`); on other data or another calendar they report "Not applicable"
- Interactive PySpark with every table registered as `<layer>.<table>` and the graph as the GraphFrame `g` (checkpoint directory set): `make pyspark`
- Refresh the public-API fallback fixtures in `fallback/`: `make capture-fallback`
- Rebuild silver and its quarantine from bronze, then gold: `make rebuild-silver`
- Recompute gold, the graph and the catalog from silver (writes only rows that changed): `make rebuild-gold`
- Build every image (partner API and pipeline): `make build` (needed after any dependency change; `make up` never rebuilds, so running sources are not restarted)
- Check the pipeline runtime against the sources: `make smoke`
- Tests: `make test` (one pytest process, and so one Spark JVM, per test file inside the pipeline container; `make test ARGS="tests/test_gtin.py"` or `ARGS="-k name"` to narrow it; locally `poetry run python scripts/run_tests.py` needs Python 3.11 and Java 21)
- Format and lint: `make format`, `make lint` (black and ruff inside the pipeline container)
- Regenerate `poetry.lock` after editing `pyproject.toml`: `make lock`
- Regenerate `docker/pipeline/spark-jars.lock` after editing `src/arkham/spark_packages.py`: `make lock-jars` (the build fails while it is stale)
- Shell in the pipeline container: `make shell`
- Optional Prefect layer (v4): `make prefect-build`, `make prefect-up` (UI at http://localhost:4200), `make prefect-pending`, `make prefect-run WINDOW=...`, `make prefect-verify WINDOW=...`, `make prefect-test`, `make prefect-down`, `make prefect-lock`
## Working rules
 
- Any change to a write path needs the re-run test updated and passing.
- Acceptance checks come in two kinds. A property that must hold on any data is an invariant in `src/arkham/invariants.py`, with no dataset value in it. An exact number of the provided dataset belongs in `src/arkham/acceptance.py`, the baseline: the only module allowed to name window IDs, tenant IDs or node IDs, and only as expected values.
- Tests never call the network; use fixtures under `tests/fixtures/`.
- Never run the whole suite in one JVM (`pytest tests/`): every test window runs all five layers and the 1 GB heap runs out. Test files that share a session fixture are grouped in `SHARED` in `scripts/run_tests.py`.
- Ask before adding a dependency or a service, or before changing a rule in this file.
- v4 items are optional and independent: each one lives in its own folder, image target, compose profile and make targets, and nothing in v1 to v3 may import or depend on it. Prefect lives in `orchestration/` with its own lock file; never add it to the main `pyproject.toml`.
- Do not commit secrets or data files larger than 1 MB.
- Keep this file under 200 lines. Put detailed rules in `.claude/rules/`.
 