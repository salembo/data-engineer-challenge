# Arkham data platform

Multi-tenant lakehouse for the Arkham Platform Engineering Assessment. It ingests the legacy ERP, the partner event gateway and public Open Food Facts data into Delta Lake through an incremental medallion pipeline (bronze, silver, gold) written in PySpark, processing one ingestion window at a time.

Product decisions, source findings and acceptance criteria are in two PRDs: [docs/prd.md](docs/prd.md) for sources, bronze and silver, and [docs/PRD v2-v4 Arkham Data Platform.md](docs/PRD%20v2-v4%20Arkham%20Data%20Platform.md) for gold, the catalog, the graph and the optional Prefect layer.

## Status

The project is delivered in versions; v3 is the evaluable deliverable.

| Version | Scope | State |
| --- | --- | --- |
| v1 | Environment, bronze and silver | Done: the invariants (`make verify`) and the v1 baseline numbers (`make verify-v1`) pass on the real data |
| v2 | Gold: inventory, sales and pipeline health | Done: the invariants and the v2 baseline numbers (`make verify-v2`) pass on the real data |
| v3 | Data catalog and supply-network graph | Done: the invariants and the v3 baseline numbers (`make verify-v3`) pass on the real data |
| v4 (optional) | Prefect orchestration layer over the CLI | Done: the same windows run through the flow with identical tables (`make prefect-verify`) |

## Requirements

- Docker with Compose v2 (Docker Desktop on macOS and Windows). Docker needs at least 3 GB of memory: 2 GB for the pipeline and 512 MB for each source service.
- GNU Make and Bash. Nothing else is needed on the host: Python, Java and Spark run in containers.

## Quick start

The short version. The [step-by-step guide](#first-run-step-by-step) below explains each command and what it should print.

```bash
make build                            # build the images (about 5 minutes the first time)
make up                               # start the ERP and the partner API, wait until healthy
make reference SKIP_DOWNLOAD=1        # reference data from the committed dump extract (optional)
make run-pending                      # reference data if not loaded yet, then W1, W2 and W3 in order
make verify-rerun WINDOW=2026-10-W1   # re-run a historical window, check nothing changed
make verify                           # check the invariants, which hold on any data
make verify-v1                        # check the v1 baseline numbers of the provided data
make verify-v2                        # check the v2 baseline numbers (gold)
make verify-v3                        # check the v3 baseline numbers (catalog and graph)
make pyspark                          # query the tables (see the usage example)
```

`make help` lists every target.

## First run, step by step

The times below were measured on a laptop with Docker Desktop. They depend on your network and CPU.

### 1. Check the prerequisites

You need Docker with Compose v2, GNU Make and Bash. Nothing else is installed on the host. Give Docker at least 3 GB of memory (in Docker Desktop: Settings, Resources): the pipeline uses up to 2 GB and each source service up to 512 MB.

```bash
docker compose version   # must print v2.x
make --version
```

### 2. Get the code

```bash
git clone <repository-url>
cd data-engineer-challenge
```

No configuration is needed. If port 5433 or 8000 is already taken on your machine, copy `.env.example` to `.env` and change `ERP_DB_HOST_PORT` or `PARTNER_API_HOST_PORT`.

### 3. Build the images

```bash
make build
```

This builds the partner API image and the pipeline image (Python 3.11, Java 21, PySpark, Delta Lake and GraphFrames). The first build takes about 5 minutes, most of it downloading PySpark (a 455 MB package that bundles Spark's own JARs). Later builds reuse the downloads and take seconds. When a dependency changes, a build downloads only that dependency.

### 4. Start the sources and check them

```bash
make up              # the ERP and the partner API; the first start seeds the ERP in about 20 seconds
make check-sources   # ERP seed counts, the pipeline's read-only role and the partner API contract
make smoke           # optional: Spark, Delta, the ERP over JDBC and the partner API, within 2 GB
```

`make ps` should show `erp-db` and `partner-api` as `healthy`. The partner API's interactive docs are at http://localhost:8000/docs.

### 5. Load the reference data

The Open Food Facts dump and the Open Prices baseline are loaded once, before the first window. Choose one of these commands:

```bash
make reference SKIP_DOWNLOAD=1   # the committed 18-row extract of the dump: about 2 minutes
make reference                   # the full 1.5 GB dump, downloaded once into landing/: about 11 minutes
```

- **Either load works:** every later step and every acceptance check passes with either one.
- **Fallback status:** `SKIP_DOWNLOAD=1` uses a committed file instead of the source, so the run ends as `SUCCESS_WITH_FALLBACK`. Running `make reference` later downloads the full dump.
- **Skipping this step:** `make run-pending` in the next step loads the reference data itself, with the full download, if it was never loaded.
- **Without internet:** the pipeline also calls the public Open Food Facts and Open Prices APIs. When they cannot be reached, the run uses the fixtures committed in [fallback/](fallback/) and also ends as `SUCCESS_WITH_FALLBACK`.

### 6. Run the three windows

```bash
make run-pending
```

This runs W1, W2 and W3 in order, each in its own process and each in about 1.5 minutes. Every window goes through bronze, silver, gold, the graph and the catalog.

- **Output:** each window prints a line such as `run 2026-10-W1: SUCCESS (run 20261116T211225Z-af7fadc6)`, then its rows per table: read, written, quarantined, filtered and superseded.
- **One window at a time:** `make run WINDOW=2026-10-W1` runs a single window, then W2 and W3 the same way. A window runs only after every earlier window is complete.

### 7. Check the state

```bash
make status
```

The reference data shows as `SUCCESS` (or `SUCCESS_WITH_FALLBACK`), and every window as `complete` with its last run. The command then lists the rows of every table and partition.

### 8. Re-run a historical window

```bash
make verify-rerun WINDOW=2026-10-W1
```

This re-runs W1 after W3 and compares every table before and after, in about 2.5 minutes. It must end with:

```text
Re-running 2026-10-W1 left every bronze, silver, gold, graph and catalog table identical, and added no commit to any derived table except gold.pipeline_health and catalog.freshness.
```

### 9. Check the invariants and the baseline numbers

```bash
make verify      # invariants, on any data: 36 checks
make verify-v1   # baseline numbers of sources, bronze and silver: 21 checks
make verify-v2   # baseline numbers of gold: 15 checks
make verify-v3   # baseline numbers of the catalog and the graph: 10 checks
```

Each command prints one line per check, `ok` or `FAIL`, with the measured and the expected value. It ends with a line such as `36 of 36 checks passed.`

The two kinds of checks answer different questions:

- **Invariants (`make verify`):** properties that every load must satisfy, whatever the data and the window calendar. Each one compares the lakehouse with itself: silver with bronze, gold with silver, health with reconciliation, the catalog with the Delta tables, and the graph with its own rules. A failure is always a defect. They are the checks to run after any window, including new windows or new source data.
- **Baseline (`make verify-v1/v2/v3`):** the exact numbers of the provided dataset, W1, W2 and W3 of the legacy sources as delivered. They were computed from the deterministic generators of those sources before the pipeline existed, so they are an independent reference and a regression test.
  - They run only when the environment is that dataset: the files under `legacy_sources/` match the provided versions, and the window calendar is the baseline's.
  - Anywhere else they print `Not applicable` with the reason, and exit without failing.
  - Values that depend on live public data, such as price coverage, are reported but not checked.

### 10. Explore the results

`make pyspark` opens a shell to query every table; the next section is a worked example. The Delta tables are also on disk, under `lakehouse/<layer>/<table>/`, and the source files as received are under `landing/`.

The shell also holds the supply network and product graph as a GraphFrame named `g`, for graph questions beyond the result tables; see [Ad hoc graph queries](#ad-hoc-graph-queries).

### 11. Stop, or start over

```bash
make down                  # stop the sources; the ERP data, landing/ and lakehouse/ are kept
make clean                 # also delete the ERP data; the next `make up` seeds it again
rm -rf lakehouse landing   # delete the pipeline output to run everything from scratch
```

Deleting `landing/` also deletes the downloaded dump, so the next full reference load downloads it again.

To run the same windows through Prefect, with retries and a run view, see [Orchestration with Prefect](#orchestration-with-prefect-optional).

## Usage example

`make pyspark` opens a Python shell with a Spark session named `spark`, every table registered as `<layer>.<table>`, and the graph as the GraphFrame `g`. The queries below answer one question per area and can be pasted into the shell as they are. Their output is from the real data after W1, W2 and W3 with the full dump. More queries are in [docs/queries.md](docs/queries.md).

```bash
make pyspark
```

**Which gold tables exist?** The catalog describes every table:

```python
spark.sql("""
    SELECT table_name, scope, description
    FROM catalog.datasets
    WHERE layer = 'gold'
    ORDER BY table_name
""").show(truncate=60)
```

```text
+--------------------------+------+------------------------------------------------------------+
|                table_name| scope|                                                 description|
+--------------------------+------+------------------------------------------------------------+
|      gold.inventory_daily|TENANT|Daily inventory movements per tenant, store and category,...|
|   gold.inventory_position|TENANT|Net inventory position per tenant, store and product sinc...|
|      gold.pipeline_health|TENANT|Pipeline health per window, tenant and silver table: coun...|
|gold.pos_promo_performance|TENANT|Promotion performance per tenant, store, currency and cam...|
|      gold.pos_sales_daily|TENANT|Store-level sales per day, product and currency, with pub...|
|       gold.reconciliation|TENANT|Every source row accounted for, per window, tenant and so...|
+--------------------------+------+------------------------------------------------------------+
```

**How much did each tenant sell?** Revenue is always grouped by currency:

```python
spark.sql("""
    SELECT tenant_id, currency, SUM(sale_lines) AS sale_lines, SUM(units_sold) AS units_sold,
           ROUND(SUM(net_revenue_cents) / 100, 2) AS net_revenue
    FROM gold.pos_sales_daily
    GROUP BY tenant_id, currency
    ORDER BY tenant_id, currency
""").show(truncate=False)
```

```text
+--------------+--------+----------+----------+-----------+
|tenant_id     |currency|sale_lines|units_sold|net_revenue|
+--------------+--------+----------+----------+-----------+
|TENANT_CPG_002|MXN     |8548      |51288     |461104.96  |
|TENANT_RTL_003|EUR     |5700      |39900     |351760.72  |
|TENANT_RTL_004|USD     |8548      |68404     |600444.58  |
+--------------+--------+----------+----------+-----------+
```

The 22,796 sale lines are the active sales in silver. Shelf audits and the load-test sandbox are not sales.

**Which supply nodes are bottlenecks?** The graph counts the stores that depend on each node, directly or through other nodes:

```python
spark.sql("""
    SELECT node_id, tenant_id, dependent_stores, store_share, dependent_tenants, is_bottleneck
    FROM graph.bottlenecks
    ORDER BY dependent_stores DESC, node_id
    LIMIT 5
""").show(truncate=False)
```

```text
+-----------------+--------------+----------------+-----------+-----------------+-------------+
|node_id          |tenant_id     |dependent_stores|store_share|dependent_tenants|is_bottleneck|
+-----------------+--------------+----------------+-----------+-----------------+-------------+
|DC_CENTRAL_HUB_01|TENANT_CPG_001|72              |0.6000     |4                |true         |
|PLANT_NA_01      |TENANT_CPG_001|72              |0.6000     |4                |true         |
|DC_EU_HUB_03     |TENANT_RTL_003|24              |0.2000     |1                |false        |
|DC_SOUTH_HUB_02  |TENANT_CPG_002|24              |0.2000     |1                |false        |
|DC_WEST_HUB_04   |TENANT_RTL_004|24              |0.2000     |1                |false        |
+-----------------+--------------+----------------+-----------+-----------------+-------------+
```

Of the 120 stores, 72 (60%), from four tenants, depend on `DC_CENTRAL_HUB_01` and on the plant behind it.

**Which stores depend on that hub, and at how many hops?** Graph questions that the result tables do not answer run on `g`, the GraphFrame of `graph.nodes` and `graph.edges`. This pattern finds every store that reaches `DC_CENTRAL_HUB_01` in 1 to 3 supply hops:

```python
from pyspark.sql import functions as F

(
    g.filterEdges("edge_type = 'SUPPLIED_BY'")
    .find("(s)-[e*1..3]->(hub)")
    .where("s.node_subtype = 'RETAIL_STORE' AND hub.id = 'DC_CENTRAL_HUB_01'")
    .groupBy(F.col("_hop").alias("hops"), F.col("s.tenant_id").alias("store_tenant"))
    .agg(F.countDistinct("s.id").alias("stores"))
    .orderBy("hops", "store_tenant")
    .show(truncate=False)
)
```

```text
+----+--------------+------+
|hops|store_tenant  |stores|
+----+--------------+------+
|1   |TENANT_CPG_001|30    |
|1   |TENANT_CPG_002|6     |
|1   |TENANT_RTL_003|6     |
|1   |TENANT_RTL_004|6     |
|2   |TENANT_RTL_004|24    |
+----+--------------+------+
```

- **Reading the pattern:** edges point from the dependent node to the node it depends on, so `(s)-[e*1..3]->(hub)` reads "store `s` depends on `hub` through 1 to 3 edges". `s` and `hub` are columns holding each vertex, and GraphFrames adds `_hop`, the number of edges in the path.
- **Result:** the same 72 stores as `graph.bottlenecks`: 48 supplied by the hub directly, and 24 of `TENANT_RTL_004` through its own `DC_WEST_HUB_04`.

**How much do those stores sell?** A graph result is a DataFrame, so it joins with any lakehouse table in the same session:

```python
stores = (
    g.filterEdges("edge_type = 'SUPPLIED_BY'")
    .find("(s)-[e*1..3]->(hub)")
    .where("s.node_subtype = 'RETAIL_STORE' AND hub.id = 'DC_CENTRAL_HUB_01'")
    .select(F.col("s.id").alias("location_id"))
    .distinct()
)

(
    stores.join(spark.table("gold.pos_sales_daily"), "location_id")
    .groupBy("currency")
    .agg(
        F.countDistinct("location_id").alias("stores"),
        F.round(F.sum("net_revenue_cents") / 100, 2).alias("net_revenue"),
    )
    .orderBy("currency")
    .show(truncate=False)
)
```

```text
+--------+------+-----------+
|currency|stores|net_revenue|
+--------+------+-----------+
|EUR     |12    |351760.72  |
|MXN     |6     |156550.17  |
|USD     |18    |600444.58  |
+--------+------+-----------+
```

Sales were recorded at 48 of the 120 stores, 36 of them behind this hub. The revenue is that of every tenant selling at those stores; add a filter on `tenant_id` for one tenant's view. More graph queries (paths, algorithms) are in [Ad hoc graph queries](#ad-hoc-graph-queries) and [docs/queries.md](docs/queries.md#ad-hoc-queries-with-graphframes).

**Is every source row accounted for?** Reconciliation follows each row from the source to silver:

```python
spark.sql("""
    SELECT window_id, SUM(source_rows) AS source_rows, SUM(bronze_rows) AS bronze_rows,
           SUM(silver_rows) AS silver_rows, SUM(quarantined) AS quarantined,
           SUM(filtered) AS filtered, SUM(superseded) AS superseded,
           SUM(unexplained_rows) AS unexplained_rows
    FROM gold.reconciliation
    GROUP BY window_id
    ORDER BY window_id
""").show(truncate=False)
```

```text
+----------+-----------+-----------+-----------+-----------+--------+----------+----------------+
|window_id |source_rows|bronze_rows|silver_rows|quarantined|filtered|superseded|unexplained_rows|
+----------+-----------+-----------+-----------+-----------+--------+----------+----------------+
|2026-10-W1|125369     |125369     |52126      |127        |72500   |616       |0               |
|2026-10-W2|100365     |100365     |41899      |102        |58000   |364       |0               |
|2026-10-W3|85364      |85364      |35785      |79         |49500   |0         |0               |
|2026-11-14|4912665    |18         |0          |0          |4912647 |18        |0               |
|baseline  |92         |92         |86         |0          |6       |0         |0               |
+----------+-----------+-----------+-----------+-----------+--------+----------+----------------+
```

- **Reading the columns:** every row read from a source is in bronze, and `unexplained_rows` is 0 in every window. Each bronze row ends in exactly one place: silver, quarantine, `filtered` or `superseded`. In the windows, `filtered` is the load-test sandbox and `superseded` holds older revisions and snapshots.
- **Reference rows:** `2026-11-14` is the dump snapshot: 4,912,665 products read and only the 18 with an ERP barcode kept. `baseline` is the Open Prices baseline. With `SKIP_DOWNLOAD=1`, the dump row shows the extract's capture date and its 18 rows.

Leave the shell with `exit()`.

## Services

| Service | What it runs | Access from the host | Memory limit |
| --- | --- | --- | --- |
| `erp-db` | Legacy ERP: PostgreSQL 16 seeded from `legacy_sources/erp_db/init_erp.sql` | `localhost:5433`, database `arkham` | 512 MB |
| `partner-api` | Legacy partner event gateway from `legacy_sources/partner_api/main.py` | `http://localhost:8000` (interactive docs at `/docs`) | 512 MB |
| `pipeline` | PySpark and Delta Lake runtime, started on demand | None | 2 GB |

The legacy sources are used exactly as provided: the SQL dump is mounted as the Postgres initialization script and the gateway is copied into its own image. Nothing under `legacy_sources/` is modified.

Source services listen on `127.0.0.1` only. The ERP uses host port 5433 by default because 5432 is often taken by a local PostgreSQL.

### Querying the ERP

- `make psql` opens a shell as `arkham_reader`, the read-only role the pipeline uses.
- From a desktop client: `localhost:5433`, database `arkham`, user `arkham_reader`, password `arkham_reader`. The admin user is `arkham` / `arkham`.
- The ERP data volume survives `make down`. `make clean` deletes it, and the next `make up` seeds it again in about 20 seconds.

## Configuration

Every setting has a default, so a clean clone needs no configuration. To override ports, credentials or Spark sizing, copy `.env.example` to `.env` and edit it.

The window calendar and the public-source settings live in [config/pipeline.toml](config/pipeline.toml), mounted into the pipeline container, so editing it needs no rebuild.

## Running the pipeline

| Command | What it does |
| --- | --- |
| `make run WINDOW=<id>` | Runs one window: bronze, silver, gold, the graph, then the catalog |
| `make run-pending` | Loads the reference data if it never succeeded, then every pending window in order, each step in its own process so one window's memory never carries into the next; stops at the first failure |
| `make reference` | Loads the Open Food Facts dump and the Open Prices baseline. `SKIP_DOWNLOAD=1` uses the committed dump extract; `FORCE=1` downloads the dump again |
| `make verify-rerun WINDOW=<id>` | Re-runs a window and compares the row count and content hash of every bronze partition, silver and gold table and the quarantine before and after; it also checks that no gold, graph or catalog table received a commit, except `gold.pipeline_health` and `catalog.freshness` (the run columns of the window that ran) |
| `make verify` | Checks the invariants: properties that hold on any data and any window calendar, so a failure is always a defect |
| `make verify-v1`, `make verify-v2`, `make verify-v3` | Check the baseline numbers of each version: the exact values of the provided dataset. On other data or another calendar they report `Not applicable` |
| `make rebuild-silver` | Recreates silver and the quarantine from bronze, replaying every complete window in order, then recomputes gold, the graph and the catalog; bronze and landing are untouched |
| `make rebuild-gold` | Recomputes gold, the graph and the catalog from silver; only rows that changed are written |
| `make status` | State of each window, last run, and row counts per table and partition |
| `make fingerprint` | Row count and content hash per table and partition, as JSON |
| `make pyspark` | Interactive PySpark with every table registered as `<layer>.<table>` and the graph as a GraphFrame `g`; example queries are in [docs/queries.md](docs/queries.md) |

How windows behave:

- **Calendar as configuration:** each window has an ID, a UTC start (included) and end (excluded). Windows must be contiguous, so every record falls in exactly one. To add a window, append it to `config/pipeline.toml`; no code changes. Running a window that is not in the calendar changes nothing and exits with code 2. The error lists the calendar and prints the entry to append, contiguous with the last window and as long as it. If the ID differs from a known window only in letter case, it suggests that window instead.
- **Chronological order:** a window runs only after every earlier window is complete. A window is complete when its latest run succeeded.
- **Re-runs:** any historical window can be re-run at any time. API responses are read from the landing zone instead of calling the APIs again, and the ERP is read again for the same snapshot or range, so the window's partitions come out identical. Only the run ID and load time change.
- **Interruptions:** a run that fails or is killed leaves its window pending, and later windows are blocked until it is re-run.
- **Empty windows:** a window with no source data finishes with status `SUCCESS` and zero rows.

## Bronze layer

Bronze stores each source as received: the original responses and files in the landing zone, and their content in Delta tables. No business filter is applied, so the load-test sandbox and invalid records are kept.

| Table | Source | Partition | Load |
| --- | --- | --- | --- |
| `bronze.erp_tenants`, `erp_locations`, `erp_products` | ERP over JDBC | `_window_id` | Full snapshot per window |
| `bronze.erp_inventory_ledger` | ERP over JDBC | `_window_id` | Rows whose `ingested_at` falls in the window |
| `bronze.partner_events` | Partner API | `_window_id` | By cursor: each window starts at the previous window's end cursor and stops at the first event that arrived after the window |
| `bronze.off_product_api` | Open Food Facts product API | `_window_id` | Snapshot of the ERP barcodes per window |
| `bronze.open_prices` | Open Prices | `_window_id` | Prices created in the window, plus a `baseline` partition with prices dated in the six months before the first window |
| `bronze.off_products` | Open Food Facts dump | `_snapshot_date` | Reference snapshot: eleven columns, only rows whose barcode is in the ERP |

- **Types:** the ERP's column types are kept, with its JSONB columns as text. JSON payloads are stored whole in a `raw_json` text column, with the key fields extracted next to it; dump columns are text. Nothing is inferred; typing happens in silver.
- **Control columns:** `_window_id`, `_run_id`, `_loaded_at` (UTC) and `_source` on every row.
- **Writes:** each run replaces only its own partition (Delta `replaceWhere`). Tables are never appended to or overwritten whole.
- **Landing zone (`landing/`):** `<source>/<partition>=<value>/` holds the API response bodies (`responses.jsonl`, one response per line) or the dump file, plus a `_manifest.json` written last with the extraction parameters, cursors and counts. A partition without a manifest counts as not landed.
- **Run log:** `platform.run_log` has one row per run with its status (`RUNNING`, `SUCCESS`, `SUCCESS_WITH_FALLBACK` or `FAILED`) and the fallbacks used; `platform.run_metrics` counts rows per table by outcome: read, written, quarantined, filtered, superseded and flagged.
- **Public sources:** requests carry a custom User-Agent, go out at most once per second and are retried three times, including when a body is truncated JSON, which Open Prices returns now and then. If a source still fails, the run uses an earlier landed response, then the fixture committed in [fallback/](fallback/), then an empty partition, and records which one it used. The committed fixtures contain no Open Prices usernames or user comments.
- **Open Food Facts dump:** a 1.5 GB tab-separated file, downloaded once into the landing zone and reused; an interrupted download resumes where it stopped. It is read as a single task with an all-text schema taken from its own header line, keeping the eleven configured columns and the rows whose barcode is in the ERP. In the verification run the dump held 4,912,665 products and all 18 ERP barcodes were found; the load took about 11 minutes, almost all of it download.

## Silver layer

Silver holds the current state of each entity, typed and resolved by its own key; the history of snapshots and revisions stays in bronze. Each run writes silver right after bronze.

| Table | Key | Current row | Scope |
| --- | --- | --- | --- |
| `silver.dim_tenant` | `tenant_id` | Latest ERP snapshot; `is_sandbox` from `workspace_env` | Tenant |
| `silver.dim_location` | `location_id` | Latest ERP snapshot; `parent_exists` validates the parent supply node | Tenant |
| `silver.dim_product` | `product_id` | Latest ERP snapshot; barcode as `gtin_raw`, `gtin14` and `gtin_status`; low-code attributes typed by the shape of each value | Tenant |
| `silver.fact_inventory_movement` | `ledger_entry_id` | One row per ledger entry of the real tenants, every status kept, `is_posted`, `is_late_arriving` and `is_reference_doc_reused` flags | Tenant |
| `silver.fact_pos_sale` | `event_id` | Newest revision of each sale; discount as a fraction, promotion columns, dates in UTC | Tenant |
| `silver.fact_shelf_audit` | `event_id` | Newest revision of each shelf audit (units observed, shelf price) | Tenant |
| `silver.ref_product_public` | `gtin14` | Most recently edited version between the dump and the product API; missing attributes filled from the other source | Global |
| `silver.ref_public_price` | `id` | Newest version of each Open Prices price, in cents, without duplicates or personal data | Global |
| `platform.quarantine` | source table, record key | Records that failed a rule, with every reason, the target table and the raw record | Platform |

- **Writes:** `MERGE` on the key, updating a row only when its content hash changed. Dimensions and public references are recomputed whole from bronze, so re-running an earlier window never brings back an older snapshot.
- **Revisions:** an event's current version is its highest (`revision`, `sequence_cursor`) seen in any window, compared against what silver already holds in both fact tables and in quarantine. Re-running an earlier window never reverts a later correction. If the current version fails a rule, the whole event goes to quarantine and the earlier revision is not restored; if it passes, any quarantined version is released.
- **Tenancy:** a fact's `tenant_id` is the tenant that recorded it; `location_owner_tenant_id` and `product_owner_tenant_id` come from the ERP snapshot of the fact's own window. Facts of the sandbox, found through `dim_tenant.workspace_env`, are excluded; facts of an unknown tenant go to quarantine.
- **Rules that quarantine:** negative or missing price, unparseable dates, unknown event type, a discount that is not a fraction between 0 and 1, an empty or non-numeric barcode, and an unknown tenant. A barcode with a wrong check digit is only flagged.
- **Money:** integer cents next to a currency column, never added across currencies. The ERP declares no currency for its product prices, so `dim_product.currency` is null rather than guessed.

On the real data, after W1, W2 and W3: 100,000 inventory movements (88,408 posted), 23,554 sales (22,796 active, 758 cancelled), 5,888 shelf audits, 308 events in quarantine for a negative price and 250 superseded revisions, matching every v1 number in the PRD (`make verify-v1`).

## Gold layer

Gold answers the three business domains of the assessment: inventory, POS sales and pipeline health. Each run recomputes gold in full from silver as its last stage, so a late record or a revision that moves a sale to another store or date needs no special handling.

| Table | Grain | Content |
| --- | --- | --- |
| `gold.inventory_position` | tenant, store, product | Inbound, outbound and net units since the first window, movement count, last movement, category and the store's `operating_status` |
| `gold.inventory_daily` | tenant, store, category, business date | Inbound, outbound and net units, movement count, the period (calendar window) the day belongs to and that period's velocity: outbound units divided by its days |
| `gold.pos_sales_daily` | tenant, store, product, currency, sale date | Sale lines, units, gross, discount and net revenue in cents, promotion columns, public product attributes and the public price reference |
| `gold.pos_promo_performance` | tenant, store, currency, campaign, mechanic | Sale lines, units, revenue, discount, weighted average discount rate and co-funded discount; `NONE` for lines without a promotion |
| `gold.reconciliation` | window, tenant, source object | Source, bronze and silver rows, quarantined, filtered, superseded, flagged, and `unexplained_rows`, which must be 0 |
| `gold.pipeline_health` | window, tenant, silver table | The same counts per table, quarantine reasons, late, reused-document and tenant-mismatch rows, barcodes by status, public price coverage, and the run's status, duration and fallbacks |

- **What counts:** posted ledger entries and `ACTIVE` sales only. `quantity_delta` keeps its sign, and no source gives an opening stock, so positions are the net change since the first window and can be negative. Shelf audits never count as sales.
- **Money:** net revenue is rounded half up to the cent on each sale line and then added. Every amount is grouped by currency; there is no conversion.
- **Dates:** metrics are aggregated by the UTC business date of the event, so a late record lands on the day it happened. Velocity periods and their length come from the window calendar.
- **Public price:** a reference only, never used to fill a sale. It is the latest Open Prices price for the same GTIN-14 and currency, dated on or before the sale date and at most six months earlier (`[gold]` in `config/pipeline.toml`). A price from the store's country (`COUNTRY`) beats a more recent one from another country (`CURRENCY`); otherwise the match level is `NONE`.
- **Sandbox:** the business tables hold no sandbox rows. The health tables show them as filtered.
- **Reconciliation from state:** every bronze row is classified on its own (current in silver, quarantined, filtered or superseded by a newer version) and attributed to the window it arrived in, so the figures do not depend on run order. `source_rows` is the count taken from the source at extraction time and stored per tenant in `platform.run_metrics`. For reference loads, `window_id` holds the bronze partition: `baseline` for the Open Prices baseline and the snapshot date for the dump. `gold.pipeline_health` aggregates the same classified rows, so its counts always add up to reconciliation's.
- **Writes:** each table is synchronised with its recomputed result. Rows that changed are merged, rows that disappeared are deleted, and unchanged rows are left alone, so re-running a window adds no commit to any gold table except `gold.pipeline_health`. Its run columns (run ID, status, start time, duration) change with every run; they are refreshed with the final values once the run is recorded as finished, and fingerprints leave them out.

## Supply network and product graph

The graph is stored as Delta tables built from the silver dimensions, and traversed with GraphFrames inside the pipeline container; no graph database is added.

| Table | Grain | Answers |
| --- | --- | --- |
| `graph.nodes` | node | Every location (plant, distribution center, store) and product of the real tenants, with its owner as `tenant_id` |
| `graph.edges` | source, target, type | `SUPPLIED_BY` (location to parent), `SOURCED_FROM` (product to supplier plant), `SUBSTITUTED_BY` (product to substitute), each from the dependent node to the node it depends on, with `is_cross_tenant` |
| `graph.upstream_dependencies` | store, upstream node | Which plants and distribution centers a store depends on, and at how many hops |
| `graph.bottlenecks` | supply node | Dependent stores, store share, dependent tenants and sourced products; `is_bottleneck` when the store share reaches the threshold (50%, `[graph]` in `config/pipeline.toml`) |
| `graph.cross_tenant_dependencies` | edge | Every edge where one tenant's node depends on another tenant's node |
| `graph.substitution_paths` | product, reachable substitute | Hops, the ordered path, whether it crosses tenants or categories, and the recommended substitute (the nearest one not discontinued) |

- **Traversals:** GraphFrames shortest paths to a set of landmark nodes, following edge direction. Each node is reached once at its shortest distance, so a cycle cannot loop, and results stop at a configurable depth (25 hops).
- **Isolation:** every result row carries the owner of its starting node as `tenant_id`. Other tenants' nodes appear only by ID, owner and category, never with cost or price.
- **Integrity:** an edge whose target is not a node is not created and is counted in the run log as a dangling reference (none in the current data). Sandbox nodes are excluded.
- **Writes:** recomputed on every run and synchronised like gold, so the same dimensions always give the same graph and a run with no changes writes nothing.

### Ad hoc graph queries

The four result tables answer the assessment's questions with plain SQL. Any other graph question runs on `g`, the GraphFrame that `make pyspark` builds from `graph.nodes` and `graph.edges`. Its vertex `id` is `node_id`, and its edges go from `src` to `dst`, from the dependent node to the node it depends on. Every result is a DataFrame, so it can be joined with any lakehouse table in the same session. [docs/queries.md](docs/queries.md#ad-hoc-queries-with-graphframes) has examples.

| Question | GraphFrames tool |
| --- | --- |
| A pattern of a few hops, such as stores behind a distribution center at 1 to 3 hops | `g.find("(s)-[e*1..3]->(dc)")` |
| A path between two sets of nodes | `g.bfs(...)` for the shortest paths, `g.all_paths(...)` for every simple path |
| Graph plus business data, such as the revenue of the stores behind a node | A pattern or path result joined with `gold.*` |
| Graph algorithms | `pageRank`, `connectedComponents`, `stronglyConnectedComponents`, `labelPropagation`, `detectingCycles`, `k_core`, `triangleCount`, `pregel` |

- **Measured on the real data:** every tool above answered in 0.2 to 6 seconds. A variable-length pattern returned the same 72 stores behind `DC_CENTRAL_HUB_01` and the same 2,080 substitution pairs as the result tables.
- **Long paths:** GraphFrames turns each length of a variable-length pattern into a chain of joins, so a 1-to-20-hop pattern is a large query; it added 0.4 GB to the session. For long paths, use `bfs` or `all_paths`, which are iterative.
- **Checkpoints:** `connectedComponents`, `detectingCycles` and `k_core` need a checkpoint directory. The shell sets a temporary one outside the lakehouse.
- **Known issue:** in GraphFrames 0.12.3, the Python `triangleCount()` requires its storage level, as in `g.triangleCount(StorageLevel.MEMORY_AND_DISK)`.
- **Memory:** a long session grows to about 1.5 GB of the 2 GB limit as the JVM fills its 1 GB heap. Avoid caching large DataFrames; if the JVM runs out of memory, the shell exits and no table is affected.
- **Tenancy:** ad hoc queries see every tenant. A query for one tenant filters on `tenant_id`.

## Data catalog

The catalog is four Delta tables generated at the end of every run from a declarative registry, [config/catalog.toml](config/catalog.toml). The registry declares what each table is; column names and types are read from the Delta tables themselves.

| Table | Grain | Content |
| --- | --- | --- |
| `catalog.datasets` | table | Description, layer, scope, isolation column, key, partitioning, load and write pattern, personal data and the source objects it comes from |
| `catalog.columns` | table, column | Name, type, nullability and description, with flags for key, tenant, partition and personal data |
| `catalog.lineage` | upstream, downstream | Table-level lineage from source objects to bronze, silver, gold and graph, with the transformation |
| `catalog.freshness` | table, window | The run that processed the window, its status and load time, rows, fallback use, and for global tables the snapshot date of their public sources |

- **Registry and reality agree:** a Delta table without a registry entry fails the run, and so does a registered table missing at the end of a window run (except the reference-load tables before the reference data is loaded). The registry and `src/arkham/lakehouse.py` must declare the same tables and scopes.
- **Personal data:** only `bronze.open_prices.raw_json` is flagged; it holds the Open Prices `owner` fields. The catalog stores metadata only, never values.
- **Freshness:** run columns and load times change with every run, so fingerprints leave them out; they are refreshed with the final values once the run is recorded as finished.
- **Queries:** [docs/queries.md](docs/queries.md) has example queries for which tables exist, a table's schema, where it comes from and how fresh it is, plus the graph's traversal questions.

## Orchestration with Prefect (optional)

An optional layer on top of the CLI that adds retries and a run view. It changes nothing in the pipeline: the pipeline image, `make up`, `make build` and every other target work the same with or without it, and removing `orchestration/`, the compose profile `orchestration` and the `prefect-*` targets removes it.

```bash
make prefect-build                     # build the orchestrator image (pipeline runtime plus Prefect)
make prefect-up                        # start the Prefect server; run view at http://localhost:4200
make prefect-pending                   # reference data if needed, then pending windows, as a flow
make prefect-run WINDOW=2026-10-W1     # run or re-run one window as a flow
make prefect-verify WINDOW=2026-10-W1  # re-run a window as a flow and check every table is unchanged
make prefect-test                      # test the flows against a fake CLI (no Spark, no sources)
make prefect-down                      # stop the Prefect server (its run history is kept)
```

- **What the flows do:** [orchestration/flows.py](orchestration/flows.py) wraps the CLI. Each step (`reference`, `run --window`) is a Prefect task that runs `python -m arkham ...` in its own process, as `make run-pending` does, with the pipeline's own interpreter and settings. The flow process imports Prefect but never Spark.
- **Retries:** a failed step is retried twice, 30 seconds apart, which is safe because every step leaves the tables identical when repeated. A step that still fails stops the flow, and later windows are not run.
- **Run view:** the Prefect UI shows each flow run, its steps, their retries and the CLI output.
- **Isolation:** Prefect has its own project and lock file ([orchestration/pyproject.toml](orchestration/pyproject.toml)), installed into a separate virtualenv (`/opt/prefect`) in its own image target, so its dependencies never mix with the pipeline's. The Prefect server keeps its state in SQLite on the `prefect_data` volume.
- **Memory:** the orchestrator container has the pipeline's 2 GB limit (the flow process and one step's JVM at a time); the Prefect server, an auxiliary service, has 512 MB.

## Pipeline runtime

| Component | Version |
| --- | --- |
| Python | 3.11.18 |
| Java (Eclipse Temurin JRE) | 21.0.12.1 |
| PySpark | 4.1.4 |
| Delta Lake | 4.4.1 (`delta-spark_4.1_2.13`) |
| PostgreSQL JDBC driver | 42.7.14 |
| GraphFrames | 0.12.3 (`graphframes-spark4_2.13`, built on Spark 4.1.2) |
| Poetry | 2.5.1 |

- **Spark 4.1:** Delta Lake and GraphFrames, which the graph in v3 uses, publish one build per Spark minor version, and 4.1 is the newest line both are built against.
- **Reproducible builds:** Python packages are locked in `poetry.lock`, and the JARs (Delta, GraphFrames, the JDBC driver and their dependencies) in [docker/pipeline/spark-jars.lock](docker/pipeline/spark-jars.lock), with the URL and SHA-256 of each one; the build downloads and verifies exactly those files. A run never downloads anything. GraphFrames' Python package imports `typing_extensions` and `pyspark.ml` (which needs `numpy`) without declaring them, so both are pinned explicitly.
- **UTC everywhere:** the JVM default time zone, the Spark session and every JDBC connection to the ERP are set to UTC.
- **Memory:** the container is limited to 2 GB. The JVM heap is 1 GB (`SPARK_DRIVER_MEMORY`); metaspace, code cache and direct buffers are capped; the JVM sizes its threads for the two cores Spark uses (`local[2]`); Spark's per-query thread pools are capped; no Python worker process is started; and `make run-pending` runs each window in a fresh process. The JVM exits on `OutOfMemoryError` instead of hanging, which leaves the window pending for a re-run. Measured peaks (`docker stats`) in a run from a clean clone with every layer (bronze to catalog): 0.9 GB for the reference load with the committed dump extract, 1.0 GB for `make run-pending` (W1, W2 and W3) and 1.0 GB for `make verify-rerun`. The reference load with the full 1.5 GB dump (4,912,665 products read, then every derived layer) peaked at 1.0 GB. While the dump is written to disk the kernel's file cache fills the rest of the limit; that cache is reclaimed on demand and causes no OOM.
- **Output:** the pipeline writes to `landing/` (source files as received) and `lakehouse/` (Delta tables), both bind-mounted and git-ignored. The container runs with the host user's UID, so these files stay owned by the host user.

## Repository layout

```text
config/            window calendar and settings (pipeline.toml), catalog registry (catalog.toml)
docker/
  erp_db/          read-only ERP role for the pipeline
  partner_api/     image for the legacy partner gateway
  pipeline/        pipeline image and build-time JAR resolution
docs/              PRDs and example queries
fallback/          committed fixtures used when a public source fails
orchestration/     optional Prefect flows, with their own pyproject and lock file
legacy_sources/    sources as provided by the assessment, unmodified
scripts/           operational checks and fixture capture
src/arkham/        pipeline package
  bronze/          one loader per source
  silver/          normalizers, dimensions, facts, references and quarantine
  gold/            inventory, sales, reconciliation and pipeline health
  graph/           nodes, edges and GraphFrames traversals
  catalog/         registry reader and catalog tables
tests/             unit and end-to-end tests with in-memory sources; no network
```

## Development

- `src/`, `tests/`, `config/` and `fallback/` are mounted into the pipeline container, so changes to them need no rebuild. Rebuild with `make build` after changing dependencies.
- `make test`, `make lint` and `make format` run pytest, black and ruff inside the pipeline container. `make test` starts one pytest process, and so one Spark JVM, per test file ([scripts/run_tests.py](scripts/run_tests.py)): every test window runs all five layers, and a single JVM for the whole suite runs out of heap. `make test ARGS="tests/test_gtin.py"` or `ARGS="-k name"` narrows the run. Both tools exclude `legacy_sources/`, so formatting never rewrites the provided sources.
- To change a Python dependency, edit `pyproject.toml`, run `make lock` to regenerate `poetry.lock`, then `make build`. To change a JAR, edit `src/arkham/spark_packages.py` (Delta and GraphFrames must stay on the same Spark minor version as `pyspark`), run `make lock-jars`, then `make build`; the build fails while the lock is stale. A new PySpark version also needs `PYSPARK_VERSION` in `docker/pipeline/Dockerfile`.
- Build speed: PySpark is its own image layer, keyed only on its version; the JARs are a separate stage built in parallel; and BuildKit cache mounts keep pip, Poetry and JAR downloads between builds. A rebuild after a dependency change downloads only what changed.

## Troubleshooting

| Symptom | Fix |
| --- | --- |
| `make up` fails with "port is already allocated" | Set `ERP_DB_HOST_PORT` or `PARTNER_API_HOST_PORT` in `.env` |
| The pipeline container exits with code 137 | Docker has less memory than the containers need; give Docker at least 3 GB |
| Permission errors in `landing/` or `lakehouse/` on Linux | Run through `make`, which builds the image with your UID; or run `make build` again after changing users |
