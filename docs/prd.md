# PRD: Arkham Data Platform

Nov 13, 2026 · @Ana · updated Nov 16, 2026 to describe only what is built

## Summary and context

The deliverable is a multi-tenant lakehouse in PySpark and Delta Lake that processes three ingestion windows (`2026-10-W1`, `W2`, `W3`) incrementally and idempotently, with a data catalog and a supply-network graph.

Arkham is a low-code platform for the CPG and Retail industry. Several organizations share infrastructure and each one extends its data model, so data varies across tenants and changes over time.

Reviewers will clone the repository, run W1, W2 and W3 in order, re-run a historical window, and inspect the Delta tables, the catalog, the graph and the repository structure. They evaluate it as a production deliverable.

## Goals and non-goals

**Goals**

- A clean clone brings up the whole environment, and a single command parameterized by window runs any window, including ones added after W3, without code changes.
- Re-running any window leaves the tables identical, with no duplicates and no loss of later corrections.
- The gold layer answers the three business domains: inventory, POS sales and pipeline health.
- Every source anomaly is detected, counted and explained in the reconciliation.
- The catalog and the graph are queried as Delta tables.

**Non-goals**

- Streaming or real-time processing.
- User interface or dashboards for tenants.

## Users and use cases

| User | What they need | Where they get it |
| --- | --- | --- |
| Analyst at a CPG or Retail tenant | Net inventory, movement velocity, revenue and discounts by store | Gold tables filtered by their tenant |
| Low-code user or automated service | Which tables exist, their schema, origin and freshness | Data catalog |
| Supply chain planner | Bottlenecks, cross-tenant dependencies, substitutes for an out-of-stock product | Graph tables |
| Platform engineer | What was loaded, what was rejected and why, by tenant and window | Health and reconciliation tables |
| Assessment reviewer | Run from scratch and verify consistency | README, Makefile and tests |

## Data sources

The two legacy sources are profiled and run with Docker Compose; the Open Food Facts dump is profiled from its documentation, and the public APIs are profiled with real responses. The README's four sources map to five extraction interfaces and eight bronze tables.

| Source | Type | Volume | Status |
| --- | --- | --- | --- |
| ERP and inventory | PostgreSQL, schema `arkham_erp` | 5 tenants, 128 locations (120 stores, 4 distribution centers, 3 plants, 1 virtual buffer), 200 products, 250,000 movements | Profiled |
| Partner API | FastAPI, `GET /v1/events` | 60,000 POS and shelf audit events | Profiled |
| Open Food Facts (dump) | Compressed CSV (tab-separated) or Parquet | 1.54 GB compressed; 4,912,665 products (snapshot of November 14, 2026) | Profiled and loaded; all 18 ERP codes are present in the dump |
| Open Prices and Open Food Facts (REST) | Two public APIs | 212 historical prices for the 3 codes queried; 1 price between October 1 and 15, 2026 across the 2 codes queried (these values can change constantly over time) | Profiled with real responses |

### Findings common to the legacy sources

- The fact's `tenant_id` never matches the tenant that owns the store or the product (100% of real rows).
- `TENANT_DEMO_000` is a load-test sandbox: 60% of the ledger and 50% of the events.
- Barcodes have leading zeros, spaces, a `GTIN:` prefix and 12-digit UPCs. The 200 products reduce to 18 real codes.
- Windows are assigned by ingestion date: W1 from October 1 to 5, W2 from 6 to 10, W3 from 11 to 15.
- There are late events and corrections that arrive in a later window than the original record.

### ERP: findings by table

| Table | Finding | Count |
| --- | --- | --- |
| `locations` | Stores that depend, directly or transitively, on `DC_CENTRAL_HUB_01` and `PLANT_NA_01` | 72 of 120 |
| `locations` | Stores whose distribution center belongs to another tenant | 18 |
| `locations` | Stores in `RENOVATION_HOLD` | 4 |
| `products` | Substitutes from another tenant and another category | 199 of 199 |
| `products` | Longest substitution chain, no cycles | 20 hops |
| `products` | Primary supplier from another tenant | 150 of 200 |
| `products` | `lowcode_attributes` with different types per tenant | 4 variants |
| `inventory_ledger` | Real rows per window (W1 / W2 / W3) | 40,000 / 32,000 / 28,000 |
| `inventory_ledger` | Status `POSTED_00`, the only one that counts | 88,408 |
| `inventory_ledger` | Voided / pending / flagged duplicate | 5,264 / 4,119 / 2,209 |
| `inventory_ledger` | Ingested in W2 with event date in W1 | 3,555 |
| `inventory_ledger` | Reuse the `reference_doc_id` of an earlier movement | 1,080 |

The store findings use the 120 stores as denominator, and the three counts (72, 18 and 4) are separate measures that can overlap. Dimensions have no incremental signal: every `updated_at` is from September 2026. All movements happen in stores. A product's `primary_supplier_id` always points to a plant in `locations`, so suppliers are locations and not a separate entity.

### Partner API: findings

| Finding | Count (real events) |
| --- | --- |
| Real events per window (W1 / W2 / W3) | 12,500 / 10,000 / 7,500 |
| Revision 2 of an earlier `event_id`, with a different store and product | 603 |
| Date in local format `YYYY/MM/DD HH:MM:SS-0600` instead of ISO UTC | 3,000 |
| `discount_rate` as text (`"15%"`) plus a `promo_mechanics` object | 13,250 |
| Negative unit price | 310 |
| Status `CANCELLED_VOID` | 966 |
| Shelf audits, all from `TENANT_CPG_001` | 6,000 |
| Barcode equal to the product master's | 1,665 of 30,000 |

The 30,000 real events are 24,000 POS sales and 6,000 shelf audits. Each event is one line for one product; there is no transaction identifier. Counting the sandbox, 706 revision-2 records have no revision 1. Revenue comes in MXN, EUR and USD, and no store has sales in more than one currency. `promo_mechanics` carries a campaign, a mechanic type and a co-funding ratio, but no discount amount, so it cannot conflict with `discount_rate`.

Extraction rules: `page_size` of 500 at most (above that, each record grows by more than 1,100 characters; above 2,000 it returns 429), and never combine `page` with `since_cursor`.

### Public APIs: findings

Public price coverage on the project dates is minimal, so enrichment must use the price history and not only the window.

| Finding | Evidence |
| --- | --- |
| Two products queried for October 1 to 15, 2026 (Nutella and Coca-Cola): one price found, for Nutella, and none for Coca-Cola | Nutella at 4.45 EUR in Nancy, France, on October 6 (W2) |
| 212 historical prices for the three codes queried, 168 of them for Nutella; the other 15 codes are not profiled yet | About one price per week per product |
| Nutella's 168 prices are in a single currency (EUR) and five countries | Only comparable with sales in EUR |
| `price_per` is null for prices of type `PRODUCT` | Cannot be used to filter per-unit prices |
| `owner` is a username in clear text, repeated in the nested `proof.owner`; `owner_comment` and `proof.owner_comment` are the user's free text | Personal data: no `owner*` field, at any depth, leaves bronze |
| Each price nests the full `product`, `location` and `proof` objects | About 100 lines of JSON per price |
| The product API did not return `brand_owner` for Nutella | The brand comes from `brands` and `brands_tags` |
| `nova_group` and `nutriscore_grade` are present in both APIs | They cover the processing classification and the nutritional grade |

Extraction rules: a custom User-Agent, `product_code__in` to request the 18 codes in one call, `type=PRODUCT`, window assigned by `created`, raw response stored per window and reused on re-runs.

## Tenancy and data scope

Every table has a declared scope, and tenant-scoped facts distinguish the tenant that records the fact from the tenants that own the referenced store and product.

| Scope | Tables | Tenant column |
| --- | --- | --- |
| Tenant | Silver facts and ERP dimensions | `tenant_id` required |
| Global public | `ref_product_public`, `ref_public_price` and the bronze tables from public sources | None; registered as `GLOBAL` in the catalog |
| Platform | Run log, quarantine and catalog | `tenant_id` only when the record refers to a tenant's row |

- In facts, `tenant_id` is the tenant that records the fact. It defines isolation.
- Facts also carry `location_owner_tenant_id` and `product_owner_tenant_id`, taken from the dimensions. They differ from `tenant_id` in 100% of real rows, and the mismatch is reported in reconciliation.
- Facts join dimensions by ID only (`location_id`, `product_id`). These IDs are unique across tenants in the ERP, so no composite key is needed.
- The sandbox is identified by joining the fact's `tenant_id` to `dim_tenant` and checking `workspace_env`, which exists only in the ERP `tenants` table. A fact whose tenant is not in `dim_tenant` goes to quarantine.
- This project has no access-control engine. Isolation is expressed through the scope and the tenant columns and is verified with isolation tests. What a tenant may see of other tenants in the graph is decided in v3.

## Functional requirements

| ID | Requirement | Origin | Version |
| --- | --- | --- | --- |
| FR-1 | Ingest the four sources into bronze with the raw data and control metadata: windowed sources once per window, and the Open Food Facts dump once as reference data | README | v1 |
| FR-2 | Process one window at a time, in chronological order | README | v1 |
| FR-3 | Re-run any window without duplicating or reverting corrections from later windows | README | v1 for silver, v2 for gold |
| FR-4 | Silver: normalized types, barcodes in GTIN-14, dates in UTC, numeric discounts, and the current state of each entity resolved by its own key | Profiling | v1 |
| FR-5 | Send records that fail the quality rules to quarantine, with a reason | Profiling | v1 |
| FR-6 | Inventory gold: net position and movement velocity by location and category | README | v2 |
| FR-7 | Sales gold: revenue, units and discounts by store and currency, with public product attributes | README | v2 |
| FR-8 | Health gold: reconciliation and pipeline metrics by tenant and window | README | v2 |
| FR-9 | Catalog: tables, schemas, lineage, freshness per window, scope and tenant isolation | README | v3 |
| FR-10 | Graph: nodes and edges for locations and products, with queries for bottlenecks, cross-tenant dependencies and multi-hop substitution | README | v3 |
| FR-11 | Windows are a parameter: their date range comes from a configurable calendar, a new window is added without code changes, one command runs pending windows in order, and a window with no data finishes without error | Refinement | v1 |

## Non-functional requirements

| ID | Requirement | How it is verified |
| --- | --- | --- |
| NFR-1 | The processing container does not exceed 2 GB of memory, including the Open Food Facts dump load | Limit in Docker Compose and peak memory measured with `docker stats` during a full run |
| NFR-2 | Each auxiliary service does not exceed 512 MB | Limit in Docker Compose and `docker stats` |
| NFR-3 | Python, PySpark and Delta Lake for processing and storage | Code review |
| NFR-4 | Reproducible from a clean clone, with pinned versions | Build and run on a fresh machine |
| NFR-5 | The pipeline waits for the sources to be ready before starting | Compose healthchecks |
| NFR-6 | Every table declares its scope (tenant, global public or platform); tenant-scoped tables carry `tenant_id`, and a query filtered by one tenant returns no rows recorded by another | Isolation tests and the scope registered in the catalog |
| NFR-7 | Public sources do not bring the pipeline down if they fail or rate-limit | Retries, local fallback and the run status |
| NFR-8 | Every run records its status and the row counts by outcome (read, written, quarantined, filtered, superseded, flagged) per table and window | Run log |
| NFR-9 | `legacy_sources/` is not modified | Repository diff |
| NFR-10 | An interrupted run leaves its window incomplete, and running the window again completes it | Interruption test |

## Extraction contract

Each source object has one rule that ties it to the window calendar, and a re-run reads the stored landing files instead of calling the source again.

- **Window calendar:** a configuration table with `window_id`, start and end. Ranges are in UTC, with the start included and the end excluded. The internal ID (`2026-10-W1`) is the only one the pipeline uses.
- **Three separate time fields:** the business time of the record, its arrival time at the source, and `_window_id`. A late record keeps its business date and belongs to the window in which it arrived.
- **Empty window:** it finishes with status `SUCCESS` and zero rows in the run log.

| Source object | Window assignment | Incremental key | Re-run |
| --- | --- | --- | --- |
| ERP dimensions | None: full snapshot | None | Reads the snapshot again and replaces the partition |
| ERP ledger | `ingested_at` within the window range | Range predicate on `ingested_at` | Reads the same range again |
| Partner events | `ingested_at` within the window range | `sequence_cursor` | Reads the landing files |
| Open Food Facts dump | None: reference snapshot | Snapshot date | Never downloaded again unless forced |
| Product API | None: snapshot of the ERP codes per window | None | Reads the landing files |
| Open Prices | `created` within the window range; baseline by `date` from April 1 to September 30, 2026 | `created` | Reads the landing files |

### Partner API cursor

- The first window starts at cursor 0; each later window starts at the end cursor stored for the previous window.
- Pages are requested with `since_cursor`, `page=1` and `page_size=500`, until `has_more` is false or `count` is 0.
- Extraction stops at the first event whose `ingested_at` is at or after the window end; that event and the ones after it belong to later windows.
- The end cursor is stored only after the bronze write succeeds.
- `sequence_cursor` identifies a delivery and `event_id` identifies the business event; they are different keys.

### Public APIs

- One extraction per window makes about 20 HTTP calls: one or two pages of Open Prices, using `product_code__in` for all codes, and one product lookup per code. Re-runs make no calls.
- If a call still fails after three retries, the run uses the stored landing response; if there is none, the fixture committed in the repository; if there is none either, the public tables stay empty for that window.

### Open Food Facts dump under the memory limit

The file is read as a single streaming task with an explicit text schema, selecting the eleven columns and filtering by code on read, with no caching and no repartitioning of the full dataset.

The eleven columns, configured in `config/pipeline.toml`, are `code`, `product_name`, `quantity`, `brands`, `brands_tags`, `brand_owner`, `categories_tags`, `main_category`, `nutriscore_grade`, `nova_group` and `last_modified_t`. They cover the product taxonomy, brand ownership, nutritional grade and processing classification that the README asks for, plus the key and the edit date that `ref_product_public` uses to pick the most recent version.

### Run and window status

| Run status | When |
| --- | --- |
| `SUCCESS` | Every stage finished with live or stored data |
| `SUCCESS_WITH_FALLBACK` | A public source failed and a fixture or an empty result was used; the run log records which source and how old the fallback is |
| `FAILED` | A legacy source or any stage failed |

A window is complete only when all its stages finish. An interrupted run leaves the window incomplete, and running it again repeats the whole window. The run log counts rows per table and window by outcome: read, written, quarantined, filtered, superseded and flagged.

## Bronze design

Bronze stores each source as it arrives, in two places: the original files in a landing zone and their content in Delta tables.

- **Landing zone (`landing/`):** API responses as `.jsonl` and the dump as `.csv.gz`, unmodified. Re-running a window reads from here and does not call the source again.
- **Delta tables:** bronze keeps the types the source declares and infers none. The ERP declares column types, so they are kept; JSON and CSV declare none, so JSON is stored as text in a `raw_json` column and CSV columns as text. Typing happens in silver.
- **No business filters:** the sandbox and invalid records enter bronze.
- **One documented exception:** `bronze.off_products` holds only the rows whose code exists in the ERP. The full file stays in the landing zone, so nothing is lost, and the run log records rows read and rows kept as filtered.
- **Idempotent writes:** each run replaces only its window's partition.
- **UTC** fixed on the Spark session and on the ERP connection.

### Control columns

| Column | Content |
| --- | --- |
| `_window_id` | Window the load belongs to |
| `_run_id` | Run identifier |
| `_loaded_at` | Load time, in UTC |
| `_source` | Source system and object |

### Tables

| Table | Source | Load type | Partition | Source key |
| --- | --- | --- | --- | --- |
| `bronze.erp_tenants` | ERP | Full snapshot per window | `_window_id` | `tenant_id` |
| `bronze.erp_locations` | ERP | Full snapshot per window | `_window_id` | `location_id` |
| `bronze.erp_products` | ERP | Full snapshot per window | `_window_id` | `product_id` |
| `bronze.erp_inventory_ledger` | ERP | Incremental by `ingested_at` | `_window_id` | `ledger_entry_id` |
| `bronze.partner_events` | Partner API | Incremental by cursor | `_window_id` | `sequence_cursor` |
| `bronze.off_products` | CSV dump | Single reference snapshot | `_snapshot_date` | `code` |
| `bronze.off_product_api` | Product API | Snapshot of the 18 codes per window | `_window_id` | `code` |
| `bronze.open_prices` | Open Prices | Six-month baseline plus increment by `created` | `_window_id` | `id` |

### Shape of each table

- **ERP:** source columns with their types; the two JSONB columns (`lowcode_attributes`, `lowcode_workflow_meta`) as text.
- **Partner events:** `raw_json` with the full event, plus `sequence_cursor`, `event_id` and `ingested_at` extracted for partitioning and deduplication.
- **Open Food Facts dump:** the eleven validated columns, as text, and only the rows whose code exists in the ERP. The filter is applied before writing, and the metrics record rows read and rows kept.
- **Public APIs:** `raw_json` per record, plus the key and the creation date.
- **Open Prices baseline:** prices from April 1 to September 30, 2026 go to a `baseline` partition, separate from W1, W2 and W3. The six months are a configuration parameter.

## Silver design

Silver holds the current state of each entity, typed and resolved by its own key; the history of snapshots and revisions stays in bronze.

### Keys and resolution

| Table | Key | How the current row is chosen |
| --- | --- | --- |
| `dim_tenant` | `tenant_id` | Latest snapshot |
| `dim_location` | `location_id` | Latest snapshot |
| `dim_product` | `product_id` | Latest snapshot |
| `fact_inventory_movement` | `ledger_entry_id` | One row per entry; entries that reuse a `reference_doc_id` are kept and flagged |
| `fact_pos_sale` | `event_id` | Highest `revision`; on a tie, highest `sequence_cursor` |
| `fact_shelf_audit` | `event_id` | Highest `revision`; on a tie, highest `sequence_cursor` |
| `ref_product_public` | `gtin14` | Highest `last_modified_t` between dump and API; on a tie, the API; a null attribute is filled from the other source |
| `ref_public_price` | `id` | Highest `updated`; rows with `duplicate_of` set are excluded |

### Revisions and re-runs

- The `MERGE` applies an incoming row only when its (`revision`, `sequence_cursor`) is higher than the stored one, so re-running an earlier window never reverts a later correction.
- A revision 2 with no revision 1 is valid and becomes the current row.
- A revision can change the store, the product and the dates of an event. In the data it never changes the tenant or the event type.
- Rules are checked on the current revision. If it fails a rule, the whole event goes to quarantine and the earlier revision is not restored.

### Status handling

| Source | Status | Silver | Gold |
| --- | --- | --- | --- |
| Ledger | `POSTED_00` | Kept | Counts |
| Ledger | `VOID_CANCELLED_99`, `PENDING_APPROVAL`, `DUPLICATE_REPLAY` | Kept with its status | Excluded |
| POS sale | `ACTIVE` | Kept | Counts |
| POS sale | `CANCELLED_VOID` | Kept with its status | Excluded |

### Column rules

- **Barcodes:** three columns. `gtin_raw` is the value as received. `gtin14` is built in steps: keep digits only; strip leading zeros to measure the significant length; if it is 14 or less, left-pad with zeros to exactly 14; otherwise leave it null. `gtin_status` is `VALID`, `INVALID_CHECK_DIGIT`, `TOO_LONG` or `INVALID` (empty or non-numeric, which goes to quarantine).
- **Product link:** events join products by `product_id`. The product barcode comes from the ERP master; the event's own barcode is kept as `event_gtin14` for audit.
- **Supplier:** `dim_product.primary_supplier_location_id` references a plant in `dim_location`.
- **Money:** integer cents with a currency column. Revenue is always grouped by currency and never added across currencies; there is no currency conversion.
- **Discount:** `discount_rate` is stored as a numeric fraction (`"15%"` becomes 0.15). `promo_mechanics` becomes `promo_campaign_id`, `promo_mechanic_type` and `promo_co_funded_ratio`.
- **Timestamps:** both source formats are converted to the same instant in UTC; a value that cannot be parsed sends the record to quarantine.
- **Low-code attributes:** the JSON is kept as text, and typed columns are extracted by the shape of each value, never by tenant ID: `pack_size_units`, `pack_size_raw`, `uom`, `shelf_life_days`, `promo_eligible`, `temp_zone` and `custom_tags`. Unknown keys stay in the JSON.
- **Public prices:** a reference indicator, never used to fill a sale. A sale with no comparable price keeps the reference null and counts as not covered.
- **Personal data:** no `owner*` field from Open Prices (`owner`, `owner_comment`, and the same fields nested in `proof`) appears in silver, gold, logs, the catalog or the committed fallback fixtures.

### Expected partner event counts after the three windows

| Step | Events |
| --- | --- |
| Real events | 30,000 (24,000 sales, 6,000 audits) |
| Superseded by a later revision | 250 |
| Current | 29,750 |
| Current with a negative unit price, to quarantine | 308 |
| `fact_pos_sale` | 23,554 (22,796 active, 758 cancelled) |
| `fact_shelf_audit` | 5,888 |

## Decisions made

| Decision | Reason |
| --- | --- |
| Python, PySpark and Delta Lake for processing and storage | Mandatory per the README |
| The ERP and the partner API run as containers in Docker Compose | Defined by the team; lets us explore the sources before building the pipeline |
| The ERP uses the official PostgreSQL image with `init_erp.sql` mounted as the initialization script | No need to modify `legacy_sources/` |
| The API uses its own Dockerfile, outside `legacy_sources/` | No need to modify `legacy_sources/` |
| Shelf audits have minimal scope in the first iteration: they are separated from sales in silver and do not count toward revenue or units sold; no gold table of their own | The README does not ask for audit analysis |
| Open Food Facts is read from the compressed CSV, with an explicit text schema, only the needed columns and a filter on the ERP barcodes | 1.54 GB download (November 14, 2026) versus 8.05 GB for the Parquet; flat schema; only 18 products are needed |
| Open Food Facts is loaded once as reference data, in a step separate from the windows, and stored in Delta with the snapshot date | It has no windows or tenant; re-running a window must not download again or change the attributes |
| Open Prices is loaded with a baseline of the six months before the first window (April 1 to September 30, 2026) for the 18 codes, plus an increment per window by `created`; gold uses the last known public price as of the sale date, in the same currency, and reports coverage | There is only 1 price on the project dates for 2 products; loading only the window would leave the table almost empty |
| Public APIs are extracted once per window (about 20 HTTP calls), at one call per second, with up to three retries; re-running a window makes no calls and reuses the stored response | About one price per week arrives per product; some 20 calls per run stay far below the limit of 100 per minute |
| All amounts are stored in silver and gold as integer cents, with a `_cents` suffix and a currency column; Open Prices prices, which arrive with decimals, are converted to cents | The ERP and the partner API already use cents; a single unit avoids comparing 445 against 4.45 and the rounding errors of decimals |
| Version 1 goes up to silver: environment, bronze, silver, quarantine, run log and re-run test | Deliver a verifiable base and iterate on top: gold in v2, catalog and graph in v3, and an optional Prefect layer in v4. The evaluable deliverable is v3 |
| Barcodes are normalized to GTIN-14 when the code has 14 significant digits or fewer: digits only, left-padded with zeros to exactly 14. A code with more than 14 significant digits is not truncated: its normalized value stays empty, the original is kept and it is reported as a longer code | A single format makes it possible to join ERP, partner API, Open Food Facts and Open Prices; length is measured after stripping leading zeros, because the legacy sources carry 15- and 16-character codes that only have extra zeros |
| When the fact's tenant and the dimension's tenant differ, the fact's tenant wins and the dimension owner is stored as well | The mismatch occurs in 100% of real rows; it is reported in reconciliation |
| The sandbox enters bronze and is excluded from silver, identified by `workspace_env` and not by its ID | It is load-test filler: 60% of the ledger and 50% of the events, with no business value |
| Events whose current revision has a negative unit price go to quarantine with a reason, and an earlier revision is not restored | There are 310 such records, 308 after resolving revisions; nothing indicates they are returns |
| When the event's barcode differs from the master's, the ERP master wins and the event's barcode is stored | They match in only 1,665 of 30,000 events; the master is the stable reference |
| Movements that reuse a `reference_doc_id` are both kept and flagged | They have different `ledger_entry_id` values and there is no way to tell a correction from a duplicate |
| Stores in `RENOVATION_HOLD` are kept in silver with their status; whether to exclude them is decided in gold | Silver does not discard valid data |
| The partner API is extracted by cursor, with a `page_size` of 500, and the window is assigned by `ingested_at` | The API's `window` parameter only accepts W1 to W3; the cursor supports future windows |
| The brand owner is `brand_owner` when present; otherwise, the first value of `brands` | `brand_owner` can be empty: the API did not return it for Nutella, whose first `brands` value is Ferrero |
| The GTIN check digit is validated and reported, without rejecting the record | It detects typing errors without losing sales or movements over a doubtful code |
| Bronze for the Open Food Facts dump holds only the codes present in the ERP, as a documented exception to the no-filter rule; the full file stays in the landing zone | Writing several million unused rows costs time and disk; nothing is lost because the original file is kept and the run log records rows read and kept |
| Every table declares a scope: tenant, global public or platform. Only tenant-scoped tables are required to carry a tenant column | Public reference data has no tenant, so a tenant column on every table cannot hold |
| Cancelled sales and ledger entries that are not posted are kept in silver with their status and excluded in gold | Silver does not discard valid data; only active sales and posted movements count toward metrics |
| Revenue is grouped by currency and never added across currencies; there is no currency conversion | Sales come in MXN, EUR and USD and no source provides exchange rates |
| The public price is a reference indicator and is never used to fill a sale | Coverage is sparse, and a price from another country is not always comparable |
| The graph is stored as node and edge tables in Delta, and traversals run with GraphFrames | The goal requires the graph to be queried as Delta tables, and GraphFrames needs no extra service |
| The catalog is a set of Delta tables generated from a declarative registry | It keeps a single metadata store that is reproducible from a clean clone |
| The sources stay up as services, and the pipeline container runs on demand, one command per window | It separates starting the infrastructure from running a window |
| The CLI, run through the Makefile, is the unit of execution; Prefect is an optional layer over the same commands (v4) | Every run works without extra services; Prefect adds retries and a run view without changing the pipeline |
| Observability is Delta tables: the run log, run metrics, `gold.pipeline_health` and `gold.reconciliation` | They are queried like every other table and need no extra service |

The definitions that v1 left to later versions are settled in the [v2-v4 PRD](PRD%20v2-v4%20Arkham%20Data%20Platform.md): the gold formulas and expected values, the reconciliation, the public price country rule, the graph model and substitution semantics, the catalog contract, and their acceptance criteria.

## Assumptions

These points are not stated by any source; the design depends on them, and each one should be revisited if the data shows otherwise.

- Revenue of a sale line is `units_sold` times `unit_price_cents` times one minus `discount_rate`.
- In a shelf audit, `units_sold` means units observed and `unit_price_cents` means the shelf price.
- A negative unit price is a data error and not a return.
- The partner API delivers events in cursor order that is consistent with their ingestion window.
- A price of type `PRODUCT` in Open Prices is the price of one unit of the product as sold.

## Acceptance criteria

This document holds the v1 criteria. The v2, v3 and v4 criteria, all verified, are in the [v2-v4 PRD](PRD%20v2-v4%20Arkham%20Data%20Platform.md).

The criteria are checked in two ways:

- **Invariants (`make verify`):** properties that hold on any data and any window calendar, so they stay the permanent criteria when new windows or new source data arrive. Examples: no revision is reverted, no sandbox row reaches a fact, every event is held in exactly one place.
- **Baseline (`make verify-v1`):** the exact numbers below, computed from the deterministic generators of the provided legacy sources before the pipeline existed. They hold only for that dataset, W1, W2 and W3. On other data or another calendar the command reports "Not applicable" instead of failing.

### v1

Verified on November 15, 2026 from a clean clone in an isolated Docker Compose project: `make up`, `make check-sources`, `make reference SKIP_DOWNLOAD=1`, `make run-pending`, `make verify-rerun` and `make verify-v1` (24 of 24 checks), plus `make test` (81 tests, no network). The full Open Food Facts dump was verified separately in the main environment. `make verify-v1` re-checks most of the numbers below on any loaded environment.

- [x] `make up` brings up the ERP and the API in a healthy state from a clean clone, and `make run WINDOW=<id>` runs one window.
  Evidence: in the clean clone both services reached healthy on `make up`, and `make run-pending` ran W1, W2 and W3 through `python -m arkham run --window <id>`, the command behind `make run`. In the main environment `make run WINDOW=2026-10-W3` finished with `SUCCESS`.
- [x] The loaded ERP contains 5 tenants, 128 locations, 200 products and 250,000 movements.
  Evidence: `make check-sources` (as the read-only role) and `make verify-v1` on the bronze snapshot.
- [x] W1, W2 and W3 run in order without errors, and the peak memory of the pipeline container stays below 2 GB, including the Open Food Facts dump load.
  Evidence: all three windows finished with `SUCCESS` from a clean clone. Peak memory of the container's own processes, without reclaimable file cache: 1.4 GB for `make run-pending`, 1.4 GB for `make verify-rerun` and `make run`, and 1.1 GB for the reference load with the full 1.5 GB dump (4,912,665 products read). No OOM kills.
- [x] After re-running W1 once W3 is done, the counts and hashes of all silver tables are identical to the previous ones.
  Evidence: `make verify-rerun WINDOW=2026-10-W1` compares the row count and content hash of every bronze partition, every silver table and the quarantine: no differences, in the main environment and in the clean clone (also for W2). Re-running W1, W2 and W3 again in one session and `make rebuild-silver` left everything identical too.
- [x] No `event_id` with revision 2 goes back to revision 1 after re-running an earlier window.
  Evidence: `make verify` finds 0 events whose current version is not their newest delivered revision after the re-runs, and every event held in exactly one place; covered by `test_rerun_and_rebuild_leave_silver_identical`.
- [x] ERP product barcodes reduce to 18 GTIN-14 values, and no code is reported as longer than 14 significant digits.
  Evidence: `make verify-v1`: 18 distinct `gtin14` in `dim_product`, 0 with status `TOO_LONG`. All 18 are present in the Open Food Facts dump.
- [x] The 3,000 dates in local format are converted to the same instant in UTC, with no nulls; event 5 becomes `2026-10-01T00:01:20Z`.
  Evidence: `make verify-v1`: 3,000 real deliveries in the local format, 0 events quarantined for an unparseable date, event 5 at `2026-10-01T00:01:20Z`.
- [x] `fact_pos_sale` holds 23,554 rows (22,796 active, 758 cancelled) and `fact_shelf_audit` holds 5,888.
  Evidence: `make verify-v1`, from a clean clone and in the main environment.
- [x] Quarantine holds the 308 current events with a negative unit price, and the run log counts 250 superseded revisions.
  Evidence: `make verify-v1`: 308 quarantined for a negative unit price; 250 superseded (real deliveries minus current events). The run log of `make rebuild-silver`, which replays W1 to W3 in order, records 250 superseded for `fact_pos_sale`.
- [x] `fact_inventory_movement` holds 100,000 rows, 88,408 of them posted, with the 1,600 movements that share a `reference_doc_id` with another movement flagged.
  Evidence: `make verify-v1`: 100,000 rows, 88,408 posted, and 1,600 flagged as `is_reference_doc_reused`. Under the decision "both are kept and flagged", every movement of a shared document is flagged. The value first listed here, 1,080, counted the real movements whose document number differs from their own ID; 280 of those share their document with no other movement, so they are not reuses.
- [x] `discount_rate` is numeric in every sale row, with no nulls.
  Evidence: `make verify`: 0 sales with a null `discount_rate` (decimal fraction; `"15%"` becomes 0.15).
- [x] No silver fact table contains rows recorded by the sandbox tenant; the sandbox stays only in the dimensions, flagged.
  Evidence: `make verify`: 0 sandbox rows in the three silver fact tables; `make verify-v1`: 1 tenant with `is_sandbox` in `dim_tenant`. Also `test_tenant_scoped_silver_tables_always_carry_a_tenant`.
- [x] A window with no data finishes with status `SUCCESS` and zero rows in the run log.
  Evidence: `test_a_window_without_data_succeeds_with_zero_rows` runs a window with no source data through bronze and silver: status `SUCCESS`, 0 rows written for the ledger, partner events and prices.
- [x] Interrupting a run and running the window again leaves the same result as an uninterrupted run.
  Evidence: in the main environment a W2 run was killed with `SIGKILL` (before silver existed): W2 stayed pending, W3 was blocked, and `make run-pending` re-ran W2 with every bronze partition identical to before. In the clean clone a W2 that failed on a cut Open Prices response was completed by a later `make run-pending`. `test_interrupted_run_leaves_the_window_incomplete_until_rerun` compares an interrupted-then-rerun window with an uninterrupted one, bronze and silver included.

## Version plan

The project is delivered in versions; each one leaves something verifiable and the next is built on top. The evaluable deliverable is v3, and v4 is optional. All four are built and verified.

| Version | Scope | Verifiable result |
| --- | --- | --- |
| v1 | Environment, bronze and silver | The three windows run and re-running W1 leaves silver identical |
| v2 | Gold: inventory, sales and health | The three business domains answer queries |
| v3 | Catalog and graph | Lineage, freshness and traversal queries |
| v4 (optional) | Prefect orchestration layer over the CLI | The same windows run through the flow with identical tables; not part of the commitment |

### v1 scope

- **Environment:** Docker Compose with ERP, API and pipeline, memory limits and healthchecks; Makefile and CLI parameterized by window, with a configurable window calendar.
- **Bronze, 8 tables:** the four ERP tables, partner events, the Open Food Facts dump, Open Prices prices and product records.
- **Support:** quarantine with a reason, a run log with status and row counts by outcome, and a command to rebuild silver from bronze.
- **Tests:** normalizers for barcodes, dates and discounts, the re-run test, the interruption test and the isolation test.
- **Out of v1:** gold, catalog, graph and Prefect, delivered in v2, v3 and v4.

| Silver table | Source | Main transformation |
| --- | --- | --- |
| `dim_tenant` | ERP | Sandbox flag based on `workspace_env` |
| `dim_location` | ERP | Normalized types, validated parent |
| `dim_product` | ERP | Barcode in GTIN-14, low-code attributes typed by value shape |
| `fact_inventory_movement` | ERP | Deduplicated, normalized status, dates in UTC |
| `fact_pos_sale` | Partner API | Latest revision, numeric discount, dates in UTC |
| `fact_shelf_audit` | Partner API | Separated from sales |
| `ref_product_public` | Dump and product API | 18 products, the most recent edit wins |
| `ref_public_price` | Open Prices | Cents, no duplicates, no personal data |
