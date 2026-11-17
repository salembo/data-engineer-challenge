# PRD v2-v4: Arkham Data Platform

Nov 15, 2026 · @Ana · updated Nov 16, 2026 to describe only what is built

## Scope

This document specifies v2 (gold), v3 (catalog and graph) and the optional v4 (a Prefect orchestration layer) of the Arkham data platform; v3 is the evaluable deliverable. It builds on the v1 PRD ([PRD: Arkham Data Platform](prd.md)), which stays as the record of sources, bronze, silver and the decisions made for v1.

Everything in the v1 PRD still applies: memory limits, UTC, windows as a parameter, tenancy and scope, integer cents, and identical tables after any re-run. This document only adds to it.

| Version | Delivers | Requirements from the v1 PRD |
| --- | --- | --- |
| v2 | Gold tables for inventory, POS sales and pipeline health | FR-6, FR-7, FR-8, and FR-3 for gold |
| v3 | Data catalog and supply-network graph, as Delta tables | FR-9, FR-10 |
| v4 (optional) | Prefect orchestration layer over the CLI | None; not part of the commitment |

v2, v3 and v4 are built and verified on the real data; each acceptance criterion below records its evidence. The v2 rules are confirmed, including the three decisions that were open (see Decisions). One v3 rule is implemented as proposed but still awaits confirmation; it is listed under Open decisions.

## v2: gold tables

Gold has six tables across the three business domains. The four business tables are built only from silver and aggregated by business date, so late records land on the day they happened. The two health tables are computed from the state of the tables: reconciliation reads bronze, silver and the quarantine, and pipeline health takes its row counts from reconciliation. From the run log they take only the run attributes (status, duration, fallback use) and the source counts.

| Table | Domain | Grain | Main columns |
| --- | --- | --- | --- |
| `gold.inventory_position` | Inventory | `tenant_id`, `location_id`, `product_id` | `category_code`, `net_position_units`, `inbound_units`, `outbound_units`, `last_movement_at`, `operating_status` |
| `gold.inventory_daily` | Inventory | `tenant_id`, `location_id`, `category_code`, `movement_date` | `inbound_units`, `outbound_units`, `net_units`, `movement_count`, `period_id`, `velocity_units_per_day` |
| `gold.pos_sales_daily` | Sales | `tenant_id`, `location_id`, `product_id`, `currency`, `sale_date` | `units_sold`, `gross_revenue_cents`, `discount_cents`, `net_revenue_cents`, `sale_lines`, promo columns, public product attributes, public price reference |
| `gold.pos_promo_performance` | Sales | `tenant_id`, `location_id`, `currency`, `promo_campaign_id`, `promo_mechanic_type` | `units_sold`, `net_revenue_cents`, `discount_cents`, `avg_discount_rate`, `co_funded_discount_cents` |
| `gold.pipeline_health` | Health | `window_id`, `tenant_id` or `GLOBAL`, `table_name` | Row counts by outcome (from `gold.reconciliation`), quarantine reasons, public price coverage, and the run status, duration and fallback use (from the run log) |
| `gold.reconciliation` | Health | `window_id`, `tenant_id` or `GLOBAL`, `source_object` | `source_rows`, `bronze_rows`, `silver_rows`, `quarantined`, `filtered`, `superseded`, `flagged`, `unexplained_rows` |

- **Public product attributes** in `gold.pos_sales_daily` come from `ref_product_public`, joined by `gtin14`: `brand_owner`, `main_category`, `nutriscore_grade` and `nova_group`. They are null when the product has no public record.
- **No null keys:** sale lines with no promotion use the value `NONE` for `promo_campaign_id` and `promo_mechanic_type` in `gold.pos_promo_performance`, so the grain never contains a null.
- **Sandbox:** the four business tables hold no sandbox rows. The two health tables do report the sandbox, because its rows are the ones filtered between bronze and silver.
- **Scope:** the six tables are tenant-scoped, except the `GLOBAL` rows of the two health tables, which describe the public sources.
- **Shelf audits** have no gold table, as decided for v1.

## v2: metric rules

### Inventory

- **Rows that count:** ledger entries with status `POSTED_00`. Entries flagged for a reused `reference_doc_id` count, and stay flagged.
- **Sign:** `quantity_delta` is used as it arrives. Positive is inbound and negative is outbound; the movement type is not used to set the sign.
- **Net position:** the sum of `quantity_delta` per tenant, location and product, over all processed windows. No source provides an opening balance, so the position is the net change since October 1, 2026 and can be negative.
- **Velocity:** `velocity_units_per_day` is computed per period, where a period is the date range of a window in the calendar: the outbound units of a tenant, location and category whose event date falls in the period, divided by the number of days in that range, read from the calendar and never fixed in code. A movement counts in the period in which it happened, not in the window in which it arrived, and every daily row carries the `period_id` and the value of its period.
- **Stores in `RENOVATION_HOLD`:** included, with `operating_status` as a column so that consumers can filter them.

A finding that shapes these metrics: in the ledger each real tenant records a single movement type, and each store receives movements from a single tenant.

| Tenant | Movement type recorded | Direction | Posted units |
| --- | --- | --- | --- |
| `TENANT_CPG_001` | `REPLENISHMENT_OUT` | Outbound | 265,254 |
| `TENANT_CPG_002` | `STORE_ALLOCATION` | Inbound | 1,259,667 |
| `TENANT_RTL_003` | `SHRINKAGE` | Outbound | 309,438 |
| `TENANT_RTL_004` | `ADJUSTMENT` | Outbound | 331,480 |

As a result, 180 of the 240 position rows are negative, only 48 of the 120 stores have real movements, and `RECEIPT` appears only in the sandbox. Decided: negative positions are reported as they are, not corrected. They come from outbound events such as replenishment sent out of the store, shrinkage and adjustments, recorded without the matching inbound stock.

### Sales

- **Rows that count:** rows of `fact_pos_sale` with status `ACTIVE`.
- **Gross revenue:** `units_sold` times `unit_price_cents`.
- **Net revenue:** gross revenue times one minus `discount_rate`, rounded half up to the cent on each sale line and then added.
- **Discount:** gross revenue minus net revenue.
- **Co-funded discount:** discount times `promo_co_funded_ratio`, rounded half up to the cent on each sale line and then added; 0 for lines with no promotion.
- **Average discount rate:** `avg_discount_rate` is weighted: total discount divided by total gross revenue.
- **Currency:** every metric is grouped by currency and never added across currencies.
- **Date:** `sale_date` is the UTC date of the event timestamp, not of its ingestion.

### Public price reference

`gold.pos_sales_daily` carries `public_price_cents`, `public_price_date` and `price_match_level`. The price is the last one in `ref_public_price` for the same `gtin14` and currency, dated on or before the sale date and no older than a configurable age, six months by default. When several prices of the same match level share the latest date, the one with the latest `updated_at` wins, then the highest `id`, so the same silver always gives the same reference: the loaded data has 5 such days with different prices for the same product, currency and country.

| `price_match_level` | Meaning |
| --- | --- |
| `COUNTRY` | A price exists in the same currency and in the store's country |
| `CURRENCY` | A price exists in the same currency, from another country |
| `NONE` | No comparable price; the reference columns stay null |

A `COUNTRY` match is preferred over a more recent `CURRENCY` match. Coverage by tenant and window goes to `gold.pipeline_health`.

On the data loaded on November 15, 2026 no sale reaches `COUNTRY`: the country of a store does not match the currency of its sales (`TENANT_CPG_002` sells in MXN at stores in the Netherlands, `TENANT_RTL_003` in EUR at stores in the United States). 2,849 of the 22,796 active sale lines, all of them `TENANT_RTL_003` in EUR, match at `CURRENCY`, and the rest are `NONE`. Requiring the same country would leave coverage at zero.

### Health and reconciliation

- **Computed from state, not from run order:** `gold.reconciliation` is derived from bronze, the rows in silver and the quarantine table. It does not depend on which run counted a row, so a re-run gives the same figures. It is the only gold table that reads bronze, and from bronze it reads only the keys, `_window_id` and the tenant of each row; for partner events the tenant is read from `raw_json`, where bronze keeps it.
- **Health counts come from reconciliation:** the row counts by outcome in `gold.pipeline_health` are taken from `gold.reconciliation`, never from the counts in the run log. The run log counts depend on run order: the first run of W1 records 9,897 sales written and none superseded, and the first run of W2 records the 250 corrected revisions as superseded; re-running W1 after W3 records 9,699 written and 250 superseded in W1.
- **Attribution:** every bronze row is attributed to the window in which it arrived. A revision that is later replaced counts as superseded in its own arrival window: the 250 superseded events all arrived in W1 and were corrected in W2, so they count in W1.
- **`unexplained_rows`** is what the identity of the source object leaves over, and must be 0.
- **Source count:** `source_rows` is taken from the source at extraction time and stored in the run log, so a re-run does not need the source. It is recorded per tenant for the ERP and the partner events, where the tenant is a column or a field of each record, and as one `GLOBAL` count for the public sources. For this, `platform.run_metrics` gains a nullable `tenant_id` column; rows written before v2 keep it null.
- **Also reported:** rows where the fact's tenant differs from the owner of the store or the product, barcodes by `gtin_status`, reused document references, late records, and sales with and without a public price.
- **Volatile columns:** run status, duration and load times in `gold.pipeline_health` change with every run and are excluded when comparing tables before and after a re-run.

Each source object has its own identity, by load pattern:

| Source object | Load pattern | Identity per window |
| --- | --- | --- |
| ERP ledger | Incremental | `bronze_rows` = `silver_rows` + `quarantined` + `filtered` (sandbox) |
| Partner events | Incremental with revisions | `bronze_rows` = `silver_rows` + `quarantined` + `filtered` (sandbox) + `superseded` |
| ERP dimensions | Full snapshot per window | `source_rows` = `bronze_rows`. The latest snapshot is the one in silver; rows of earlier snapshots count as `superseded` |
| Open Food Facts dump | Reference, filtered before bronze | `source_rows` (about 4.9 million) = `bronze_rows` (18) + `filtered`; reported once, as `GLOBAL` |
| Product API | Snapshot of the ERP codes per window | `bronze_rows` (18 per window, 54 in total) = current rows + `superseded` (rows of earlier snapshots, and rows whose version lost to the dump) |
| Open Prices | Baseline plus incremental | `bronze_rows` = `silver_rows` + `superseded` (older version of the same `id`) + `filtered` (rows with `duplicate_of` set) + `quarantined`; the baseline is its own row. Silver keeps every currency, and gold matches prices by currency. On the loaded data: 137 = 103 + 28 + 6 + 0 |

The dump and the product API feed one silver table: `ref_product_public` has one row per `gtin14` present in either source, and a row counts as current for the source whose version won; the other source's row for the same `gtin14` counts as superseded. On the loaded data the product API won for all 18 codes.

### Writes and re-runs

- **Full recompute:** every run recomputes the four business tables in full from silver. There is no tracking of affected dates or periods, so a revision that moves a sale to another date (242 of the 250 superseded events change date), a late record, or a period velocity are all handled by the same rule.
- **Write pattern:** a `MERGE` on the table's grain that inserts new rows, updates only rows whose values changed, and deletes rows that no longer exist in the recomputed result. A re-run with no changes rewrites nothing, and the Delta history shows it. The one exception is `gold.pipeline_health`, which rewrites the rows of the window that ran because its volatile columns change.
- **Determinism:** identical silver gives identical gold. Re-running W1 after W3 leaves every gold table unchanged, apart from the volatile columns of `gold.pipeline_health`.
- The whole-table overwrite ban in `CLAUDE.md` still holds, and its gold section summarises these rules.

## v2: acceptance criteria

The criteria are checked in two ways:

- **Invariants (`make verify`):** properties that hold on any data and any window calendar, so they are the permanent criteria when new windows or new source data arrive. Examples: `unexplained_rows` is 0, health equals reconciliation, gold matches silver, no sandbox row, no amount added across currencies.
- **Baseline (`make verify-v2`):** the exact values below. They were computed from the source generation logic with the v1 silver rules and the metric rules above, and hold only for the provided dataset after W1, W2 and W3. On other data or another calendar the command reports "Not applicable" instead of failing.

Verified on November 16, 2026 on the real data and from a clean clone, and the invariants again on November 17. `make verify` passes 36 of 36 invariants, and `make verify-v2` passed every baseline number. `make verify-rerun WINDOW=2026-10-W1` and `make prefect-verify` for W1, W2 and W3 left every table identical. The pipeline tests pass, with no network.

- [x] After re-running W1 once W3 is done, the counts and hashes of all gold tables are identical to the previous ones, leaving out the run status, duration and load times of `gold.pipeline_health`.
  Evidence: `make verify-rerun WINDOW=2026-10-W1` compares the row count and content hash of every table before and after; fingerprints leave out the volatile columns.
- [x] A re-run with no changes adds no rewritten rows to the Delta history of any gold table except `gold.pipeline_health`, which rewrites only the rows of the window that ran.
  Evidence: `make verify-rerun` also fails if any derived table other than `gold.pipeline_health` and `catalog.freshness` receives a commit.
- [x] The row counts by outcome in `gold.pipeline_health` equal those of `gold.reconciliation`, and do not change when W1 is re-run after W3.
  Evidence: `make verify`: 0 health counts that differ from reconciliation; both tables are unchanged by the re-run.
- [x] `gold.inventory_position` has 240 rows across 48 stores, 180 of them with a negative net position.
  Evidence: `make verify-v2`.
- [x] Inbound units add up to 1,259,667, outbound units to 906,172, and the net position across all rows to 353,495.
  Evidence: `make verify-v2`. `make verify` checks that `gold.inventory_daily` adds up to the same net units per store and category, and that every daily row has a period and a velocity.
- [x] `gold.pos_sales_daily` adds up to the totals in the table below, per currency, and counts 22,796 sale lines after W1, W2 and W3 are processed, and again after re-running W2 and after re-running W3, with no superseded sale left on its old date.
  Evidence: `make verify-v2`: the three tenant totals match. `make verify`: 0 rows differ from silver's active lines. Re-running W2 and W3 through `make prefect-verify` left the table identical.
- [x] `gold.pos_promo_performance` has no null in its grain, counts 8,548, 672 and 1,006 sale lines under `NONE` for the three tenants in the table below, and its row count does not grow on a re-run.
  Evidence: `make verify` (no null in the grain, and promotion totals equal sales totals per store and currency), `make verify-v2` (the `NONE` counts) and `make verify-rerun`.
- [x] No gold metric adds amounts in different currencies.
  Evidence: currency is part of the grain of both sales tables; `make verify` finds 0 sales rows with an unknown currency, and `make verify-v2` shows each tenant's totals in its single currency.
- [x] The inventory, sales and promotion tables contain no rows recorded by the sandbox tenant.
  Evidence: `make verify`: 0 sandbox rows.
- [x] Cancelled sales, shelf audits and ledger entries that are not posted do not contribute to any metric.
  Evidence: `make verify`: movements counted equal the posted movements per tenant, and every sales row equals silver's active lines. `make verify-v2`: 22,796 sale lines.
- [x] `unexplained_rows` is 0 in every row of `gold.reconciliation`, for every source object.
  Evidence: `make verify`.
- [x] For partner events, the three windows reconcile as 60,000 bronze rows: 29,442 in silver, 308 quarantined, 250 superseded (all attributed to W1) and 30,000 filtered as sandbox.
  Evidence: `make verify-v2`.
- [x] For the ledger, the three windows reconcile as 250,000 bronze rows: 100,000 in silver and 150,000 filtered as sandbox.
  Evidence: `make verify-v2`.
- [x] Every sale row has a `price_match_level`, and `gold.pipeline_health` reports public price coverage by tenant and window.
  Evidence: `make verify`: 0 rows without a match level, and 0 tenant-windows with sales but no coverage figure. `make verify-v2` reports the levels without checking them: on the loaded data, 2,849 lines match at `CURRENCY` and 19,947 are `NONE`.
- [x] W1, W2 and W3 still run within the 2 GB memory limit with gold included.
  Evidence: the peak of the pipeline container with every layer was 1.0 GB, for `make run-pending` and for `make verify-rerun`.

| Tenant | Currency | Sale lines | Units sold | Gross revenue (cents) | Net revenue (cents) |
| --- | --- | --- | --- | --- | --- |
| `TENANT_CPG_002` | MXN | 8,548 | 51,288 | 51,413,752 | 46,110,496 |
| `TENANT_RTL_003` | EUR | 5,700 | 39,900 | 39,957,344 | 35,176,072 |
| `TENANT_RTL_004` | USD | 8,548 | 68,404 | 68,358,608 | 60,044,458 |

`TENANT_CPG_001` has no sales: all its events are shelf audits. Public price coverage has no fixed expected value, because it depends on the live Open Prices data at extraction time.

## v3: data catalog

The catalog is four Delta tables generated from one declarative registry file kept in the repository, and refreshed at the end of every run.

| Table | Grain | Content |
| --- | --- | --- |
| `catalog.datasets` | One row per table | Identity, description, layer, scope, tenant column, keys, partitioning, sensitivity and write pattern |
| `catalog.columns` | One row per table and column | Name, type, nullability, description, and flags for key, tenant and personal data |
| `catalog.lineage` | One row per upstream and downstream pair | Upstream table or source object, downstream table, and the transformation that links them |
| `catalog.freshness` | One row per table and window | Run status, row count, load time, source snapshot date, and whether a fallback was used |

### Metadata contract for `catalog.datasets`

| Field | Content |
| --- | --- |
| `table_name` | Unique identifier, as `layer.table` |
| `description` | One sentence in business terms |
| `layer` | `bronze`, `silver`, `gold`, `graph`, `catalog` or `platform` (run log and quarantine) |
| `scope` | `TENANT`, `GLOBAL` or `PLATFORM`; a separate axis from the layer |
| `tenant_column` | The column that defines isolation; null for global tables |
| `primary_key` | The columns of the table's grain |
| `partition_columns` | Partitioning, if any |
| `load_pattern` | Snapshot, incremental by window, reference or derived |
| `contains_personal_data` | True only for `bronze.open_prices` |
| `source_objects` | The source objects the table ultimately comes from |

### Rules

- **Registry and reality must agree:** the registry declares descriptions, scope and lineage; column names and types are read from the Delta tables themselves.
- **When the check applies:** a Delta table with no registry entry fails any run. A registry entry with no table fails at the end of a window run, except the reference-load tables before the reference data is loaded; the reference load, which runs before gold and the graph exist, skips that half of the check. The run log tables are listed from their declared schema, so the catalog is the same from the first run. The scratch folder of the lakehouse is ignored.
- **Lineage** goes from source object to bronze, silver, gold and graph, at table level. Column-level lineage is out of scope.
- **Freshness for tenant-scoped tables** is per window; **for global tables** it is the snapshot date of the public source and the window in which it was last refreshed.
- **Isolation metadata:** a consumer filters a tenant-scoped table by its `tenant_column`. The catalog tables themselves are platform-scoped and hold no tenant data, only table metadata.
- **Personal data:** `bronze.open_prices` has no `owner` column; the usernames are inside `raw_json`. Since `catalog.columns` reads the real schema, it flags `raw_json` as personal data and its description names the `owner` fields it contains. No value of those fields appears in the catalog.
- **Write pattern:** the catalog tables are written with a `MERGE` on their grain that also deletes rows that no longer apply, so a run with no changes rewrites nothing except load times in `catalog.freshness`.
- **Queries:** the repository includes example queries for the four common questions: which tables exist, what a table's schema is, where a table comes from, and how fresh it is.

## v3: supply network and product graph

The graph is two base tables in Delta, built from the silver dimensions, plus four result tables that answer the three traversal questions in the README. Traversals run with GraphFrames inside the pipeline container; no graph database is added.

### Nodes

`graph.nodes` has one row per node: `node_id`, `node_type`, `node_subtype`, `tenant_id` (the tenant that owns the node) and a status.

| Node type | Subtypes | Count | Source |
| --- | --- | --- | --- |
| `LOCATION` | Plant, distribution center, store | 127 (3, 4 and 120) | `dim_location`, without the sandbox buffer |
| `PRODUCT` | Category | 200 | `dim_product` |

Tenants are not nodes: ownership is the `tenant_id` column, as in every tenant-scoped table. Suppliers are not a separate node type either, because a product's supplier is always a plant.

### Edges

`graph.edges` has one row per edge: `src_id`, `dst_id`, `edge_type`, `tenant_id` (the owner of the source node), `dst_tenant_id` and `is_cross_tenant`. Every edge points from the dependent node to the node it depends on.

| Edge type | From | To | Source column | Count | Cross-tenant |
| --- | --- | --- | --- | --- | --- |
| `SUPPLIED_BY` | Location | Parent location | `parent_supply_node_id` | 124 | 19 |
| `SOURCED_FROM` | Product | Plant | `primary_supplier_id` | 200 | 150 |
| `SUBSTITUTED_BY` | Product | Substitute product | `substitute_product_id` | 199 | 199 |

An edge whose target does not exist in the nodes is not created; it is counted in the run log as a dangling reference. The current data has none.

### Result tables

| Table | Answers | Grain | Rows expected |
| --- | --- | --- | --- |
| `graph.upstream_dependencies` | Which nodes does a store depend on, and at how many hops? | Store and upstream node | 264 |
| `graph.bottlenecks` | Which upstream nodes concentrate the network? | Upstream node | 7 |
| `graph.cross_tenant_dependencies` | Where does one tenant depend on another? | Cross-tenant edge | 368 |
| `graph.substitution_paths` | What can replace a product, and through which chain? | Product and reachable substitute | 2,080 |

### Bottleneck definition

Every store has exactly one parent, so every upstream node is a single point of failure for its stores. A bottleneck is therefore ranked by concentration, not by the lack of an alternative route.

- `dependent_stores`: stores that depend on the node, directly or through other nodes.
- `store_share`: `dependent_stores` divided by all stores.
- `dependent_tenants`: distinct tenants that own those stores.
- `sourced_products`: products whose supplier is the node, for plants.
- `is_bottleneck`: true when `store_share` is at or above a configurable threshold, 50% by default.

| Node | Dependent stores | Store share | Dependent tenants |
| --- | --- | --- | --- |
| `PLANT_NA_01` | 72 | 60% | 4 |
| `DC_CENTRAL_HUB_01` | 72 (48 direct, 24 through `DC_WEST_HUB_04`) | 60% | 4 |
| `DC_WEST_HUB_04` | 24 | 20% | 1 |
| `DC_SOUTH_HUB_02` and `PLANT_LA_01` | 24 each | 20% | 1 |
| `DC_EU_HUB_03` and `PLANT_EU_01` | 24 each | 20% | 1 |

### Substitution semantics

- **Directed:** `SUBSTITUTED_BY` goes from a product to its substitute. The reverse is not implied.
- **One substitute per product** in the ERP, so a product has a single chain, not a tree.
- **Paths:** `graph.substitution_paths` holds every substitute reachable from a product, with `hops`, the ordered `path`, `crosses_tenant`, `crosses_category` and whether the substitute is discontinued.
- **Recommended substitute:** the nearest substitute in the chain that is not discontinued. More hops means a weaker match; the table does not claim commercial equivalence.
- **Depth and cycles:** decided: all reachable substitutes are stored, and consumers filter by hop count. Traversal stops at a configurable maximum, 25 hops by default, and at any node already visited. The current data has no cycles and its longest chain is 20 hops, for 9 products.

### Cross-tenant exposure

All 199 substitution edges and 150 of the 200 supplier edges cross tenants, so the graph cannot be both useful and fully hidden across tenants. Decided: a path is exposed in full to the tenant that owns its starting node, showing for other tenants' nodes only the ID, type, owner tenant and category, and never cost or price. The result tables carry the starting node's owner as `tenant_id`, which defines isolation.

### Build and re-runs

Nodes, edges and result tables are recomputed from the silver dimensions on every run, in a fixed order, and written with a `MERGE` on their grain that also deletes rows that no longer exist. The same dimensions always give the same graph, no edge is duplicated, and a run with no changes rewrites nothing. Store-to-product edges from the ledger are not part of v3.

### Ad hoc queries

The result tables answer the three traversal questions with plain SQL. Any other graph question runs on the GraphFrame `g` that `make pyspark` builds from `graph.nodes` and `graph.edges`, with a checkpoint directory already set. The tools available are motifs (including variable-length patterns), `bfs`, `all_paths` and graph algorithms. Every result is a DataFrame that can be joined with any lakehouse table. Examples are in `docs/queries.md`.

## v3: acceptance criteria

As for v2, the catalog criteria and the graph's structural rules are invariants checked by `make verify` on any data. The graph's counts are the baseline of the provided dataset, checked by `make verify-v3`.

Verified on November 16, 2026 on the real data and from a clean clone, and the invariants again on November 17. `make verify` passes 36 of 36 invariants, and `make verify-v3` passed every baseline number. `make verify-rerun WINDOW=2026-10-W1` left every table identical, and the graph and catalog tests pass, with no network.

### Catalog

- [x] `catalog.datasets` lists every Delta table in the lakehouse, with no table missing from the registry and no registry entry without a table at the end of a window run.
  Evidence: `make verify`: no table missing from either side, and no table with duplicate values of its declared key.
- [x] The reference load finishes without failing the registry check while gold and graph tables do not exist yet.
  Evidence: from a clean clone, `make reference SKIP_DOWNLOAD=1` ran before any window and finished; `make run-pending` then ran W1, W2 and W3.
- [x] Every row has a description, a layer, a scope and a key; every tenant-scoped table names its `tenant_column`.
  Evidence: `make verify`: 0 datasets missing any of them, and 0 columns without a description.
- [x] `catalog.columns` matches the actual schema of each Delta table.
  Evidence: `make verify`.
- [x] Following `catalog.lineage` upstream from any gold or graph table reaches at least one source object.
  Evidence: `make verify`.
- [x] `catalog.freshness` has one row per table and processed window, and shows the snapshot date for the global tables.
  Evidence: `make verify`: 105 rows (35 tables by 3 windows), and 0 rows of global tables without a snapshot date.
- [x] Only `bronze.open_prices` is flagged as containing personal data, on its `raw_json` column, and no catalog table contains a username from Open Prices.
  Evidence: `make verify-v3` (the flagged table and column); `make verify`: no table outside bronze is flagged, and 0 usernames in the catalog tables.
- [x] After re-running W1 once W3 is done, the catalog tables are identical except for load times.
  Evidence: `make verify-rerun`; only `catalog.freshness` receives a commit, for its run columns.

### Graph

- [x] `graph.nodes` has 327 rows (127 locations and 200 products) and `graph.edges` has 523 (124, 200 and 199 by type).
  Evidence: `make verify-v3`.
- [x] 72 stores depend, directly or transitively, on `DC_CENTRAL_HUB_01`: 48 directly and 24 through `DC_WEST_HUB_04`.
  Evidence: `make verify-v3`.
- [x] `graph.upstream_dependencies` has 264 rows: 96 stores at 2 hops from their plant and 24 stores at 3.
  Evidence: `make verify-v3`.
- [x] `graph.bottlenecks` flags exactly `PLANT_NA_01` and `DC_CENTRAL_HUB_01`, each with a 60% store share and 4 dependent tenants.
  Evidence: `make verify-v3`, with the threshold at 50% (see Open decisions).
- [x] `graph.cross_tenant_dependencies` has 368 rows: 19 supply edges, 150 supplier edges and 199 substitution edges.
  Evidence: `make verify-v3`.
- [x] `graph.substitution_paths` has 2,080 rows, a longest chain of 20 hops for 9 products, and no row for `PROD_0001`, which has no substitute.
  Evidence: `make verify-v3`.
- [x] A traversal over data with an artificial cycle, in a test, ends without looping.
  Evidence: `test_a_cycle_ends_and_paths_follow_the_chain`.
- [x] A query on a result table filtered by one tenant returns no rows whose starting node belongs to another tenant.
  Evidence: `make verify` and `test_graph_results_are_isolated_by_the_starting_node`: 0 result rows whose `tenant_id` is not the starting node's owner. `make verify` also checks that every edge endpoint is a node, that cross-tenant flags match the owners, that bottleneck flags follow the configured threshold, and that every path stays within the hop limit.
- [x] After re-running W1 once W3 is done, all graph tables are identical to the previous ones.
  Evidence: `make verify-rerun`; no graph table receives a commit.
- [x] The full run, including the graph, stays within the 2 GB memory limit.
  Evidence: the peak of the pipeline container with every layer was 1.0 GB.

## v4: Prefect orchestration layer

v4 is optional and not part of the commitment. It was built after v3 was complete and verified, and it changes no v1 to v3 table or code path.

- **What it adds:** Prefect flows that wrap the existing CLI, with retries and a run view:
  - `arkham-pending` loads the reference data if it never succeeded, then runs every pending window in order.
  - `arkham-window` runs or re-runs one window.
  - `arkham-verify-rerun` re-runs a window and compares the fingerprint of every table before and after.
- **Execution:** each step runs `python -m arkham <command>` in its own process, with the pipeline's interpreter and settings, as `make run-pending` does. The flow process imports Prefect but never Spark.
- **Retries:** a failed step is retried twice, 30 seconds apart. This is safe because every step leaves the tables identical when repeated. A step that still fails stops the flow, and later windows are not run.
- **Run view:** a Prefect server with its UI at `http://localhost:4200`, keeping its state in SQLite on a Docker volume.
- **Isolation:** Prefect has its own project and lock file (`orchestration/`), installed into its own virtualenv in a separate image target. It also has its own Compose profile (`orchestration`) and `prefect-*` make targets. The pipeline image and its dependencies are unchanged, nothing in v1 to v3 imports Prefect, and removing these pieces removes the layer.
- **Memory:** the orchestrator container has the pipeline's 2 GB limit, since it runs the flow process and one step's JVM at a time. The Prefect server, an auxiliary service, has 512 MB.

### Acceptance criteria

- [x] The same windows run through the flow and through the CLI with identical tables.
  Evidence: after the windows ran through the CLI, `make prefect-verify` re-ran W1, W2 and W3 through the flow and left all 48 table partitions identical.
- [x] A failed step is retried, and a step that still fails stops the flow before later windows run.
  Evidence: `make prefect-test` (6 tests against a fake CLI) covers the step order, retries, stopping on a failure and the re-run verification.
- [x] The pipeline does not depend on Prefect.
  Evidence: the pipeline image layers are identical before and after the layer was added, Prefect is not in the pipeline's virtualenv, and `make up`, `make build` and `make run-pending` work without the `orchestration` profile.
- [x] The flows run within the memory limits.
  Evidence: the orchestrator peaked at 840 to 963 MiB during `make prefect-verify`, and the Prefect server stayed under 200 MiB of its 512 MB.

## Decisions

Decided on November 15, 2026; each one changes results if decided differently.

| Decision | Decided | Alternative not taken | Reason | Version |
| --- | --- | --- | --- | --- |
| Stores in `RENOVATION_HOLD` | Included, with `operating_status` as a column | Excluded from gold | Silver keeps them, and consumers can filter them; 3 of the 48 stores with movements are on hold | v2 |
| Public price country | Prefer the store's country, fall back to the same currency, and label the match level | Require the same country | On the loaded data no sale matches at country level, so requiring it would leave coverage at zero; the label keeps the weaker match visible | v2 |
| Public price age | At most six months before the sale, configurable | No limit | Matches the six-month Open Prices baseline and avoids comparing a sale with a stale price | v2 |

## Open decisions

Each row is a proposal made in this document that changes results if decided differently; the rest of the rules follow directly from the v1 PRD.

| Decision | Proposal, as implemented | Alternative | Version |
| --- | --- | --- | --- |
| Bottleneck threshold | Store share at or above 50%, configurable in `[graph]` of `config/pipeline.toml` | Rank only, with no flag | v3 |

## Assumptions

- No source gives an opening stock balance, so inventory positions start at zero on October 1, 2026.
- The sign of `quantity_delta` is trusted over the name of the movement type.
- Net revenue is rounded per sale line, half up, before adding.
- A substitution link is a suggestion recorded in the ERP, not a guarantee that the products are commercially equivalent.
- The assumptions of the v1 PRD still hold.
