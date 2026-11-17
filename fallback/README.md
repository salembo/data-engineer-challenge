# Fallback fixtures

Public sources can fail or rate-limit. When a request still fails after three retries and no earlier landed response exists, the pipeline loads these committed fixtures instead, finishes with status `SUCCESS_WITH_FALLBACK` and records which fixture it used and when it was captured (`docs/prd.md`, NFR-7).

| Folder | Content | Used for |
| --- | --- | --- |
| `open_food_facts/` | The 18 rows of the Open Food Facts dump whose barcode exists in the ERP, with the dump's header and all of its columns | `bronze.off_products` when the dump download fails, or with `make reference SKIP_DOWNLOAD=1` |
| `off_product_api/` | One product API response per ERP barcode | `bronze.off_product_api` |
| `open_prices/` | Open Prices prices of the ERP barcodes for the baseline months and the window calendar; the `owner` field (a username) is removed | `bronze.open_prices`, filtered to the window or baseline range |

Refresh the API fixtures with `make capture-fallback` (needs the ERP running and internet access).
