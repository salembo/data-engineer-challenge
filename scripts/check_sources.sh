#!/usr/bin/env bash
# Smoke test for the source services:
#   - the ERP holds the complete seed from legacy_sources/erp_db/init_erp.sql,
#   - the pipeline's ERP role is read-only and works in UTC,
#   - the partner API answers with the expected contract.
# Everything runs inside the containers, so the host needs only Docker.
# Override COMPOSE to target another project, e.g. COMPOSE="docker compose -p arkham-verify".
set -euo pipefail

# Kept as an array so the command does not depend on IFS: the reads below set
# IFS='|', which would otherwise stop "docker compose" from splitting into words.
read -r -a compose_cmd <<<"${COMPOSE:-docker compose}"
dc() { "${compose_cmd[@]}" "$@"; }
failures=0

check() {
    local label="$1" actual="$2" expected="$3"
    if [[ "$actual" == "$expected" ]]; then
        printf 'ok    %-44s %s\n' "$label" "$actual"
    else
        printf 'FAIL  %-44s got %s, expected %s\n' "$label" "$actual" "$expected"
        failures=$((failures + 1))
    fi
}

# Runs a query as the pipeline's read-only role over TCP, the same way the pipeline connects.
erp_query() {
    dc exec -T erp-db sh -c \
        'PGPASSWORD="$ERP_DB_READER_PASSWORD" psql -h 127.0.0.1 -U "$ERP_DB_READER_USER" -d "$POSTGRES_DB" --no-psqlrc -At -F "|" -v ON_ERROR_STOP=1' \
        <<<"$1"
}

# Row counts produced by init_erp.sql. A function instead of an associative
# array, so the script also runs on the bash 3.2 that ships with macOS.
expected_rows() {
    case "$1" in
        tenants) echo 5 ;;
        locations) echo 128 ;;
        products) echo 200 ;;
        inventory_ledger) echo 250000 ;;
        *) echo "unknown table" ;;
    esac
}

echo "== ERP seed (as the read-only role)"
while IFS='|' read -r table rows; do
    check "arkham_erp.${table} rows" "$rows" "$(expected_rows "$table")"
done < <(erp_query "
SELECT 'tenants', COUNT(*) FROM arkham_erp.tenants
UNION ALL SELECT 'locations', COUNT(*) FROM arkham_erp.locations
UNION ALL SELECT 'products', COUNT(*) FROM arkham_erp.products
UNION ALL SELECT 'inventory_ledger', COUNT(*) FROM arkham_erp.inventory_ledger;")

echo "== ERP read-only role"
IFS='|' read -r timezone read_only can_insert can_create < <(erp_query "
SELECT current_setting('TimeZone'),
       current_setting('default_transaction_read_only'),
       has_table_privilege('arkham_erp.inventory_ledger', 'INSERT'),
       has_schema_privilege('arkham_erp', 'CREATE');")
check "session timezone" "$timezone" "UTC"
check "default_transaction_read_only" "$read_only" "on"
check "can INSERT into the ledger" "$can_insert" "f"
check "can CREATE in arkham_erp" "$can_create" "f"

echo "== Partner API"
api_result="$(dc exec -T partner-api python - <<'PY'
import json
import urllib.request

with urllib.request.urlopen("http://127.0.0.1:8000/health", timeout=5) as resp:
    health = json.load(resp)["status"]
with urllib.request.urlopen("http://127.0.0.1:8000/v1/events?since_cursor=0&page=1&page_size=1", timeout=5) as resp:
    schema_version = resp.headers.get("X-Arkham-Schema-Version")
    page = json.load(resp)
print(health, schema_version, page["count"], page["data"][0]["sequence_cursor"], sep="|")
PY
)"
IFS='|' read -r health schema_version page_count first_cursor <<<"$api_result"
check "GET /health status" "$health" "healthy"
check "X-Arkham-Schema-Version header" "$schema_version" "1.4"
check "first page with page_size=1: count" "$page_count" "1"
check "first page with since_cursor=0: cursor" "$first_cursor" "1"

echo
if ((failures > 0)); then
    echo "Source check FAILED: ${failures} check(s) did not match."
    exit 1
fi
echo "Source check passed."
