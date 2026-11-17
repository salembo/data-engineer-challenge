#!/bin/sh
# Creates the read-only role the pipeline uses to extract from the ERP, so the
# pipeline can never write to its source.
#
# The official Postgres image runs this once, on the first start with an empty
# data volume, right after legacy_sources/erp_db/init_erp.sql (alphabetical
# order). It is idempotent, so it can also be applied to an existing volume:
#   docker compose exec erp-db /docker-entrypoint-initdb.d/02_create_reader_role.sh
set -eu

psql -v ON_ERROR_STOP=1 --no-psqlrc --quiet \
    --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" \
    --set db="$POSTGRES_DB" \
    --set reader_user="$ERP_DB_READER_USER" \
    --set reader_password="$ERP_DB_READER_PASSWORD" <<'SQL'
SELECT format('CREATE ROLE %I LOGIN', :'reader_user')
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = :'reader_user')
\gexec

ALTER ROLE :"reader_user" WITH LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE PASSWORD :'reader_password';
ALTER ROLE :"reader_user" SET default_transaction_read_only = on;
ALTER ROLE :"reader_user" SET timezone = 'UTC';

GRANT CONNECT ON DATABASE :"db" TO :"reader_user";
GRANT USAGE ON SCHEMA arkham_erp TO :"reader_user";
GRANT SELECT ON ALL TABLES IN SCHEMA arkham_erp TO :"reader_user";
ALTER DEFAULT PRIVILEGES IN SCHEMA arkham_erp GRANT SELECT ON TABLES TO :"reader_user";
SQL

echo "Read-only role '$ERP_DB_READER_USER' is ready on database '$POSTGRES_DB'."
