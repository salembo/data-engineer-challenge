SHELL := /bin/bash
COMPOSE ?= docker compose
SOURCES := erp-db partner-api

# The pipeline image runs as the host user, so files it writes to the
# bind-mounted landing/ and lakehouse/ stay owned by that user.
HOST_UID ?= $(shell id -u)
HOST_GID ?= $(shell id -g)
export HOST_UID HOST_GID

# The pipeline explains its own errors; Docker's "What's next" hint after a failed
# command adds nothing to that. DOCKER_CLI_HINTS=true brings it back.
DOCKER_CLI_HINTS ?= false
export DOCKER_CLI_HINTS

PIPELINE := $(COMPOSE) run --rm pipeline
# For commands that need no source service and must see the whole repository.
PIPELINE_WORKSPACE := $(COMPOSE) run --rm --no-deps -v "$(CURDIR):/workspace" -w /workspace pipeline

.DEFAULT_GOAL := help
.PHONY: help up down clean ps logs psql check-sources build smoke run run-pending reference status \
	fingerprint verify-rerun rebuild-silver rebuild-gold verify verify-v1 verify-v2 verify-v3 pyspark \
	capture-fallback test \
	lint format shell lock lock-jars dirs prefect-build prefect-up prefect-down prefect-pending \
	prefect-run prefect-verify prefect-test prefect-lock

# Same Python and Poetry versions as the pipeline image (docker/pipeline/Dockerfile).
LOCK_IMAGE := python:3.11.18-slim-trixie
POETRY_VERSION := 2.5.1

help: ## List the available targets
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "} {printf "  %-15s %s\n", $$1, $$2}'

# ---- Source services ----

# No --build here: every build gets a new image ID, which would recreate (restart) the
# running sources on every `make run`. Images are built when missing, or by `make build`.
up: ## Start the source services (ERP and partner API) and wait until they are healthy
	$(COMPOSE) up -d --wait $(SOURCES)

down: ## Stop the services and keep the ERP data volume
	$(COMPOSE) down

clean: ## Stop the services and delete the ERP data volume; the next `make up` seeds it again
	$(COMPOSE) down -v --remove-orphans

ps: ## Show service status and health
	$(COMPOSE) ps

logs: ## Follow the source service logs
	$(COMPOSE) logs -f --tail=100 $(SOURCES)

psql: ## Open a psql shell on the ERP as the pipeline's read-only role
	$(COMPOSE) exec erp-db sh -c 'PGPASSWORD="$$ERP_DB_READER_PASSWORD" psql -h 127.0.0.1 -U "$$ERP_DB_READER_USER" -d "$$POSTGRES_DB"'

check-sources: ## Verify the ERP seed counts, the read-only role and the partner API contract
	COMPOSE="$(COMPOSE)" ./scripts/check_sources.sh

# ---- Pipeline runtime ----

build: ## Build every image: the partner API and the pipeline (Python 3.11, Java 21, PySpark, Delta)
	$(COMPOSE) --profile pipeline build partner-api pipeline

smoke: up dirs ## Check the pipeline runtime end to end: Spark, Delta, ERP over JDBC, partner API, memory
	$(PIPELINE) python -m arkham.smoke

# ---- Pipeline ----

run: up dirs ## Run one window: make run WINDOW=2026-10-W1
	@test -n "$(WINDOW)" || { echo "Usage: make run WINDOW=<window id from config/pipeline.toml>"; exit 2; }
	$(PIPELINE) python -m arkham run --window "$(WINDOW)"

run-pending: up dirs ## Load reference data if needed, then every pending window in order
	$(PIPELINE) python -m arkham run-pending

reference: up dirs ## Load the Open Food Facts dump and the Open Prices baseline (SKIP_DOWNLOAD=1 uses the fixture, FORCE=1 downloads again)
	$(PIPELINE) python -m arkham reference $(if $(SKIP_DOWNLOAD),--skip-download) $(if $(FORCE),--force-download)

status: up dirs ## Show the state of each window and the bronze row counts
	$(PIPELINE) python -m arkham status

fingerprint: up dirs ## Print the row count and content hash of every bronze partition
	$(PIPELINE) python -m arkham fingerprint

verify-rerun: up dirs ## Re-run a window and check every table is unchanged: make verify-rerun WINDOW=2026-10-W1
	@test -n "$(WINDOW)" || { echo "Usage: make verify-rerun WINDOW=<window id>"; exit 2; }
	$(PIPELINE) python -m arkham verify-rerun --window "$(WINDOW)"

rebuild-silver: up dirs ## Recreate silver and its quarantine from bronze, then gold (bronze and landing are untouched)
	$(PIPELINE) python -m arkham rebuild-silver

rebuild-gold: up dirs ## Recompute gold, the graph and the catalog from silver (only rows that changed are written)
	$(PIPELINE) python -m arkham rebuild-gold

verify: up dirs ## Check the invariants: properties that hold on any data and any window calendar
	$(PIPELINE) python -m arkham verify

verify-v1: up dirs ## Check the v1 baseline numbers of the provided data (not applicable to other data)
	$(PIPELINE) python -m arkham verify-v1

verify-v2: up dirs ## Check the v2 baseline numbers of the provided data (not applicable to other data)
	$(PIPELINE) python -m arkham verify-v2

verify-v3: up dirs ## Check the v3 baseline numbers of the provided data (not applicable to other data)
	$(PIPELINE) python -m arkham verify-v3

pyspark: up dirs ## Interactive PySpark with every lakehouse table registered (bronze.*, platform.*)
	$(PIPELINE) python -i -m arkham.shell

capture-fallback: up dirs ## Refresh the committed public-API fallback fixtures (needs internet)
	$(PIPELINE) python scripts/capture_public_fallback.py

test: dirs ## Run the tests inside the pipeline container, one JVM per test file (no network); ARGS="-k name" filters
	$(COMPOSE) run --rm --no-deps pipeline python scripts/run_tests.py $(ARGS)

lint: ## Check formatting (black) and lint rules (ruff)
	$(PIPELINE_WORKSPACE) sh -c 'black --check . && ruff check .'

format: ## Format the code (black) and apply safe lint fixes (ruff)
	$(PIPELINE_WORKSPACE) sh -c 'black . && ruff check --fix .'

shell: up dirs ## Open a shell in the pipeline container
	$(PIPELINE) bash

lock-jars: ## Regenerate docker/pipeline/spark-jars.lock after editing src/arkham/spark_packages.py (then run `make build`)
	$(PIPELINE_WORKSPACE) python docker/pipeline/lock_spark_jars.py src/arkham/spark_packages.py docker/pipeline/spark-jars.lock

lock: ## Regenerate poetry.lock after editing pyproject.toml (then run `make build`)
	docker run --rm --user "$(HOST_UID):$(HOST_GID)" -e HOME=/tmp -v "$(CURDIR):/workspace" -w /workspace $(LOCK_IMAGE) \
		sh -c 'pip install --quiet --user --no-warn-script-location --disable-pip-version-check "poetry==$(POETRY_VERSION)" && /tmp/.local/bin/poetry lock'

dirs:
	@mkdir -p landing lakehouse

# ---- Optional orchestration layer (v4): Prefect ----
# Independent of everything above: these targets, orchestration/ and the compose profile
# "orchestration" can be removed without touching the pipeline.
ORCHESTRATOR := $(COMPOSE) --profile orchestration run --rm orchestrator /opt/prefect/bin/python orchestration/flows.py

prefect-build: ## [Prefect] Build the orchestrator image (pipeline runtime plus Prefect)
	$(COMPOSE) --profile orchestration build orchestrator

prefect-up: up ## [Prefect] Start the Prefect server; run view at http://localhost:4200
	$(COMPOSE) --profile orchestration up -d --wait prefect-server

prefect-down: ## [Prefect] Stop the Prefect server (its run history is kept)
	$(COMPOSE) --profile orchestration stop prefect-server

prefect-pending: up dirs ## [Prefect] Reference data if needed, then pending windows, through the flow
	$(ORCHESTRATOR) pending

prefect-run: up dirs ## [Prefect] Run one window through the flow: make prefect-run WINDOW=2026-10-W1
	@test -n "$(WINDOW)" || { echo "Usage: make prefect-run WINDOW=<window id>"; exit 2; }
	$(ORCHESTRATOR) run --window "$(WINDOW)"

prefect-verify: up dirs ## [Prefect] Re-run a window through the flow and check every table is unchanged
	@test -n "$(WINDOW)" || { echo "Usage: make prefect-verify WINDOW=<window id>"; exit 2; }
	$(ORCHESTRATOR) verify-rerun --window "$(WINDOW)"

prefect-test: ## [Prefect] Test the flows against a fake CLI on a temporary Prefect server
	$(COMPOSE) --profile orchestration run --rm --no-deps orchestrator \
		/opt/prefect/bin/python -m pytest -p no:cacheprovider orchestration/tests

prefect-lock: ## [Prefect] Regenerate orchestration/poetry.lock after editing orchestration/pyproject.toml
	docker run --rm --user "$(HOST_UID):$(HOST_GID)" -e HOME=/tmp -v "$(CURDIR):/workspace" -w /workspace/orchestration $(LOCK_IMAGE) \
		sh -c 'pip install --quiet --user --no-warn-script-location --disable-pip-version-check "poetry==$(POETRY_VERSION)" && /tmp/.local/bin/poetry lock'
