# Pipeline runtime: Python 3.11, Java 21, PySpark 4.1, Delta Lake 4.4 and GraphFrames.
# Every Python package comes from poetry.lock and every JAR from spark-jars.lock (with its
# checksum), both fixed at build time, so a run never downloads anything.
#
# Build speed: PySpark (about 480 MB installed) is its own layer, keyed only on its
# version, so changing another dependency does not download it again; the JARs are a
# separate stage that BuildKit builds in parallel; and BuildKit cache mounts keep pip,
# Poetry and JAR downloads between builds, so a rebuild downloads only what changed.

ARG PYTHON_IMAGE=python:3.11.18-slim-trixie
ARG JRE_IMAGE=eclipse-temurin:21.0.12.1_1-jre-noble

FROM ${JRE_IMAGE} AS jre

FROM ${PYTHON_IMAGE} AS base
ENV JAVA_HOME=/opt/java/openjdk \
    VIRTUAL_ENV=/opt/venv \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    MALLOC_ARENA_MAX=2 \
    TZ=UTC
ENV PATH=${VIRTUAL_ENV}/bin:${JAVA_HOME}/bin:${PATH}
COPY --from=jre /opt/java/openjdk /opt/java/openjdk

# ---- Spark JARs: downloaded from the lock file and verified; no JVM needed ----
FROM ${PYTHON_IMAGE} AS jars
WORKDIR /build
COPY src/arkham/spark_packages.py docker/pipeline/fetch_spark_jars.py docker/pipeline/spark-jars.lock ./
RUN --mount=type=cache,target=/root/.cache/spark-jars \
    python fetch_spark_jars.py spark_packages.py spark-jars.lock /opt/spark-jars /root/.cache/spark-jars

# ---- Python packages: PySpark first, then everything else from the lock file ----
FROM base AS deps
ARG POETRY_VERSION=2.5.1
# Must match the pyspark pin in pyproject.toml; the build fails below if it does not.
ARG PYSPARK_VERSION=4.1.4
# The base image disables pip's cache (any value of PIP_NO_CACHE_DIR does); these steps
# unset it and use a cache mount, which also keeps the wheel pip builds for PySpark.
RUN --mount=type=cache,target=/root/.cache/pip \
    python -m venv /opt/poetry \
    && env -u PIP_NO_CACHE_DIR /opt/poetry/bin/pip install "poetry==${POETRY_VERSION}" \
    && python -m venv "${VIRTUAL_ENV}" \
    && env -u PIP_NO_CACHE_DIR pip install "pyspark==${PYSPARK_VERSION}"
WORKDIR /build
# pyproject.toml names README.md; an empty one keeps README edits from invalidating
# this layer.
COPY pyproject.toml poetry.lock ./
RUN touch README.md
# Poetry installs into the active virtualenv given by VIRTUAL_ENV, keeping the PySpark
# installed above when its version matches the lock. The dev group (pytest, black, ruff)
# is included so tests and lint run in this same image.
RUN --mount=type=cache,target=/root/.cache/pypoetry \
    /opt/poetry/bin/poetry install --no-root --with dev --no-interaction --no-ansi \
    && installed="$(python -c 'import pyspark; print(pyspark.__version__)')" \
    && if [ "${installed}" != "${PYSPARK_VERSION}" ]; then \
         echo "PYSPARK_VERSION=${PYSPARK_VERSION} but poetry.lock pins ${installed}: update the Dockerfile" >&2; \
         exit 1; \
       fi

# ---- Runtime ----
FROM base AS runtime
COPY --from=deps /opt/venv /opt/venv
COPY --from=jars /opt/spark-jars /opt/spark-jars

# Runs as the host user's UID/GID (passed by the Makefile), so files written to
# the bind-mounted landing/ and lakehouse/ stay owned by that user on Linux.
ARG APP_UID=10001
ARG APP_GID=10001
RUN groupadd --non-unique --gid "${APP_GID}" app \
    && useradd --non-unique --uid "${APP_UID}" --gid "${APP_GID}" --create-home --shell /bin/bash app

WORKDIR /app
COPY pyproject.toml README.md ./
COPY config ./config
COPY fallback ./fallback
COPY scripts ./scripts
COPY src ./src
COPY tests ./tests
ENV PYTHONPATH=/app/src \
    ARKHAM_SPARK_JARS_DIR=/opt/spark-jars \
    ARKHAM_CONFIG=/app/config/pipeline.toml \
    ARKHAM_FALLBACK_ROOT=/app/fallback

USER app
CMD ["python", "-m", "arkham.smoke"]

# ---- Optional orchestration layer (v4): the runtime plus Prefect ----
# Built only by `make prefect-build` (target orchestrator); the pipeline image above is
# untouched. Prefect lives in its own virtualenv from its own lock file
# (orchestration/poetry.lock), so its dependencies never mix with the pipeline's.
FROM deps AS orchestration-deps
WORKDIR /build/orchestration
COPY orchestration/pyproject.toml orchestration/poetry.lock ./
RUN --mount=type=cache,target=/root/.cache/pypoetry \
    python -m venv /opt/prefect \
    && VIRTUAL_ENV=/opt/prefect /opt/poetry/bin/poetry install --no-root --with dev \
       --no-interaction --no-ansi

FROM runtime AS orchestrator
USER root
COPY --from=orchestration-deps /opt/prefect /opt/prefect
USER app
# Prefect server state (a named volume takes this directory's owner).
RUN mkdir -p /home/app/prefect
COPY orchestration ./orchestration
# Prefect keeps its local state under PREFECT_HOME; the CLI steps still run with the
# pipeline's interpreter (/opt/venv), so the pipeline's PATH order is kept.
ENV PREFECT_HOME=/tmp/prefect \
    PREFECT_SERVER_ANALYTICS_ENABLED=false
CMD ["/opt/prefect/bin/python", "orchestration/flows.py", "--help"]
