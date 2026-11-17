"""Runtime settings read from environment variables.

docker-compose.yml sets every variable for the pipeline container; the defaults
here only serve local runs and tests.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path


def _required(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"Environment variable {name} is required but not set.")
    return value


@dataclass(frozen=True)
class SparkSettings:
    """Spark session sizing. Defaults fit the 2 GB limit of the pipeline container."""

    master: str = "local[2]"
    driver_memory: str = "1g"
    shuffle_partitions: int = 4
    jars_dir: Path | None = None

    @classmethod
    def from_env(cls) -> SparkSettings:
        jars_dir = os.environ.get("ARKHAM_SPARK_JARS_DIR")
        return cls(
            master=os.environ.get("SPARK_MASTER", cls.master),
            driver_memory=os.environ.get("SPARK_DRIVER_MEMORY", cls.driver_memory),
            shuffle_partitions=int(
                os.environ.get("SPARK_SHUFFLE_PARTITIONS", cls.shuffle_partitions)
            ),
            jars_dir=Path(jars_dir) if jars_dir else None,
        )


@dataclass(frozen=True)
class ErpConnection:
    """JDBC connection to the legacy ERP, using the read-only role."""

    jdbc_url: str
    user: str
    password: str = field(repr=False)

    @classmethod
    def from_env(cls) -> ErpConnection:
        return cls(
            jdbc_url=_required("ARKHAM_ERP_JDBC_URL"),
            user=_required("ARKHAM_ERP_USER"),
            password=_required("ARKHAM_ERP_PASSWORD"),
        )


def partner_api_url_from_env() -> str:
    return _required("ARKHAM_PARTNER_API_URL").rstrip("/")
