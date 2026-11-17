"""Downloads the Spark JARs listed in spark-jars.lock at image build time.

Every file is checked against its SHA-256 in the lock, downloads run in parallel, and a
cache directory (a BuildKit cache mount) keeps verified files between builds, so a
rebuild downloads nothing that did not change. No JVM is started. The build fails if the
lock does not match src/arkham/spark_packages.py: run `make lock-jars` after changing it.

Standard library only: it runs before any Python package is installed.

Usage: python fetch_spark_jars.py <spark_packages.py> <spark-jars.lock> <output_dir> <cache_dir>
"""

from __future__ import annotations

import hashlib
import http.client
import importlib.util
import json
import shutil
import sys
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

# Docker Desktop's network can stall a download when several run at once: a stalled read
# fails after READ_TIMEOUT seconds and is retried.
ATTEMPTS = 5
READ_TIMEOUT = 30


def load_packages(module_path: Path) -> tuple[str, ...]:
    spec = importlib.util.spec_from_file_location("spark_packages", module_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Cannot load {module_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.SPARK_PACKAGES


def digest(path: Path) -> str:
    sha = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            sha.update(chunk)
    return sha.hexdigest()


def fetch(jar: dict[str, str], cache_dir: Path) -> tuple[Path, bool]:
    """The verified JAR in the cache, and whether it had to be downloaded."""
    cached = cache_dir / jar["file"]
    if cached.exists() and digest(cached) == jar["sha256"]:
        return cached, False
    partial = cached.with_suffix(".part")
    for attempt in range(1, ATTEMPTS + 1):
        try:
            with (
                urllib.request.urlopen(jar["url"], timeout=READ_TIMEOUT) as response,  # noqa: S310
                partial.open("wb") as handle,
            ):
                shutil.copyfileobj(response, handle)
            if digest(partial) != jar["sha256"]:
                raise ValueError(f"checksum mismatch for {jar['url']}")
            partial.replace(cached)
            return cached, True
        except (OSError, http.client.HTTPException, ValueError) as error:
            if attempt == ATTEMPTS:
                raise RuntimeError(f"Could not fetch {jar['url']}: {error}") from error
            time.sleep(2 * attempt)
    raise AssertionError("unreachable")


def main(module_path: Path, lock_path: Path, output_dir: Path, cache_dir: Path) -> None:
    lock = json.loads(lock_path.read_text())
    packages = load_packages(module_path)
    if tuple(lock["packages"]) != packages:
        raise SystemExit(
            f"{lock_path.name} locks {lock['packages']} but spark_packages.py declares "
            f"{list(packages)}. Run `make lock-jars`, then build again."
        )
    cache_dir.mkdir(parents=True, exist_ok=True)
    output_dir.mkdir(parents=True, exist_ok=True)
    with ThreadPoolExecutor(max_workers=4) as pool:
        results = list(pool.map(lambda jar: fetch(jar, cache_dir), lock["jars"]))
    for (cached, _), jar in zip(results, lock["jars"], strict=True):
        shutil.copy2(cached, output_dir / jar["file"])
    downloaded = sum(1 for _, was_downloaded in results if was_downloaded)
    cached = len(results) - downloaded
    print(f"{len(results)} JARs verified ({downloaded} downloaded, {cached} cached)")


if __name__ == "__main__":
    main(Path(sys.argv[1]), Path(sys.argv[2]), Path(sys.argv[3]), Path(sys.argv[4]))
