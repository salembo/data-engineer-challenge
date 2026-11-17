"""Landing zone: source files exactly as received, one directory per source and partition.

    landing/<source>/<partition_key>=<value>/
        responses.jsonl   one HTTP response body per line (API sources)
        <file>.csv.gz     the downloaded file (dump)
        _manifest.json    written last: extraction parameters and results

A partition counts as landed only when its manifest exists. Directories are
replaced atomically, so an interrupted extraction never leaves a partial
partition that a re-run would mistake for a complete one.
"""

from __future__ import annotations

import json
import shutil
import uuid
from collections.abc import Callable, Iterable
from pathlib import Path
from typing import Any

MANIFEST = "_manifest.json"
RESPONSES = "responses.jsonl"


def one_line(body: bytes) -> bytes:
    """A JSON body on a single line. Raw newlines can only be whitespace between JSON
    tokens (inside strings they must be escaped), so replacing them keeps the document
    identical."""
    return body.replace(b"\r", b" ").replace(b"\n", b" ")


class LandingZone:
    def __init__(self, root: Path) -> None:
        self.root = root

    def partition_dir(self, source: str, key: str, value: str) -> Path:
        return self.root / source / f"{key}={value}"

    def read_manifest(self, source: str, key: str, value: str) -> dict[str, Any] | None:
        path = self.partition_dir(source, key, value) / MANIFEST
        if not path.exists():
            return None
        return json.loads(path.read_text())

    def partitions(self, source: str, key: str) -> list[str]:
        """Values of the landed partitions of a source, sorted."""
        base = self.root / source
        if not base.exists():
            return []
        prefix = f"{key}="
        return sorted(
            path.name[len(prefix) :]
            for path in base.iterdir()
            if path.is_dir() and path.name.startswith(prefix) and (path / MANIFEST).exists()
        )

    def write_partition(
        self,
        source: str,
        key: str,
        value: str,
        manifest: dict[str, Any],
        responses: Iterable[bytes] | None = None,
        populate: Callable[[Path], None] | None = None,
    ) -> Path:
        """Writes a partition in a temporary directory, then swaps it into place."""
        final = self.partition_dir(source, key, value)
        final.parent.mkdir(parents=True, exist_ok=True)
        staging = final.parent / f".staging-{uuid.uuid4().hex}"
        staging.mkdir()
        try:
            if responses is not None:
                with (staging / RESPONSES).open("wb") as handle:
                    for body in responses:
                        handle.write(one_line(body) + b"\n")
            if populate is not None:
                populate(staging)
            (staging / MANIFEST).write_text(json.dumps(manifest, indent=2, sort_keys=True))
            if final.exists():
                retired = final.parent / f".retired-{uuid.uuid4().hex}"
                final.rename(retired)
                staging.rename(final)
                shutil.rmtree(retired)
            else:
                staging.rename(final)
        except BaseException:
            shutil.rmtree(staging, ignore_errors=True)
            raise
        return final

    def read_responses(self, source: str, key: str, value: str) -> list[bytes]:
        path = self.partition_dir(source, key, value) / RESPONSES
        if not path.exists():
            return []
        return [line for line in path.read_bytes().splitlines() if line]
