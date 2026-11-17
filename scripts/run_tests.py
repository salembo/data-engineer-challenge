"""Runs the test suite with one pytest process, and so one Spark JVM, per test file.

Every window a test runs goes through bronze, silver, gold, the graph and the catalog.
In a single JVM for the whole suite, what each test leaves behind (Delta logs, cached
plans, GraphX state) adds up until the 1 GB heap runs out, and once the JVM dies every
later test fails with "Connection refused". A JVM per file bounds that to one file.

Files that share a session fixture run in the same process (SHARED), so the fixture is
built once. Arguments are passed to pytest; if some of them are test paths, only those
run, together, in one process.

Usage (inside the pipeline container): python scripts/run_tests.py [pytest arguments]
"""

from __future__ import annotations

import subprocess
import sys
import time
from pathlib import Path

TESTS = Path(__file__).resolve().parents[1] / "tests"
# Files that use the session fixture `derived` (tests/conftest.py): three windows through
# every layer, built once for both.
SHARED = (("test_gold_pipeline.py", "test_graph_catalog_pipeline.py", "test_invariants.py"),)


def groups() -> list[list[str]]:
    files = sorted(path.name for path in TESTS.glob("test_*.py"))
    shared = {name for group in SHARED for name in group}
    result = [[f"tests/{name}" for name in group if name in files] for group in SHARED]
    result += [[f"tests/{name}"] for name in files if name not in shared]
    return [group for group in result if group]


def main(arguments: list[str]) -> int:
    paths = [a for a in arguments if a.startswith("tests/") or a.endswith(".py")]
    options = [a for a in arguments if a not in paths]
    plan = [paths] if paths else groups()
    results = []
    for group in plan:
        print(f"\n=== {' '.join(group)}", flush=True)
        started = time.monotonic()
        code = subprocess.run(
            [sys.executable, "-m", "pytest", "-p", "no:cacheprovider", *options, *group],
            check=False,
        ).returncode
        results.append((group, code, time.monotonic() - started))
    print("\n=== Summary (one JVM per line)")
    for group, code, seconds in results:
        # pytest exit code 5 means no test was selected (for example with -k).
        state = "passed" if code in (0, 5) else f"FAILED (exit {code})"
        print(f"  {state:<18} {seconds:>6.0f}s  {' '.join(group)}")
    failed = [group for group, code, _ in results if code not in (0, 5)]
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
