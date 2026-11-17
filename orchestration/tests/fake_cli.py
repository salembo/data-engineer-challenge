"""A stand-in for `python -m arkham` in the flow tests: records each call and answers
like the real CLI. FAKE_CLI_DIR holds the call log and the scripted failures."""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

state = Path(os.environ["FAKE_CLI_DIR"])
args = sys.argv[1:]
with (state / "calls.log").open("a") as log:
    log.write(" ".join(args) + "\n")

# failures.json: {"run --window W2": 1} fails that command once; -1 fails it always.
failures_file = state / "failures.json"
failures = json.loads(failures_file.read_text()) if failures_file.exists() else {}
key = " ".join(args)
if failures.get(key):
    if failures[key] > 0:
        failures[key] -= 1
        failures_file.write_text(json.dumps(failures))
    print(f"{key}: simulated failure", flush=True)
    sys.exit(1)

if args == ["plan-pending"]:
    print("26/11/16 10:00:00 WARN Spark noise that the flow does not log")
    print(json.dumps([["reference"], ["run", "--window", "W1"], ["run", "--window", "W2"]]))
elif args[0] == "fingerprint":
    content = (state / "tables.json").read_text() if (state / "tables.json").exists() else "[]"
    Path(args[args.index("--output") + 1]).write_text(content)
else:
    print(f"{key}: SUCCESS")
