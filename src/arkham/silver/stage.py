"""The silver stage of a run: dimensions, facts and public references, in that order."""

from __future__ import annotations

from arkham.bronze.context import RunContext
from arkham.config import Window
from arkham.lakehouse import TABLES
from arkham.silver import dimensions, inventory, partner, quarantine, reference

# Everything silver writes, including its quarantine; `rebuild-silver` recreates these.
SILVER_TABLES = tuple(name for name, spec in TABLES.items() if spec.layer == "silver") + (
    quarantine.TABLE,
)


def run_facts(ctx: RunContext, window: Window) -> None:
    """The window's facts. They read their own window's ERP snapshot, never the current one."""
    inventory.load(ctx, window)
    partner.load(ctx, window)


def run_window(ctx: RunContext, window: Window) -> None:
    dimensions.load(ctx)
    run_facts(ctx, window)
    reference.load(ctx)


def run_reference(ctx: RunContext) -> None:
    reference.load(ctx)


def rebuild(ctx: RunContext, windows: list[Window]) -> None:
    """Replays the facts of `windows` in order. Dimensions and references are recomputed
    whole from bronze, so they are loaded once rather than once per window."""
    dimensions.load(ctx)
    for window in windows:
        run_facts(ctx, window)
    reference.load(ctx)
