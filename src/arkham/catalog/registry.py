"""The catalog registry: what each table is, declared in config/catalog.toml.

The registry declares descriptions, scope, keys, load pattern, lineage and personal data.
Column names and types are never declared: the catalog reads them from the Delta tables.
"""

from __future__ import annotations

import tomllib
from dataclasses import dataclass, field
from pathlib import Path

from arkham.config import ConfigError

SCOPES = ("TENANT", "GLOBAL", "PLATFORM")
# Snapshot, incremental by window, reference or derived (the v2-v4 PRD); operational for
# the run log, which records runs rather than data.
LOAD_PATTERNS = ("snapshot", "incremental", "reference", "derived", "operational")


@dataclass(frozen=True)
class SourceObject:
    name: str
    system: str
    location: str
    description: str


@dataclass(frozen=True)
class TableEntry:
    name: str
    description: str
    scope: str
    tenant_column: str | None
    primary_key: tuple[str, ...]
    load_pattern: str
    upstream: tuple[str, ...]
    transformation: str
    personal_data_columns: tuple[str, ...] = ()
    columns: dict[str, str] = field(default_factory=dict)

    @property
    def layer(self) -> str:
        return self.name.split(".", 1)[0]


@dataclass(frozen=True)
class Registry:
    sources: dict[str, SourceObject]
    tables: dict[str, TableEntry]
    glossary: dict[str, str]

    def column_description(self, table: str, column: str) -> str | None:
        return self.tables[table].columns.get(column) or self.glossary.get(column)

    def source_objects_of(self, table: str) -> list[str]:
        """The source objects a table ultimately comes from, following its lineage."""
        found: set[str] = set()
        pending, seen = [table], set()
        while pending:
            current = pending.pop()
            if current in seen:
                continue
            seen.add(current)
            for upstream in self.tables[current].upstream:
                if upstream in self.sources:
                    found.add(upstream)
                else:
                    pending.append(upstream)
        return sorted(found)


def _tuple(value: object, where: str) -> tuple[str, ...]:
    if not isinstance(value, list) or not all(isinstance(v, str) for v in value):
        raise ConfigError(f"{where} must be a list of strings")
    return tuple(value)


def load_registry(path: Path) -> Registry:
    with path.open("rb") as handle:
        raw = tomllib.load(handle)
    try:
        sources = {
            name: SourceObject(name, item["system"], item["location"], item["description"])
            for name, item in raw["sources"].items()
        }
        tables = {}
        for name, item in raw["tables"].items():
            tables[name] = TableEntry(
                name=name,
                description=item["description"],
                scope=item["scope"],
                tenant_column=item.get("tenant_column"),
                primary_key=_tuple(item["primary_key"], f"{name}.primary_key"),
                load_pattern=item["load_pattern"],
                upstream=_tuple(item["upstream"], f"{name}.upstream"),
                transformation=item["transformation"],
                personal_data_columns=_tuple(
                    item.get("personal_data_columns", []), f"{name}.personal_data_columns"
                ),
                columns=dict(item.get("columns", {})),
            )
        glossary = dict(raw.get("columns", {}))
    except (KeyError, TypeError) as error:
        raise ConfigError(f"Invalid catalog registry {path}: missing {error}") from error

    problems = []
    for entry in tables.values():
        if not entry.description.strip():
            problems.append(f"{entry.name}: empty description")
        if entry.scope not in SCOPES:
            problems.append(f"{entry.name}: scope {entry.scope!r} is not one of {SCOPES}")
        if entry.load_pattern not in LOAD_PATTERNS:
            problems.append(f"{entry.name}: load_pattern {entry.load_pattern!r} is unknown")
        if entry.scope == "TENANT" and not entry.tenant_column:
            problems.append(f"{entry.name}: a tenant-scoped table must name its tenant_column")
        if entry.scope != "TENANT" and entry.tenant_column:
            problems.append(f"{entry.name}: only tenant-scoped tables have a tenant_column")
        if not entry.primary_key:
            problems.append(f"{entry.name}: empty primary_key")
        for upstream in entry.upstream:
            if upstream not in tables and upstream not in sources:
                problems.append(f"{entry.name}: unknown upstream {upstream!r}")
    if problems:
        raise ConfigError(f"Invalid catalog registry {path}:\n  " + "\n  ".join(problems))
    return Registry(sources, tables, glossary)
