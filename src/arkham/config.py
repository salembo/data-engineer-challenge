"""Pipeline configuration: the window calendar and the source settings in config/pipeline.toml."""

from __future__ import annotations

import json
import os
import tomllib
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_CONFIG_PATH = REPO_ROOT / "config" / "pipeline.toml"


class ConfigError(ValueError):
    """The configuration file is invalid."""


class UnknownWindowError(ConfigError):
    """A command named a window that is not in the calendar."""


def _shown(path: Path) -> str:
    """The path as the user knows it: relative to the repository when it is inside it."""
    try:
        return str(path.resolve().relative_to(REPO_ROOT))
    except ValueError:
        return str(path)


def _toml_datetime(instant: datetime) -> str:
    return instant.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


@dataclass(frozen=True)
class Window:
    """An ingestion window: UTC range, start included and end excluded."""

    id: str
    start: datetime
    end: datetime

    def contains(self, instant: datetime) -> bool:
        return self.start <= instant < self.end


class WindowCalendar:
    def __init__(self, windows: list[Window], source: Path = DEFAULT_CONFIG_PATH) -> None:
        self.source = source
        if not windows:
            raise ConfigError("The window calendar is empty.")
        ids = [window.id for window in windows]
        if len(set(ids)) != len(ids):
            raise ConfigError(f"Window IDs must be unique: {ids}")
        for window in windows:
            if window.start.utcoffset() != timedelta(0) or window.end.utcoffset() != timedelta(0):
                raise ConfigError(f"Window {window.id} must be expressed in UTC.")
            if window.start >= window.end:
                raise ConfigError(f"Window {window.id} must start before it ends.")
        for previous, current in zip(windows, windows[1:], strict=False):
            # Contiguous ranges guarantee that every record falls in exactly one window.
            if current.start != previous.end:
                raise ConfigError(
                    f"Windows must be contiguous and in order: {previous.id} ends at "
                    f"{previous.end.isoformat()} but {current.id} starts at "
                    f"{current.start.isoformat()}."
                )
        self.windows: tuple[Window, ...] = tuple(windows)

    def get(self, window_id: str) -> Window:
        for window in self.windows:
            if window.id == window_id:
                return window
        raise UnknownWindowError(self.unknown_window_message(window_id))

    def unknown_window_message(self, window_id: str, now: datetime | None = None) -> str:
        """Why the window cannot run, and how to add it to the calendar."""
        source = _shown(self.source)
        lines = [
            f"window {window_id!r} is not in the window calendar ({source}). "
            "Nothing was run and no table was changed.",
            "",
            "Windows in the calendar:",
            *(
                f"  {w.id}  {w.start:%Y-%m-%d %H:%M} -> {w.end:%Y-%m-%d %H:%M} UTC"
                for w in self.windows
            ),
            "",
        ]
        typed = window_id.strip().casefold()
        close = [w.id for w in self.windows if w.id.casefold() == typed]
        if close:
            lines.append(f"Did you mean {close[0]!r}? Window IDs are case sensitive.")
            return "\n".join(lines)

        last = self.windows[-1]
        start, end = last.end, last.end + (last.end - last.start)
        lines += [
            f"To run it, append this entry at the end of the calendar in {source} and run the "
            "command again; no rebuild is needed. Windows are contiguous, so it starts where "
            f"{last.id} ends. The end keeps the length of {last.id}; change it if the new "
            "window is longer or shorter.",
            "",
            "  [[windows]]",
            f"  id = {json.dumps(window_id)}",
            f"  start = {_toml_datetime(start)}",
            f"  end = {_toml_datetime(end)}",
        ]
        now = now or datetime.now(UTC)
        if end > now:
            lines += [
                "",
                f"That range has not ended yet (it is now {now:%Y-%m-%d %H:%M} UTC): a window "
                "run before its end loads only the records that have arrived so far.",
            ]
        return "\n".join(lines)

    def previous(self, window_id: str) -> Window | None:
        index = self.windows.index(self.get(window_id))
        return self.windows[index - 1] if index > 0 else None

    def before(self, window_id: str) -> tuple[Window, ...]:
        return self.windows[: self.windows.index(self.get(window_id))]

    @property
    def first(self) -> Window:
        return self.windows[0]


def subtract_months(day: date, months: int) -> date:
    """Same day of the month, `months` earlier; clamped to the last day of a shorter month."""
    month_index = day.year * 12 + (day.month - 1) - months
    year, month = divmod(month_index, 12)
    month += 1
    next_month = date(year + month // 12, month % 12 + 1, 1)
    last_day = (next_month - timedelta(days=1)).day
    return date(year, month, min(day.day, last_day))


@dataclass(frozen=True)
class HttpSettings:
    user_agent: str
    min_interval_seconds: float
    max_retries: int
    timeout_seconds: float


@dataclass(frozen=True)
class OpenPricesSettings:
    url: str
    page_size: int
    price_type: str
    baseline_months: int


@dataclass(frozen=True)
class ProductApiSettings:
    url: str
    fields: tuple[str, ...]


@dataclass(frozen=True)
class DumpSettings:
    url: str
    file_name: str
    columns: tuple[str, ...]


@dataclass(frozen=True)
class GoldSettings:
    # A public price older than this, relative to the sale date, is not comparable.
    public_price_max_age_months: int = 6


@dataclass(frozen=True)
class GraphSettings:
    # Traversals stop after this many hops, and at any node already visited.
    max_hops: int = 25
    # A supply node is a bottleneck when at least this share of all stores depends on it.
    bottleneck_store_share: float = 0.5


@dataclass(frozen=True)
class PipelineConfig:
    calendar: WindowCalendar
    public_http: HttpSettings
    partner_page_size: int
    open_prices: OpenPricesSettings
    product_api: ProductApiSettings
    dump: DumpSettings
    gold: GoldSettings = GoldSettings()
    graph: GraphSettings = GraphSettings()
    # The data catalog registry (config/catalog.toml next to the pipeline configuration).
    catalog_registry: Path = DEFAULT_CONFIG_PATH.parent / "catalog.toml"

    def baseline_range(self) -> tuple[date, date]:
        """Open Prices baseline by price date: [first window start - N months, first start)."""
        end = self.calendar.first.start.date()
        return subtract_months(end, self.open_prices.baseline_months), end


def _as_utc(value: object, field_name: str) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None:
        raise ConfigError(
            f"{field_name} must be a TOML datetime with an offset, e.g. ...T00:00:00Z"
        )
    return value.astimezone(UTC)


def _windows(raw: dict[str, object], path: Path) -> list[Window]:
    entries = raw.get("windows")
    if not isinstance(entries, list):
        raise ConfigError(f"{_shown(path)} has no [[windows]] entries.")
    windows = []
    for number, item in enumerate(entries, start=1):
        missing = [key for key in ("id", "start", "end") if key not in item]
        if missing:
            raise ConfigError(
                f"Window entry {number} in {_shown(path)} is missing {', '.join(missing)}; "
                "every [[windows]] entry needs id, start and end."
            )
        windows.append(
            Window(
                id=str(item["id"]),
                start=_as_utc(item["start"], f"windows[{item['id']}].start"),
                end=_as_utc(item["end"], f"windows[{item['id']}].end"),
            )
        )
    return windows


def load_config(path: Path | None = None) -> PipelineConfig:
    path = path or Path(os.environ.get("ARKHAM_CONFIG", DEFAULT_CONFIG_PATH))
    try:
        with path.open("rb") as handle:
            raw = tomllib.load(handle)
    except FileNotFoundError as error:
        raise ConfigError(f"Configuration file not found: {path}") from error
    except tomllib.TOMLDecodeError as error:
        raise ConfigError(f"{_shown(path)} is not valid TOML: {error}") from error
    try:
        return PipelineConfig(
            calendar=WindowCalendar(_windows(raw, path), source=path),
            public_http=HttpSettings(**raw["public_http"]),
            partner_page_size=int(raw["partner_api"]["page_size"]),
            open_prices=OpenPricesSettings(**raw["open_prices"]),
            product_api=ProductApiSettings(
                url=raw["off_product_api"]["url"],
                fields=tuple(raw["off_product_api"]["fields"]),
            ),
            dump=DumpSettings(
                url=raw["off_dump"]["url"],
                file_name=raw["off_dump"]["file_name"],
                columns=tuple(raw["off_dump"]["columns"]),
            ),
            gold=GoldSettings(**raw.get("gold", {})),
            graph=GraphSettings(**raw.get("graph", {})),
            catalog_registry=path.parent / "catalog.toml",
        )
    except (KeyError, TypeError) as error:
        raise ConfigError(f"Invalid configuration in {path}: {error}") from error
