from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest
from fakes import make_config, utc

from arkham import __main__ as cli
from arkham.config import (
    DEFAULT_CONFIG_PATH,
    ConfigError,
    Window,
    WindowCalendar,
    load_config,
    subtract_months,
)


def test_repository_config_loads_the_three_october_windows() -> None:
    config = load_config()
    assert [w.id for w in config.calendar.windows] == ["2026-10-W1", "2026-10-W2", "2026-10-W3"]
    assert config.calendar.first.start == utc(1)
    assert config.calendar.windows[-1].end == utc(16)
    assert len(config.dump.columns) == 11


def test_baseline_covers_the_six_months_before_the_first_window() -> None:
    assert make_config().baseline_range() == (date(2026, 4, 1), date(2026, 10, 1))


def test_window_range_includes_start_and_excludes_end() -> None:
    window = Window("w", utc(1), utc(6))
    assert window.contains(utc(1))
    assert not window.contains(utc(6))


def test_previous_and_before() -> None:
    calendar = make_config().calendar
    assert calendar.previous("2026-10-W1") is None
    assert calendar.previous("2026-10-W3").id == "2026-10-W2"
    assert [w.id for w in calendar.before("2026-10-W3")] == ["2026-10-W1", "2026-10-W2"]


def test_a_gap_between_windows_is_rejected() -> None:
    with pytest.raises(ConfigError, match="contiguous"):
        WindowCalendar([Window("a", utc(1), utc(6)), Window("b", utc(7), utc(11))])


def test_duplicate_window_ids_are_rejected() -> None:
    with pytest.raises(ConfigError, match="unique"):
        WindowCalendar([Window("a", utc(1), utc(6)), Window("a", utc(6), utc(11))])


def test_unknown_window_lists_the_known_ones() -> None:
    with pytest.raises(ConfigError, match="2026-10-W1"):
        make_config().calendar.get("2026-11-W9")


def test_unknown_window_explains_how_to_add_it() -> None:
    message = make_config().calendar.unknown_window_message("NEW", now=utc(30))
    assert "Nothing was run and no table was changed." in message
    # Contiguous with the last window and as long as it: 16 to 21 October.
    assert '  id = "NEW"\n  start = 2026-10-16T00:00:00Z\n  end = 2026-10-21T00:00:00Z' in message
    assert "has not ended yet" not in message


def test_unknown_window_warns_when_the_suggested_range_has_not_ended() -> None:
    message = make_config().calendar.unknown_window_message("NEW", now=utc(18))
    assert "has not ended yet" in message


def test_unknown_window_with_another_case_suggests_the_known_one() -> None:
    message = make_config().calendar.unknown_window_message(" 2026-10-w2")
    assert "Did you mean '2026-10-W2'?" in message
    assert "[[windows]]" not in message


def test_invalid_toml_is_reported_as_a_configuration_error(tmp_path: Path) -> None:
    broken = tmp_path / "pipeline.toml"
    broken.write_text('[[windows]]\nid = "W1"\nstart = 2026-10-01T00:00:00Z\nend = \n')
    with pytest.raises(ConfigError, match="not valid TOML"):
        load_config(broken)


def test_a_window_entry_without_end_is_reported(tmp_path: Path) -> None:
    config = tmp_path / "pipeline.toml"
    config.write_text(
        DEFAULT_CONFIG_PATH.read_text()
        + '\n[[windows]]\nid = "NEW"\nstart = 2026-10-16T00:00:00Z\n'
    )
    with pytest.raises(ConfigError, match="Window entry 4 .* is missing end"):
        load_config(config)


def test_the_cli_explains_an_unknown_window_without_starting_spark(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    def no_spark(config: object) -> None:
        raise AssertionError("the pipeline must not be built for an unknown window")

    monkeypatch.setattr(cli, "build_pipeline", no_spark)
    assert cli.main(["run", "--window", "2099-01-W1"]) == 2
    error = capsys.readouterr().err
    assert error.startswith("error: window '2099-01-W1' is not in the window calendar")
    assert "[[windows]]" in error


@pytest.mark.parametrize(
    ("day", "months", "expected"),
    [
        (date(2026, 10, 1), 6, date(2026, 4, 1)),
        (date(2026, 1, 15), 1, date(2025, 12, 15)),
        (date(2026, 3, 31), 1, date(2026, 2, 28)),
    ],
)
def test_subtract_months(day: date, months: int, expected: date) -> None:
    assert subtract_months(day, months) == expected
