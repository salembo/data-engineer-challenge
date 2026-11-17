from __future__ import annotations

import http.client
import io
import urllib.error
from pathlib import Path
from typing import Any

import pytest

from arkham.http import HttpClient, HttpError


class FakeResponse(io.BytesIO):
    headers: dict[str, str] = {}
    status = 200


class PartialResponse(FakeResponse):
    status = 206


class ScriptedOpener:
    """Returns or raises the scripted outcomes in order and records each request."""

    def __init__(self, outcomes: list[Any]) -> None:
        self.outcomes = outcomes
        self.requests: list[Any] = []

    def __call__(self, request: Any, timeout: float) -> FakeResponse:
        self.requests.append(request)
        outcome = self.outcomes.pop(0)
        if isinstance(outcome, Exception):
            raise outcome
        if isinstance(outcome, FakeResponse):
            return outcome
        return FakeResponse(outcome)


def http_error(code: int) -> urllib.error.HTTPError:
    return urllib.error.HTTPError("https://x.test", code, "error", {}, None)  # type: ignore[arg-type]


def client(opener: ScriptedOpener, sleeps: list[float], **kwargs: Any) -> HttpClient:
    return HttpClient(
        "arkham-test-agent", opener=opener, sleep=sleeps.append, clock=lambda: 0.0, **kwargs
    )


def test_returns_the_body_as_received_and_sends_the_user_agent() -> None:
    opener = ScriptedOpener([b'{"a": 1}'])
    body, payload = client(opener, []).get_json("https://x.test/p", {"q": "v w"})
    assert body == b'{"a": 1}'
    assert payload == {"a": 1}
    assert opener.requests[0].full_url == "https://x.test/p?q=v+w"
    assert opener.requests[0].get_header("User-agent") == "arkham-test-agent"


def test_retries_server_errors_and_truncated_json() -> None:
    opener = ScriptedOpener([http_error(503), b'{"items": [{"id"', b'{"ok": true}'])
    sleeps: list[float] = []
    _, payload = client(opener, sleeps, max_retries=3).get_json("https://x.test")
    assert payload == {"ok": True}
    assert len(opener.requests) == 3
    assert sleeps == [1, 2]  # exponential backoff between attempts


class CutResponse(FakeResponse):
    """A body cut mid-transfer, as Open Prices did during a verification run."""

    def read(self, *args: Any) -> bytes:
        raise http.client.IncompleteRead(b'{"items": [', 12099)


def test_retries_a_body_cut_mid_transfer() -> None:
    opener = ScriptedOpener([CutResponse(b""), b'{"ok": true}'])
    _, payload = client(opener, [], max_retries=3).get_json("https://x.test")
    assert payload == {"ok": True}
    assert len(opener.requests) == 2


def test_a_source_that_keeps_cutting_bodies_ends_in_http_error() -> None:
    # HttpError is what triggers the public-source fallback instead of failing the run.
    opener = ScriptedOpener([CutResponse(b"") for _ in range(4)])
    with pytest.raises(HttpError, match="after 3 retries"):
        client(opener, [], max_retries=3).get_json("https://x.test")


def test_gives_up_after_the_configured_retries() -> None:
    opener = ScriptedOpener([http_error(429)] * 4)
    with pytest.raises(HttpError, match="after 3 retries"):
        client(opener, [], max_retries=3).get_json("https://x.test")
    assert len(opener.requests) == 4


def test_does_not_retry_client_errors() -> None:
    opener = ScriptedOpener([http_error(404)])
    with pytest.raises(HttpError, match="HTTP 404"):
        client(opener, [], max_retries=3).get_json("https://x.test")
    assert len(opener.requests) == 1


def test_download_resumes_from_the_bytes_already_received(tmp_path: Path) -> None:
    # The first attempt stops after 3 of 6 bytes; the retry asks only for the rest.
    opener = ScriptedOpener([b"abc", PartialResponse(b"def")])
    destination = tmp_path / "dump.csv.gz"
    client(opener, [], max_retries=3).download("https://x.test/d", destination, 6, etag='"v1"')
    assert destination.read_bytes() == b"abcdef"
    assert opener.requests[1].get_header("Range") == "bytes=3-"
    assert opener.requests[1].get_header("If-range") == '"v1"'
    assert not (tmp_path / "dump.csv.gz.part").exists()


def test_download_starts_over_when_the_server_ignores_the_range(tmp_path: Path) -> None:
    # A 200 instead of 206 means the server sent the whole (possibly changed) file.
    opener = ScriptedOpener([b"abc", b"abcdef"])
    destination = tmp_path / "dump.csv.gz"
    client(opener, [], max_retries=3).download("https://x.test/d", destination, 6)
    assert destination.read_bytes() == b"abcdef"


def test_paces_requests_to_the_minimum_interval() -> None:
    # First request at t=0; the second asks at t=0.25, so it waits 0.75 s.
    times = iter([0.0, 0.25, 1.0])
    sleeps: list[float] = []
    paced = HttpClient(
        "agent",
        min_interval_seconds=1.0,
        opener=ScriptedOpener([b"{}", b"{}"]),
        sleep=sleeps.append,
        clock=lambda: next(times),
    )
    paced.get_json("https://x.test/1")
    paced.get_json("https://x.test/2")
    assert sleeps == [0.75]
