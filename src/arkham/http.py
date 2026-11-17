"""Small HTTP client on the standard library: custom User-Agent, pacing, retries and
validation of JSON bodies (a truncated body is retried, never stored)."""

from __future__ import annotations

import http.client
import json
import shutil
import time
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Any

RETRYABLE_STATUS = {429, 500, 502, 503, 504}


class HttpError(RuntimeError):
    """A request failed after every retry, or with a status that is not retried."""


class HttpClient:
    def __init__(
        self,
        user_agent: str,
        timeout_seconds: float = 60,
        max_retries: int = 3,
        min_interval_seconds: float = 0.0,
        opener: Callable[..., Any] = urllib.request.urlopen,
        sleep: Callable[[float], None] = time.sleep,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self.user_agent = user_agent
        self.timeout_seconds = timeout_seconds
        self.max_retries = max_retries
        self.min_interval_seconds = min_interval_seconds
        self._opener = opener
        self._sleep = sleep
        self._clock = clock
        self._last_request_at: float | None = None
        self.requests_sent = 0

    @staticmethod
    def build_url(url: str, params: Mapping[str, object] | None = None) -> str:
        if not params:
            return url
        return f"{url}?{urllib.parse.urlencode({k: str(v) for k, v in params.items()})}"

    def _pace(self) -> None:
        if self._last_request_at is not None and self.min_interval_seconds > 0:
            wait = self.min_interval_seconds - (self._clock() - self._last_request_at)
            if wait > 0:
                self._sleep(wait)
        self._last_request_at = self._clock()

    def _with_retries(self, url: str, attempt_once: Callable[[], Any]) -> Any:
        last_error: Exception | None = None
        for attempt in range(self.max_retries + 1):
            if attempt:
                self._sleep(2 ** (attempt - 1))
            self._pace()
            self.requests_sent += 1
            try:
                return attempt_once()
            except urllib.error.HTTPError as error:
                if error.code not in RETRYABLE_STATUS:
                    raise HttpError(f"GET {url} returned HTTP {error.code}") from error
                last_error = error
            except (OSError, http.client.HTTPException, ValueError) as error:
                # Transport failures are retried: OSError covers URLError, timeouts and
                # connection resets; HTTPException covers a body cut mid-transfer
                # (IncompleteRead), seen from Open Prices; ValueError covers a body that
                # is not valid JSON.
                last_error = error
        raise HttpError(
            f"GET {url} failed after {self.max_retries} retries: {last_error}"
        ) from last_error

    def _request(self, url: str) -> urllib.request.Request:
        return urllib.request.Request(url, headers={"User-Agent": self.user_agent})

    def get_json(self, url: str, params: Mapping[str, object] | None = None) -> tuple[bytes, Any]:
        """Returns the body exactly as received and its parsed JSON."""
        full_url = self.build_url(url, params)

        def attempt() -> tuple[bytes, Any]:
            with self._opener(self._request(full_url), timeout=self.timeout_seconds) as response:
                body = response.read()
            return body, json.loads(body)

        return self._with_retries(full_url, attempt)

    def head(self, url: str) -> Mapping[str, str]:
        def attempt() -> Mapping[str, str]:
            request = urllib.request.Request(
                url, method="HEAD", headers={"User-Agent": self.user_agent}
            )
            with self._opener(request, timeout=self.timeout_seconds) as response:
                return dict(response.headers.items())

        return self._with_retries(url, attempt)

    def download(
        self,
        url: str,
        destination: Path,
        expected_size: int | None = None,
        etag: str | None = None,
    ) -> None:
        """Streams a large file to `destination`, through a temporary file renamed at the end.

        A retry resumes from the bytes already received (HTTP Range). With the file's ETag,
        If-Range makes the server send the whole file again if it changed in between, so
        two versions are never stitched together.
        """
        partial = destination.with_name(destination.name + ".part")
        destination.parent.mkdir(parents=True, exist_ok=True)
        partial.unlink(missing_ok=True)

        def attempt() -> None:
            offset = partial.stat().st_size if partial.exists() else 0
            headers = {"User-Agent": self.user_agent}
            if offset:
                headers["Range"] = f"bytes={offset}-"
                if etag:
                    headers["If-Range"] = etag
            request = urllib.request.Request(url, headers=headers)
            with self._opener(request, timeout=self.timeout_seconds) as response:
                resumed = offset > 0 and getattr(response, "status", 200) == 206
                with partial.open("ab" if resumed else "wb") as handle:
                    shutil.copyfileobj(response, handle, length=8 * 1024 * 1024)
            size = partial.stat().st_size
            if expected_size is not None and size != expected_size:
                raise ConnectionError(f"incomplete download: {size} of {expected_size} bytes")

        try:
            self._with_retries(url, attempt)
        except HttpError:
            partial.unlink(missing_ok=True)
            raise
        partial.replace(destination)
