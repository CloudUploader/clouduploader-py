"""Low-level HTTP transport with retry, auth, and structured error handling."""

from __future__ import annotations

import logging
import time
from typing import Any, BinaryIO

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from cloud_uploader.exceptions import (
    AuthenticationError,
    CloudUploaderError,
)

logger = logging.getLogger("cloud_uploader.http")

# ── Default configuration ───────────────────────────────────────────────

_DEFAULT_TIMEOUT = 30  # seconds (connect + read)
_DEFAULT_MAX_RETRIES = 3
_DEFAULT_BACKOFF_FACTOR = 0.5  # 0s, 0.5s, 1s, 2s …
_RETRYABLE_STATUS_CODES = frozenset({429, 500, 502, 503, 504})


class HttpClient:
    """Thin wrapper around :class:`requests.Session` tailored for the
    CloudUploader backend.

    Features:
        * ``Authorization: Bearer`` + ``X-Api-Key`` headers on every request.
        * Automatic retry with exponential back-off for transient errors.
        * Structured JSON error extraction from backend responses.
        * Configurable timeouts per-request or globally.

    Args:
        api_key: Secret API key (e.g. ``"ck_live_xxx"``).
        base_url: Root URL of the CloudUploader backend.
        timeout: Default request timeout in seconds.
        max_retries: Maximum retry attempts for transient failures.
        backoff_factor: Multiplier for exponential back-off between retries.
    """

    def __init__(
        self,
        api_key: str,
        base_url: str,
        *,
        timeout: int = _DEFAULT_TIMEOUT,
        max_retries: int = _DEFAULT_MAX_RETRIES,
        backoff_factor: float = _DEFAULT_BACKOFF_FACTOR,
    ) -> None:
        self._api_key = api_key
        self._base_url = base_url.rstrip("/")
        self._timeout = timeout
        self._max_retries = max_retries
        self._backoff_factor = backoff_factor

        self._session = self._build_session()

    # ── Session construction ────────────────────────────────────────────

    def _build_session(self) -> requests.Session:
        session = requests.Session()
        session.headers.update(
            {
                "Authorization": f"Bearer {self._api_key}",
                "X-Api-Key": self._api_key,
                "User-Agent": "cloud-uploader-python/0.1.0",
            }
        )

        # urllib3-level retry for connection errors & certain status codes.
        retry_strategy = Retry(
            total=self._max_retries,
            status_forcelist=list(_RETRYABLE_STATUS_CODES),
            backoff_factor=self._backoff_factor,
            allowed_methods=["GET", "POST"],
            raise_on_status=False,
        )
        adapter = HTTPAdapter(
            max_retries=retry_strategy,
            pool_connections=100,
            pool_maxsize=100,
        )
        session.mount("https://", adapter)
        session.mount("http://", adapter)

        return session

    # ── Public request methods ──────────────────────────────────────────

    def post_json(
        self,
        path: str,
        payload: dict[str, Any],
        *,
        timeout: int | None = None,
    ) -> dict[str, Any]:
        """POST *payload* as JSON to ``base_url + path`` and return the
        parsed JSON response body.

        Raises:
            AuthenticationError: on HTTP 401.
            CloudUploaderError: on any non-2xx response.
        """
        url = f"{self._base_url}{path}"
        logger.debug("POST %s  payload=%s", url, payload)

        resp = self._session.post(
            url, json=payload, timeout=timeout or self._timeout
        )
        return self._handle_json_response(resp)

    def get(
        self,
        path: str,
        *,
        params: dict[str, str] | None = None,
        timeout: int | None = None,
    ) -> dict[str, Any]:
        """GET ``base_url + path`` and return parsed JSON."""
        url = f"{self._base_url}{path}"
        logger.debug("GET %s  params=%s", url, params)

        resp = self._session.get(
            url, params=params, timeout=timeout or self._timeout
        )
        return self._handle_json_response(resp)

    def put_binary(
        self,
        url: str,
        data: bytes | BinaryIO,
        *,
        content_type: str = "application/octet-stream",
        content_length: int | None = None,
        timeout: int | None = None,
        retries: int | None = None,
        backoff: float | None = None,
    ) -> requests.Response:
        """PUT raw binary *data* to an absolute presigned *url*.

        This method does **not** use the session retry adapter because
        presigned URLs are typically on a different host (S3/R2) and we
        need per-part retry control for multipart uploads.

        Returns the raw :class:`requests.Response` so callers can read
        the ``ETag`` header.
        """
        max_attempts = (retries or self._max_retries) + 1
        delay = backoff or self._backoff_factor

        # Prepare headers:
        # 1. Disable "Expect: 100-continue" which R2/S3 sometimes reject with 400.
        # 2. Set Content-Type.
        # 3. Set Content-Length if known to avoid chunked encoding.
        headers = {
            "Content-Type": content_type,
            "Expect": "",
        }
        if content_length is not None:
            headers["Content-Length"] = str(content_length)
        elif isinstance(data, bytes):
            headers["Content-Length"] = str(len(data))

        last_exc: Exception | None = None
        for attempt in range(1, max_attempts + 1):
            try:
                # If data is a bytes object we can simply re-send; if it's
                # a file-like we need to seek back to the start on retry.
                if hasattr(data, "seek"):
                    data.seek(0)

                resp = requests.put(
                    url,
                    data=data,
                    headers=headers,
                    timeout=timeout or max(self._timeout, 120),
                )
                if resp.status_code in _RETRYABLE_STATUS_CODES:
                    raise requests.HTTPError(
                        f"Retryable status {resp.status_code}", response=resp
                    )
                resp.raise_for_status()
                return resp

            except (requests.ConnectionError, requests.Timeout, requests.HTTPError) as exc:
                last_exc = exc
                if attempt < max_attempts:
                    wait = delay * (2 ** (attempt - 1))
                    logger.warning(
                        "PUT %s attempt %d/%d failed (%s), retrying in %.1fs",
                        url,
                        attempt,
                        max_attempts,
                        exc,
                        wait,
                    )
                    time.sleep(wait)

        raise CloudUploaderError(
            f"PUT failed after {max_attempts} attempts: {last_exc}",
            status_code=getattr(getattr(last_exc, "response", None), "status_code", None),
        )

    def download_stream(
        self,
        url: str,
        *,
        timeout: int | None = None,
    ) -> requests.Response:
        """GET *url* with ``stream=True`` for large file downloads."""
        resp = requests.get(
            url,
            stream=True,
            timeout=timeout or max(self._timeout, 300),
        )
        resp.raise_for_status()
        return resp

    # ── Response handling ───────────────────────────────────────────────

    def _handle_json_response(self, resp: requests.Response) -> dict[str, Any]:
        """Parse a JSON response, raising typed exceptions on errors."""
        if resp.status_code == 401:
            body = self._safe_json(resp)
            raise AuthenticationError(
                body.get("message", "Authentication failed"),
                error_code=body.get("error"),
                status_code=401,
            )

        if not resp.ok:
            body = self._safe_json(resp)
            raise CloudUploaderError(
                body.get("message", f"HTTP {resp.status_code}"),
                error_code=body.get("error"),
                status_code=resp.status_code,
            )

        return self._safe_json(resp)

    @staticmethod
    def _safe_json(resp: requests.Response) -> dict[str, Any]:
        try:
            return resp.json()
        except (ValueError, TypeError):
            return {"message": resp.text or "Unknown error"}

    # ── Context manager ─────────────────────────────────────────────────

    def close(self) -> None:
        self._session.close()

    def __enter__(self) -> "HttpClient":
        return self

    def __exit__(self, *_: Any) -> None:
        self.close()
