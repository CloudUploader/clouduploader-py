"""Upload orchestrator — decides direct vs multipart, drives the flow."""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from typing import Any

from cloud_uploader.exceptions import (
    UploadFailedError,
    UploadInitError,
)
from cloud_uploader.http_client import HttpClient
from cloud_uploader.multipart import (
    MultipartUploadEngine,
    ProgressCallback,
)
from cloud_uploader.utils import format_bytes, sanitize_filename, validate_file

logger = logging.getLogger("cloud_uploader.uploader")


@dataclass(frozen=True)
class UploadResult:
    """Immutable result returned after a successful upload.

    Attributes:
        upload_id: Backend upload-session identifier.
        key: Object key in the storage backend.
        storage: Storage backend name (``r2``, ``s3``, ``minio``, …).
        storage_path: Full ``provider://bucket/key`` path (if provided).
        mode: ``"direct"`` or ``"multipart"``.
    """

    upload_id: str
    key: str
    storage: str
    storage_path: str = ""
    mode: str = "direct"


@dataclass(frozen=True)
class FolderUploadFailure:
    """Represents a failed file upload within a folder upload operation."""
    file_path: str
    error: str


@dataclass(frozen=True)
class FolderUploadResult:
    """Result of a folder upload operation."""
    source_folder: str
    results: list[UploadResult]
    failures: list[FolderUploadFailure]
    total_files: int
    succeeded: int
    failed: int


class UploadOrchestrator:
    """Stateless orchestrator that handles the full upload lifecycle.

    1. ``POST /api/upload/iaas/create`` to initialize the session.
    2. Direct PUT **or** parallel multipart upload via presigned URLs.
    3. ``POST /api/upload/iaas/complete`` to finalize.

    Args:
        http: Configured :class:`HttpClient`.
        max_parallel_uploads: Thread-pool size for multipart.
        chunk_size_override: Force a specific chunk size (bytes).
                             ``None`` defers to the backend's ``chunk_size``.
        storage: Default storage backend (``r2``, ``s3``, ``minio``,
                 ``azure``, ``gcs``).
        network_mbps: Optional hint for the server's chunk-size tuning
                      algorithm.  Pass your measured or estimated download
                      speed; ``None`` lets the server use its default.
    """

    def __init__(
        self,
        http: HttpClient,
        *,
        max_parallel_uploads: int = 5,
        chunk_size_override: int | None = None,
        storage: str = "r2",
        network_mbps: float | None = None,
    ) -> None:
        self._http = http
        self._max_workers = max_parallel_uploads
        self._chunk_override = chunk_size_override
        self._storage = storage
        self._network_mbps = network_mbps

    # ── Public API ──────────────────────────────────────────────────────

    def upload(
        self,
        file_path: str,
        *,
        progress_callback: ProgressCallback | None = None,
        storage: str | None = None,
    ) -> UploadResult:
        """Upload *file_path* end-to-end and return an :class:`UploadResult`.

        Args:
            file_path: Path to the local file.
            progress_callback: Optional ``(uploaded_bytes, total_bytes)`` hook.
            storage: Override the default storage backend for this upload.

        Returns:
            An :class:`UploadResult` with the backend-assigned identifiers.

        Raises:
            FileNotFoundError_: if the file does not exist.
            UploadInitError: if the backend rejects the create call.
            UploadFailedError: if part uploads or the complete call fail.
        """
        abs_path, file_size = validate_file(file_path)
        filename = sanitize_filename(abs_path)
        chosen_storage = storage or self._storage

        logger.info(
            "Starting upload: file=%s  size=%s  storage=%s",
            filename,
            format_bytes(file_size),
            chosen_storage,
        )

        # ── Step 1: Initialize upload session ───────────────────────────
        init_resp = self._init_upload(filename, file_size, chosen_storage)
        upload_id: str = init_resp["upload_id"]
        key: str = init_resp["key"]
        mode: str = init_resp.get("mode", "direct")

        logger.info(
            "Session created: upload_id=%s  mode=%s  key=%s",
            upload_id,
            mode,
            key,
        )

        # ── Step 2: Upload data ─────────────────────────────────────────
        try:
            if mode == "multipart":
                result = self._do_multipart(
                    abs_path,
                    file_size,
                    init_resp,
                    progress_callback=progress_callback,
                )
            else:
                self._do_direct(
                    abs_path,
                    file_size,
                    init_resp,
                    progress_callback=progress_callback,
                )
                result = None
        except Exception:
            logger.error("Upload failed for upload_id=%s, aborting", upload_id)
            self._safe_abort(upload_id)
            raise

        # ── Step 3: Complete ────────────────────────────────────────────
        complete_resp = self._complete_upload(upload_id, mode, result)

        storage_path = complete_resp.get("storagePath", "")
        logger.info("Upload complete: storagePath=%s", storage_path)

        return UploadResult(
            upload_id=upload_id,
            key=key,
            storage=complete_resp.get("storage", chosen_storage),
            storage_path=storage_path,
            mode=mode,
        )

    def status(self, upload_id: str) -> dict[str, Any]:
        """Query the status of an upload session."""
        return self._http.get(f"/api/upload/iaas/status/{upload_id}")

    def abort(self, upload_id: str) -> dict[str, Any]:
        """Abort an in-progress upload session."""
        return self._http.post_json(
            "/api/upload/iaas/abort",
            {"upload_id": upload_id},
        )

    def retry(self, upload_id: str, failed_parts: list[int]) -> dict[str, Any]:
        """Request fresh presigned URLs for specific failed parts.

        The server re-issues presigned URLs for *failed_parts* only, reusing
        the existing multipart upload ID — no data already uploaded is lost.
        Use the returned ``retry_urls`` dict (``{ "3": "<url>", … }``) with
        :meth:`MultipartUploadEngine.execute_retry`.
        """
        return self._http.post_json(
            "/api/upload/iaas/retry",
            {"upload_id": upload_id, "failed_parts": failed_parts},
        )

    # ── Internal helpers ────────────────────────────────────────────────

    def _init_upload(
        self, filename: str, file_size: int, storage: str
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "filename": filename,
            "size": file_size,
            "storage": storage,
            "cpu_threads": os.cpu_count() or 1,
        }
        if self._network_mbps is not None:
            payload["network_mbps"] = self._network_mbps

        try:
            resp = self._http.post_json("/api/upload/iaas/create", payload)
        except Exception as exc:
            raise UploadInitError(
                f"Failed to initialize upload: {exc}",
                error_code=getattr(exc, "error_code", None),
                status_code=getattr(exc, "status_code", None),
            ) from exc

        if not resp.get("success"):
            raise UploadInitError(
                resp.get("message", "Upload init rejected"),
                error_code=resp.get("error"),
            )
        return resp

    def _do_direct(
        self,
        file_path: str,
        file_size: int,
        init_resp: dict[str, Any],
        *,
        progress_callback: ProgressCallback | None = None,
    ) -> None:
        """Single PUT to ``presigned_url``."""
        url = init_resp["presigned_url"]
        logger.debug("Direct upload to presigned URL (%s)", url[:80])

        with open(file_path, "rb") as f:
            self._http.put_binary(url, data=f, content_length=file_size)

        if progress_callback:
            try:
                progress_callback(file_size, file_size)
            except Exception:
                pass

    def _do_multipart(
        self,
        file_path: str,
        file_size: int,
        init_resp: dict[str, Any],
        *,
        progress_callback: ProgressCallback | None = None,
    ) -> list[dict[str, object]]:
        """Parallel multipart upload with automatic retry for failed parts."""
        upload_id: str = init_resp["upload_id"]
        chunk_size: int = self._chunk_override or int(init_resp["chunk_size"])
        presigned_urls: list[str] = init_resp["presigned_urls"]
        max_workers: int = min(
            self._max_workers,
            int(init_resp.get("part_parallelism", self._max_workers)),
        )

        engine = MultipartUploadEngine(
            http=self._http,
            file_path=file_path,
            file_size=file_size,
            chunk_size=chunk_size,
            presigned_urls=presigned_urls,
            max_workers=max_workers,
            progress_callback=progress_callback,
        )
        result = engine.execute()

        # Retry failed parts once via /retry (gets fresh presigned URLs, no new multipart init)
        if not result.ok:
            logger.warning(
                "%d part(s) failed on first attempt — calling /retry for upload_id=%s",
                len(result.failed_parts),
                upload_id,
            )
            try:
                retry_resp = self.retry(upload_id, result.failed_parts)
            except Exception as exc:
                raise UploadFailedError(
                    f"{len(result.failed_parts)} parts failed and /retry call failed: {exc}",
                    upload_id=upload_id,
                    failed_parts=result.failed_parts,
                ) from exc

            if not retry_resp.get("success"):
                raise UploadFailedError(
                    f"Retry init rejected: {retry_resp.get('message', retry_resp.get('error'))}",
                    upload_id=upload_id,
                    error_code=retry_resp.get("error"),
                    failed_parts=result.failed_parts,
                )

            retry_result = engine.execute_retry(retry_resp["retry_urls"])
            if not retry_result.ok:
                raise UploadFailedError(
                    f"{len(retry_result.failed_parts)} part(s) still failed after retry: "
                    f"{retry_result.failed_parts}",
                    upload_id=upload_id,
                    failed_parts=retry_result.failed_parts,
                )

            # Merge: keep first-pass successes, replace with retried results
            retried_nums = {p.part_number for p in retry_result.parts}
            merged_parts = [
                p for p in result.parts if p.part_number not in retried_nums
            ] + retry_result.parts
            result.parts.clear()
            result.parts.extend(sorted(merged_parts, key=lambda p: p.part_number))
            result.failed_parts.clear()

        return result.to_complete_payload()

    def _complete_upload(
        self,
        upload_id: str,
        mode: str,
        parts: list[dict[str, object]] | None,
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {"upload_id": upload_id}
        if mode == "multipart" and parts:
            payload["parts"] = parts

        try:
            resp = self._http.post_json("/api/upload/iaas/complete", payload)
        except Exception as exc:
            raise UploadFailedError(
                f"Complete call failed: {exc}",
                upload_id=upload_id,
                error_code=getattr(exc, "error_code", None),
                status_code=getattr(exc, "status_code", None),
            ) from exc

        if not resp.get("success"):
            raise UploadFailedError(
                resp.get("message", "Complete call rejected"),
                upload_id=upload_id,
                error_code=resp.get("error"),
            )
        return resp

    def _safe_abort(self, upload_id: str) -> None:
        try:
            self.abort(upload_id)
        except Exception:
            logger.debug("Abort for %s failed; ignoring", upload_id, exc_info=True)
