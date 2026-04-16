"""Parallel multipart upload engine.

Splits a file into server-defined chunks, uploads each chunk to a
presigned URL in parallel, and collects part numbers + ETags for the
``/complete`` call.
"""

from __future__ import annotations

import io
import logging
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from typing import Callable

from cloud_uploader.exceptions import UploadFailedError
from cloud_uploader.http_client import HttpClient

logger = logging.getLogger("cloud_uploader.multipart")

# Type alias for the progress callback:
#   progress_callback(uploaded_bytes: int, total_bytes: int)
ProgressCallback = Callable[[int, int], None]


@dataclass
class PartResult:
    """Outcome of uploading a single part."""

    part_number: int
    etag: str
    size: int


@dataclass
class MultipartUploadResult:
    """Aggregate result of a multipart upload session."""

    parts: list[PartResult] = field(default_factory=list)
    failed_parts: list[int] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return len(self.failed_parts) == 0

    def to_complete_payload(self) -> list[dict[str, object]]:
        """Return the ``parts`` list for ``/api/upload/iaas/complete``."""
        return [
            {"part_number": p.part_number, "etag": p.etag}
            for p in sorted(self.parts, key=lambda p: p.part_number)
        ]


class MultipartUploadEngine:
    """Upload a file in parallel chunks to presigned part URLs.

    Args:
        http: Shared :class:`HttpClient` (only the ``put_binary`` method
              is used — presigned URLs don't need auth headers).
        file_path: Absolute path to the local file.
        file_size: Total file size in bytes.
        chunk_size: Number of bytes per part (from backend response).
        presigned_urls: Ordered list of presigned PUT URLs, one per part
                        (index 0 = part 1).
        max_workers: Thread-pool size for parallel uploads.
        progress_callback: Optional ``(uploaded_bytes, total_bytes)`` hook.
        content_type: MIME type for the ``Content-Type`` header on PUTs.
    """

    def __init__(
        self,
        http: HttpClient,
        file_path: str,
        file_size: int,
        chunk_size: int,
        presigned_urls: list[str],
        *,
        max_workers: int = 5,
        progress_callback: ProgressCallback | None = None,
        content_type: str = "application/octet-stream",
    ) -> None:
        self._http = http
        self._file_path = file_path
        self._file_size = file_size
        self._chunk_size = chunk_size
        self._presigned_urls = presigned_urls
        self._max_workers = max_workers
        self._progress_callback = progress_callback
        self._content_type = content_type

        # thread-safe progress tracking
        self._uploaded_bytes = 0
        self._lock = threading.Lock()

    # ── Public API ──────────────────────────────────────────────────────

    def execute(self) -> MultipartUploadResult:
        """Run the parallel upload and return aggregated results."""
        num_parts = len(self._presigned_urls)
        result = MultipartUploadResult()

        logger.info(
            "Starting multipart upload: %d parts, chunk=%d, workers=%d",
            num_parts,
            self._chunk_size,
            self._max_workers,
        )

        with ThreadPoolExecutor(max_workers=self._max_workers) as pool:
            futures = {
                pool.submit(self._upload_part, part_num): part_num
                for part_num in range(1, num_parts + 1)
            }

            for future in as_completed(futures):
                part_num = futures[future]
                try:
                    part_result = future.result()
                    result.parts.append(part_result)
                    logger.debug(
                        "Part %d/%d uploaded (etag=%s)",
                        part_num,
                        num_parts,
                        part_result.etag,
                    )
                except Exception:
                    logger.exception("Part %d/%d failed", part_num, num_parts)
                    result.failed_parts.append(part_num)

        if not result.ok:
            raise UploadFailedError(
                f"{len(result.failed_parts)} of {num_parts} parts failed: "
                f"{result.failed_parts}",
                failed_parts=result.failed_parts,
            )

        return result

    # ── Internal ────────────────────────────────────────────────────────

    def _upload_part(self, part_number: int) -> PartResult:
        """Read a chunk from disk and PUT it to the presigned URL."""
        offset = (part_number - 1) * self._chunk_size
        end = min(offset + self._chunk_size, self._file_size)
        length = end - offset

        # Read the chunk into memory (bounded by chunk_size, typically 5–25 MB).
        with open(self._file_path, "rb") as f:
            f.seek(offset)
            chunk_data = f.read(length)

        url = self._presigned_urls[part_number - 1]
        resp = self._http.put_binary(
            url,
            data=io.BytesIO(chunk_data),
            content_type=self._content_type,
            content_length=length,
        )


        # S3/R2 return the ETag in the response header.
        etag = resp.headers.get("ETag", "").strip('"')

        # Update aggregate progress.
        self._report_progress(length)

        return PartResult(part_number=part_number, etag=etag, size=length)

    def _report_progress(self, bytes_uploaded: int) -> None:
        if self._progress_callback is None:
            return
        with self._lock:
            self._uploaded_bytes += bytes_uploaded
            current = self._uploaded_bytes
        try:
            self._progress_callback(current, self._file_size)
        except Exception:
            logger.debug("Progress callback raised; ignoring", exc_info=True)
