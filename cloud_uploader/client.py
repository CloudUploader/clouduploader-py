"""Public-facing ``CloudUploader`` client — the main entry point for SDK users."""

from __future__ import annotations

import logging
import sys
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Callable

from cloud_uploader.exceptions import CloudUploaderError, DownloadError, FileNotFoundError_
from cloud_uploader.http_client import HttpClient
from cloud_uploader.multipart import ProgressCallback
from cloud_uploader.uploader import (
    UploadOrchestrator,
    UploadResult,
    FolderUploadResult,
    FolderUploadFailure,
)

logger = logging.getLogger("cloud_uploader")

_DEFAULT_BASE_URL = "http://localhost:8080"


class CloudUploader:
    """High-level client for the CloudUploader file upload platform.

    Usage::

        uploader = CloudUploader(api_key="ck_live_xxx")
        result = uploader.upload_file("video.mp4")
        print(result.storage_path)

    Args:
        api_key: Required secret API key.
        base_url: Root URL of the CloudUploader backend.
                  Defaults to ``http://localhost:8080``.
        timeout: HTTP timeout in seconds (default 30).
        max_retries: Retry attempts for transient failures (default 3).
        backoff_factor: Exponential back-off multiplier (default 0.5).
        max_parallel_uploads: Thread-pool size for multipart uploads
                              (default 5).
        chunk_size_override: Force a specific chunk size in bytes.
                             ``None`` defers to the backend's recommendation.
        storage: Default storage backend — ``r2``, ``s3``, ``minio``,
                 ``azure``, or ``gcs`` (default ``r2``).
        debug: Enable debug logging to stderr.
    """

    def __init__(
        self,
        api_key: str,
        *,
        base_url: str = _DEFAULT_BASE_URL,
        timeout: int = 30,
        max_retries: int = 3,
        backoff_factor: float = 0.5,
        max_parallel_uploads: int = 5,
        chunk_size_override: int | None = None,
        storage: str = "r2",
        debug: bool = False,
    ) -> None:
        if not api_key:
            raise ValueError("api_key is required")

        if debug:
            self._configure_debug_logging()

        self._http = HttpClient(
            api_key=api_key,
            base_url=base_url,
            timeout=timeout,
            max_retries=max_retries,
            backoff_factor=backoff_factor,
        )
        self._orchestrator = UploadOrchestrator(
            self._http,
            max_parallel_uploads=max_parallel_uploads,
            chunk_size_override=chunk_size_override,
            storage=storage,
        )
        self._storage = storage

    # ── Upload ──────────────────────────────────────────────────────────

    def upload_file(
        self,
        file_path: str,
        *,
        progress_callback: ProgressCallback | None = None,
        storage: str | None = None,
    ) -> UploadResult:
        """Upload a local file to the cloud.

        Args:
            file_path: Path to the file to upload.
            progress_callback: Optional callback invoked as bytes are uploaded.
                Signature: ``(uploaded_bytes: int, total_bytes: int) -> None``
            storage: Override default storage backend for this upload.

        Returns:
            An :class:`UploadResult` with ``upload_id``, ``key``,
            ``storage``, ``storage_path``, and ``mode``.
        """
        return self._orchestrator.upload(
            file_path,
            progress_callback=progress_callback,
            storage=storage,
        )

    def upload_folder(
        self,
        folder_path: str,
        *,
        file_filter: str = "*",
        skip_hidden: bool = True,
        storage: str | None = None,
    ) -> FolderUploadResult:
        """Recursively upload all files in a folder.

        Args:
            folder_path: Path to the local directory.
            file_filter: Glob pattern to filter files (e.g., ``"*.jpg"``). Defaults to ``"*"``.
            skip_hidden: If True, skips files and directories starting with ``.``.
            storage: Override default storage backend for these uploads.

        Returns:
            A :class:`FolderUploadResult` summarizing the operation.

        Raises:
            FileNotFoundError_: if the folder does not exist or is not a directory.
        """
        p = Path(folder_path).resolve()
        if not p.exists():
            raise FileNotFoundError_(f"Folder not found: {folder_path}")
        if not p.is_dir():
            raise FileNotFoundError_(f"Path is not a directory: {folder_path}")

        logger.info("Starting folder upload: folder=%s", p)

        lock = threading.Lock()
        results: list[UploadResult] = []
        failures: list[FolderUploadFailure] = []
        
        files_to_upload: list[str] = []
        for file_path in p.rglob(file_filter):
            if not file_path.is_file():
                continue
            if skip_hidden and any(part.startswith(".") for part in file_path.relative_to(p).parts):
                continue
            files_to_upload.append(str(file_path))
            
        total = len(files_to_upload)
        counts = {"succeeded": 0, "failed": 0}
        
        def _upload_single(fname: str) -> None:
            try:
                res = self.upload_file(fname, storage=storage)
                with lock:
                    results.append(res)
                    counts["succeeded"] += 1
            except Exception as exc:
                logger.error("Failed to upload %s: %s", fname, exc)
                with lock:
                    failures.append(FolderUploadFailure(file_path=fname, error=str(exc)))
                    counts["failed"] += 1

        # Use the existing configured max_parallel_uploads logic to size folder parallelism too.
        # This prevents aggressive over-subscription if there are many large multipart files.
        max_workers = self._orchestrator._max_workers
        
        logger.info("Scheduling %d files in folder via %d threads", total, max_workers)
        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            futures = [pool.submit(_upload_single, fname) for fname in files_to_upload]
            for future in as_completed(futures):
                pass


        logger.info(
            "Folder upload complete: total=%d succeeded=%d failed=%d",
            total, counts["succeeded"], counts["failed"]
        )

        return FolderUploadResult(
            source_folder=str(p),
            results=results,
            failures=failures,
            total_files=total,
            succeeded=counts["succeeded"],
            failed=counts["failed"],
        )

    # ── Download ────────────────────────────────────────────────────────

    def download_file(
        self,
        file_id: str,
        output_path: str,
        *,
        progress_callback: Callable[[int, int], None] | None = None,
    ) -> str:
        """Download a file by its *file_id* and write it to *output_path*.

        Args:
            file_id: The backend file identifier.
            output_path: Local destination path.
            progress_callback: Optional ``(downloaded_bytes, total_bytes)``
                hook.  ``total_bytes`` may be ``0`` if the server does
                not send a ``Content-Length`` header.

        Returns:
            The absolute path of the downloaded file.
        """
        try:
            meta = self._http.get(
                "/file/download",
                params={"fileId": file_id},
            )
            download_url = meta.get("url") or meta.get("presigned_url", "")
            if not download_url:
                raise DownloadError(
                    f"No download URL returned for fileId={file_id}"
                )
        except CloudUploaderError:
            raise
        except Exception as exc:
            raise DownloadError(f"Failed to get download URL: {exc}") from exc

        try:
            resp = self._http.download_stream(download_url)
            total = int(resp.headers.get("Content-Length", 0))
            downloaded = 0

            out = Path(output_path)
            out.parent.mkdir(parents=True, exist_ok=True)

            with open(out, "wb") as f:
                for chunk in resp.iter_content(chunk_size=8192):
                    f.write(chunk)
                    downloaded += len(chunk)
                    if progress_callback:
                        try:
                            progress_callback(downloaded, total)
                        except Exception:
                            pass

            return str(out.resolve())

        except CloudUploaderError:
            raise
        except Exception as exc:
            raise DownloadError(f"Download failed: {exc}") from exc

    # ── Status / abort ──────────────────────────────────────────────────

    def get_upload_status(self, upload_id: str) -> dict[str, Any]:
        """Return the current status of the upload session."""
        return self._orchestrator.status(upload_id)

    def abort_upload(self, upload_id: str) -> dict[str, Any]:
        """Abort an in-progress upload."""
        return self._orchestrator.abort(upload_id)

    # ── Utilities ───────────────────────────────────────────────────────

    @staticmethod
    def _configure_debug_logging() -> None:
        handler = logging.StreamHandler(sys.stderr)
        handler.setLevel(logging.DEBUG)
        handler.setFormatter(
            logging.Formatter(
                "%(asctime)s  %(name)s  %(levelname)s  %(message)s",
                datefmt="%H:%M:%S",
            )
        )
        root = logging.getLogger("cloud_uploader")
        root.setLevel(logging.DEBUG)
        if not root.handlers:
            root.addHandler(handler)

    # ── Context manager / cleanup ───────────────────────────────────────

    def close(self) -> None:
        """Close the underlying HTTP session."""
        self._http.close()

    def __enter__(self) -> "CloudUploader":
        return self

    def __exit__(self, *_: Any) -> None:
        self.close()

    def __repr__(self) -> str:
        return (
            f"CloudUploader(base_url={self._http._base_url!r}, "
            f"storage={self._storage!r})"
        )
