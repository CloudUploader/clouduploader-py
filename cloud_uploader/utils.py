"""Utility helpers shared across the SDK."""

from __future__ import annotations

import os
from pathlib import Path

# ── MIME type map (mirrors backend's guessContentType) ──────────────────

_MIME_MAP: dict[str, str] = {
    ".jpg": "image/jpeg",
    ".jpeg": "image/jpeg",
    ".png": "image/png",
    ".gif": "image/gif",
    ".webp": "image/webp",
    ".svg": "image/svg+xml",
    ".pdf": "application/pdf",
    ".json": "application/json",
    ".xml": "application/xml",
    ".html": "text/html",
    ".css": "text/css",
    ".js": "application/javascript",
    ".txt": "text/plain",
    ".csv": "text/csv",
    ".zip": "application/zip",
    ".gz": "application/gzip",
    ".mp4": "video/mp4",
    ".mp3": "audio/mpeg",
    ".wav": "audio/wav",
    ".wasm": "application/wasm",
}


def guess_content_type(filename: str) -> str:
    """Return the MIME type for *filename* based on its extension.

    Falls back to ``application/octet-stream`` for unknown extensions.
    """
    ext = Path(filename).suffix.lower()
    return _MIME_MAP.get(ext, "application/octet-stream")


def sanitize_filename(path: str) -> str:
    """Extract the basename from *path*, stripping directory components."""
    return os.path.basename(path)


def format_bytes(n: int) -> str:
    """Return a human-readable representation of *n* bytes.

    >>> format_bytes(1536)
    '1.50 KB'
    >>> format_bytes(10485760)
    '10.00 MB'
    """
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if abs(n) < 1024.0:
            return f"{n:.2f} {unit}"
        n /= 1024.0  # type: ignore[assignment]
    return f"{n:.2f} PB"


def validate_file(file_path: str) -> tuple[str, int]:
    """Validate that *file_path* exists and return ``(abs_path, size)``.

    Raises:
        cloud_uploader.exceptions.FileNotFoundError_: if the path does not
            exist or is not a regular file.
    """
    from cloud_uploader.exceptions import FileNotFoundError_

    p = Path(file_path).resolve()
    if not p.exists():
        raise FileNotFoundError_(f"File not found: {file_path}")
    if not p.is_file():
        raise FileNotFoundError_(f"Path is not a regular file: {file_path}")
    return str(p), p.stat().st_size
