#!/usr/bin/env python3
"""CloudUploader SDK — basic usage examples.

Run with:
    python examples/basic_upload.py
"""

import sys
import os

# ── Allow running from the repo root without installing ─────────────
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from cloud_uploader import CloudUploader, CloudUploaderError


# ── Configuration ───────────────────────────────────────────────────

API_KEY = os.environ.get("CLOUD_UPLOADER_API_KEY", "ck_live_xxx")
BASE_URL = os.environ.get("CLOUD_UPLOADER_URL", "http://localhost:8080")


# ── 1. Simple upload ───────────────────────────────────────────────

def simple_upload(file_path: str) -> None:
    """Upload a file with the simplest possible API."""
    uploader = CloudUploader(api_key=API_KEY, base_url=BASE_URL)
    result = uploader.upload_file(file_path)

    print(f"✅ Upload complete!")
    print(f"   upload_id   : {result.upload_id}")
    print(f"   mode        : {result.mode}")
    print(f"   key         : {result.key}")
    print(f"   storage_path: {result.storage_path}")


# ── 2. Upload with progress bar ────────────────────────────────────

def upload_with_progress(file_path: str) -> None:
    """Upload a file with a real-time progress bar."""

    def progress(uploaded: int, total: int) -> None:
        pct = (uploaded / total * 100) if total else 0
        bar_len = 40
        filled = int(bar_len * uploaded / total) if total else 0
        bar = "█" * filled + "░" * (bar_len - filled)
        print(f"\r  [{bar}] {pct:5.1f}%  ({uploaded:,}/{total:,} bytes)", end="", flush=True)

    uploader = CloudUploader(
        api_key=API_KEY,
        base_url=BASE_URL,
        debug=True,             # enable detailed logging
    )

    result = uploader.upload_file(file_path, progress_callback=progress)
    print()  # newline after progress bar
    print(f"✅ Done → {result.storage_path}")


# ── 3. Upload to a specific storage backend ────────────────────────

def upload_to_s3(file_path: str) -> None:
    """Upload explicitly to S3 instead of the default R2."""
    uploader = CloudUploader(api_key=API_KEY, base_url=BASE_URL)
    result = uploader.upload_file(file_path, storage="s3")
    print(f"✅ Uploaded to S3: {result.storage_path}")


# ── 4. Error handling ──────────────────────────────────────────────

def safe_upload(file_path: str) -> None:
    """Upload with comprehensive error handling."""
    try:
        with CloudUploader(api_key=API_KEY, base_url=BASE_URL) as uploader:
            result = uploader.upload_file(file_path)
            print(f"✅ {result.storage_path}")
    except CloudUploaderError as e:
        print(f"❌ Upload failed: {e.message}")
        if e.error_code:
            print(f"   Error code : {e.error_code}")
        if e.status_code:
            print(f"   HTTP status: {e.status_code}")
        sys.exit(1)


# ── 5. Check upload status ─────────────────────────────────────────

def check_status(upload_id: str) -> None:
    """Query the status of a previous upload."""
    uploader = CloudUploader(api_key=API_KEY, base_url=BASE_URL)
    status = uploader.get_upload_status(upload_id)
    print(f"Upload {upload_id}:")
    for k, v in status.items():
        print(f"  {k}: {v}")


# ── Main ────────────────────────────────────────────────────────────

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python basic_upload.py <file_path> [--progress]")
        sys.exit(1)

    path = sys.argv[1]
    use_progress = "--progress" in sys.argv

    if use_progress:
        upload_with_progress(path)
    else:
        safe_upload(path)
