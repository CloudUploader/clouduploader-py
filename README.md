# CloudUploader Python SDK

A production-ready Python SDK for the **CloudUploader** file upload platform. Upload files to S3, Cloudflare R2, MinIO, Azure Blob, or GCS using presigned URLs — with parallel multipart uploads, automatic retries, and real-time progress tracking.

## Quick Start

```bash
pip install clouduploader-py
```

```python
from cloud_uploader import CloudUploader

uploader = CloudUploader(api_key="ck_live_xxx")
result = uploader.upload_file("video.mp4")
print(result.storage_path)
# → r2://my-bucket/ab/cd/1713080000000-a1b2c3-video.mp4
```

## Installation (from source)

```bash
cd sdk/pythonSDK
pip install -e .

# With dev dependencies (for running tests):
pip install -e ".[dev]"
```

## Features

| Feature | Details |
|---|---|
| **Simple API** | Two lines to upload any file |
| **Multipart uploads** | Automatic chunking for large files |
| **Parallel uploads** | Configurable thread pool (default 5 threads) |
| **Retry with backoff** | Exponential backoff for transient failures |
| **Progress tracking** | Real-time callback with bytes uploaded/total |
| **Multiple backends** | `r2`, `s3`, `minio`, `azure`, `gcs` |
| **Download** | Download files by ID via presigned URLs |
| **Type hints** | Full type annotations, Python 3.9+ |

## Configuration

```python
uploader = CloudUploader(
    api_key="ck_live_xxx",          # Required
    base_url="https://api.myapp.com",  # Default: http://localhost:8080
    timeout=30,                      # HTTP timeout (seconds)
    max_retries=3,                   # Retry attempts for transient errors
    max_parallel_uploads=5,          # Thread pool size for multipart
    chunk_size_override=None,        # Override backend chunk size (bytes)
    storage="r2",                    # Default storage backend
    debug=False,                     # Enable debug logging
)
```

## Upload with Progress

```python
def progress(uploaded: int, total: int) -> None:
    pct = uploaded / total * 100
    print(f"\r{pct:.1f}%", end="", flush=True)

result = uploader.upload_file("large_video.mp4", progress_callback=progress)
```

## Upload to Specific Backend

```python
result = uploader.upload_file("data.csv", storage="s3")
```

## Upload a Folder

```python
# Recursively upload all files in a directory (skips hidden files by default)
result = uploader.upload_folder("./path/to/assets")

print(f"Succeeded: {result.succeeded}/{result.total_files}")
if result.failures:
    print(f"Failed files: {len(result.failures)}")

# You can also use a glob pattern to filter specific files
result = uploader.upload_folder("./path/to/assets", file_filter="*.png")
```

## Download a File

```python
path = uploader.download_file(file_id="file_123", output_path="./downloads/file.jpg")
```

## Error Handling

```python
from cloud_uploader import (
    CloudUploaderError,
    AuthenticationError,
    UploadInitError,
    UploadFailedError,
)

try:
    result = uploader.upload_file("file.pdf")
except AuthenticationError:
    print("Invalid API key")
except UploadInitError as e:
    print(f"Backend rejected upload: {e.error_code}")
except UploadFailedError as e:
    print(f"Failed parts: {e.failed_parts}")
except CloudUploaderError as e:
    print(f"Error: {e.message} (HTTP {e.status_code})")
```

## Check Upload Status

```python
status = uploader.get_upload_status("up_abc123")
print(status)
# {'success': True, 'upload_id': '...', 'status': 'completed', ...}
```

## Abort an Upload

```python
uploader.abort_upload("up_abc123")
```

## Architecture

```
cloud_uploader/
├── __init__.py        # Public API re-exports
├── client.py          # CloudUploader — main user-facing class
├── uploader.py        # UploadOrchestrator — direct vs multipart routing
├── multipart.py       # Parallel multipart engine (ThreadPoolExecutor)
├── http_client.py     # HTTP transport with retry + auth
├── utils.py           # MIME types, file validation, formatting
└── exceptions.py      # Exception hierarchy
```
## To test with your running backend:
cd sdk/pythonSDK
source .venv/bin/activate
CLOUD_UPLOADER_API_KEY=your_key python examples/basic_upload.py path/to/file --progress

## Usage is exactly as specified:
from cloud_uploader import CloudUploader

uploader = CloudUploader(api_key="ck_live_xxx")
result = uploader.upload_file("video.mp4")

## License

MIT
