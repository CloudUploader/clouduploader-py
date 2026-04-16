"""CloudUploader Python SDK — upload files to S3/R2/Azure/GCS via presigned URLs.

Usage::

    from cloud_uploader import CloudUploader

    uploader = CloudUploader(api_key="ck_live_xxx")
    result = uploader.upload_file("video.mp4")
    print(result.storage_path)
"""

from cloud_uploader.client import CloudUploader
from cloud_uploader.exceptions import (
    AuthenticationError,
    CloudUploaderError,
    DownloadError,
    FileNotFoundError_,
    UploadFailedError,
    UploadInitError,
)
from cloud_uploader.uploader import (
    UploadResult,
    FolderUploadResult,
    FolderUploadFailure,
)

__all__ = [
    "CloudUploader",
    "UploadResult",
    "FolderUploadResult",
    "FolderUploadFailure",
    # Exceptions
    "CloudUploaderError",
    "AuthenticationError",
    "UploadInitError",
    "UploadFailedError",
    "DownloadError",
    "FileNotFoundError_",
]

__version__ = "0.1.0"
