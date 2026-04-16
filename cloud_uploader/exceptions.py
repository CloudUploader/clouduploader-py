"""Custom exception hierarchy for the CloudUploader SDK."""

from __future__ import annotations


class CloudUploaderError(Exception):
    """Base exception for all CloudUploader SDK errors.

    Attributes:
        message: Human-readable error description.
        error_code: Optional machine-readable error code from the backend
                    (e.g. ``"STORAGE_DISABLED"``, ``"INIT_FAILED"``).
        status_code: HTTP status code that triggered the error, if applicable.
    """

    def __init__(
        self,
        message: str,
        *,
        error_code: str | None = None,
        status_code: int | None = None,
    ) -> None:
        super().__init__(message)
        self.message = message
        self.error_code = error_code
        self.status_code = status_code

    def __repr__(self) -> str:
        parts = [f"message={self.message!r}"]
        if self.error_code:
            parts.append(f"error_code={self.error_code!r}")
        if self.status_code is not None:
            parts.append(f"status_code={self.status_code}")
        return f"{type(self).__name__}({', '.join(parts)})"


class AuthenticationError(CloudUploaderError):
    """Raised when the API key is missing, invalid, or rejected (HTTP 401)."""


class UploadInitError(CloudUploaderError):
    """Raised when ``/api/upload/iaas/create`` fails."""


class UploadFailedError(CloudUploaderError):
    """Raised when one or more parts fail to upload, or ``/complete`` fails.

    Attributes:
        failed_parts: Part numbers that could not be uploaded.
        upload_id: The backend upload-session identifier, if available.
    """

    def __init__(
        self,
        message: str,
        *,
        error_code: str | None = None,
        status_code: int | None = None,
        failed_parts: list[int] | None = None,
        upload_id: str | None = None,
    ) -> None:
        super().__init__(message, error_code=error_code, status_code=status_code)
        self.failed_parts = failed_parts or []
        self.upload_id = upload_id


class DownloadError(CloudUploaderError):
    """Raised when a file download fails."""


class FileNotFoundError_(CloudUploaderError):
    """Raised when the local file to upload does not exist."""
