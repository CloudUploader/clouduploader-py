"""Tests for cloud_uploader.exceptions."""

from cloud_uploader.exceptions import (
    AuthenticationError,
    CloudUploaderError,
    DownloadError,
    FileNotFoundError_,
    UploadFailedError,
    UploadInitError,
)


class TestExceptionHierarchy:
    """All custom exceptions inherit from CloudUploaderError."""

    def test_base_exception(self):
        e = CloudUploaderError("boom", error_code="E1", status_code=500)
        assert str(e) == "boom"
        assert e.message == "boom"
        assert e.error_code == "E1"
        assert e.status_code == 500

    def test_base_repr(self):
        e = CloudUploaderError("x", error_code="CODE", status_code=418)
        r = repr(e)
        assert "error_code='CODE'" in r
        assert "status_code=418" in r

    def test_authentication_error(self):
        e = AuthenticationError("bad key")
        assert isinstance(e, CloudUploaderError)

    def test_upload_init_error(self):
        e = UploadInitError("no storage")
        assert isinstance(e, CloudUploaderError)

    def test_upload_failed_error_with_parts(self):
        e = UploadFailedError(
            "partial",
            failed_parts=[2, 5],
            upload_id="up_123",
        )
        assert isinstance(e, CloudUploaderError)
        assert e.failed_parts == [2, 5]
        assert e.upload_id == "up_123"

    def test_download_error(self):
        assert isinstance(DownloadError("dl fail"), CloudUploaderError)

    def test_file_not_found(self):
        assert isinstance(FileNotFoundError_("nope"), CloudUploaderError)

    def test_defaults(self):
        e = CloudUploaderError("plain")
        assert e.error_code is None
        assert e.status_code is None
