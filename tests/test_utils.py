"""Tests for cloud_uploader.utils."""

import os
import tempfile

import pytest

from cloud_uploader.exceptions import FileNotFoundError_
from cloud_uploader.utils import format_bytes, guess_content_type, sanitize_filename, validate_file


class TestGuessContentType:
    """MIME type guessing mirrors the backend."""

    @pytest.mark.parametrize(
        "filename,expected",
        [
            ("photo.jpg", "image/jpeg"),
            ("Photo.JPEG", "image/jpeg"),
            ("doc.pdf", "application/pdf"),
            ("script.js", "application/javascript"),
            ("video.mp4", "video/mp4"),
            ("archive.zip", "application/zip"),
            ("data.csv", "text/csv"),
            ("module.wasm", "application/wasm"),
            ("unknown.xyz", "application/octet-stream"),
            ("noext", "application/octet-stream"),
        ],
    )
    def test_known_extensions(self, filename: str, expected: str):
        assert guess_content_type(filename) == expected


class TestSanitizeFilename:
    def test_strips_directory(self):
        assert sanitize_filename("/home/user/file.txt") == "file.txt"
        assert sanitize_filename("file.txt") == "file.txt"

    def test_plain_basename(self):
        # sanitize_filename uses os.path.basename, which is OS-native.
        # On Linux, backslashes are valid filename characters, so we only
        # test forward-slash stripping here.
        assert sanitize_filename("dir/sub/doc.pdf") == "doc.pdf"


class TestFormatBytes:
    def test_small(self):
        assert format_bytes(512) == "512.00 B"

    def test_kilobytes(self):
        assert format_bytes(1536) == "1.50 KB"

    def test_megabytes(self):
        assert format_bytes(10 * 1024 * 1024) == "10.00 MB"


class TestValidateFile:
    def test_existing_file(self):
        with tempfile.NamedTemporaryFile(delete=False) as f:
            f.write(b"hello")
            path = f.name
        try:
            abs_path, size = validate_file(path)
            assert os.path.isabs(abs_path)
            assert size == 5
        finally:
            os.unlink(path)

    def test_nonexistent(self):
        with pytest.raises(FileNotFoundError_):
            validate_file("/no/such/file.txt")

    def test_directory(self):
        with pytest.raises(FileNotFoundError_):
            validate_file(tempfile.gettempdir())
