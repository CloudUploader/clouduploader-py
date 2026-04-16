"""Tests for the full upload flow (direct + multipart) via mocked backend."""

import os
import tempfile

import pytest
import responses

from cloud_uploader import CloudUploader, UploadResult
from cloud_uploader.exceptions import AuthenticationError, UploadInitError

BASE = "http://test-api.local"
API_KEY = "ck_test_abc"


def _create_temp_file(size: int) -> str:
    """Create a temporary file of the given size and return its path."""
    f = tempfile.NamedTemporaryFile(delete=False, suffix=".bin")
    f.write(os.urandom(size))
    f.close()
    return f.name


class TestDirectUpload:
    """Test the direct (single presigned PUT) upload path."""

    @responses.activate
    def test_small_file_direct_upload(self):
        file_path = _create_temp_file(1024)
        try:
            # Mock /iaas/create → direct mode
            responses.add(
                responses.POST,
                f"{BASE}/api/upload/iaas/create",
                json={
                    "success": True,
                    "upload_id": "up_001",
                    "key": "ab/cd/file.bin",
                    "mode": "direct",
                    "presigned_url": "https://s3.example.com/put-here",
                    "part_parallelism": 1,
                },
                status=200,
            )
            # Mock presigned PUT
            responses.add(
                responses.PUT,
                "https://s3.example.com/put-here",
                status=200,
                headers={"ETag": '"etag1"'},
            )
            # Mock /iaas/complete
            responses.add(
                responses.POST,
                f"{BASE}/api/upload/iaas/complete",
                json={
                    "success": True,
                    "key": "ab/cd/file.bin",
                    "storage": "r2",
                    "storagePath": "r2://bucket/ab/cd/file.bin",
                },
                status=200,
            )

            uploader = CloudUploader(api_key=API_KEY, base_url=BASE)
            result = uploader.upload_file(file_path)

            assert isinstance(result, UploadResult)
            assert result.upload_id == "up_001"
            assert result.mode == "direct"
            assert result.storage_path == "r2://bucket/ab/cd/file.bin"
        finally:
            os.unlink(file_path)

    @responses.activate
    def test_progress_callback_called(self):
        file_path = _create_temp_file(512)
        progress_calls: list[tuple[int, int]] = []
        try:
            responses.add(
                responses.POST,
                f"{BASE}/api/upload/iaas/create",
                json={
                    "success": True,
                    "upload_id": "up_002",
                    "key": "k",
                    "mode": "direct",
                    "presigned_url": "https://s3.example.com/up",
                    "part_parallelism": 1,
                },
            )
            responses.add(responses.PUT, "https://s3.example.com/up", status=200)
            responses.add(
                responses.POST,
                f"{BASE}/api/upload/iaas/complete",
                json={"success": True, "key": "k", "storage": "r2", "storagePath": "r2://b/k"},
            )

            uploader = CloudUploader(api_key=API_KEY, base_url=BASE)
            uploader.upload_file(
                file_path,
                progress_callback=lambda u, t: progress_calls.append((u, t)),
            )
            assert len(progress_calls) >= 1
            assert progress_calls[-1][0] == progress_calls[-1][1]  # 100%
        finally:
            os.unlink(file_path)


class TestMultipartUpload:
    """Test the multipart (parallel chunks) upload path."""

    @responses.activate
    def test_multipart_two_parts(self):
        file_path = _create_temp_file(2048)
        try:
            responses.add(
                responses.POST,
                f"{BASE}/api/upload/iaas/create",
                json={
                    "success": True,
                    "upload_id": "up_mp_01",
                    "key": "ab/cd/big.bin",
                    "mode": "multipart",
                    "chunk_size": 1024,
                    "num_parts": 2,
                    "part_parallelism": 2,
                    "presigned_urls": [
                        "https://s3.example.com/part1",
                        "https://s3.example.com/part2",
                    ],
                },
            )
            # Both part PUTs succeed
            responses.add(
                responses.PUT,
                "https://s3.example.com/part1",
                status=200,
                headers={"ETag": '"etag_p1"'},
            )
            responses.add(
                responses.PUT,
                "https://s3.example.com/part2",
                status=200,
                headers={"ETag": '"etag_p2"'},
            )
            responses.add(
                responses.POST,
                f"{BASE}/api/upload/iaas/complete",
                json={
                    "success": True,
                    "key": "ab/cd/big.bin",
                    "storage": "r2",
                    "storagePath": "r2://bucket/ab/cd/big.bin",
                },
            )

            uploader = CloudUploader(api_key=API_KEY, base_url=BASE)
            result = uploader.upload_file(file_path)

            assert result.mode == "multipart"
            assert result.upload_id == "up_mp_01"

            # Verify the complete call sent parts
            complete_body = responses.calls[-1].request.body
            assert b"part_number" in complete_body
            assert b"etag" in complete_body
        finally:
            os.unlink(file_path)


class TestErrorScenarios:
    """Error handling and edge cases."""

    @responses.activate
    def test_auth_failure(self):
        responses.add(
            responses.POST,
            f"{BASE}/api/upload/iaas/create",
            json={"error": "UNAUTHORIZED", "message": "Invalid key"},
            status=401,
        )
        file_path = _create_temp_file(64)
        try:
            uploader = CloudUploader(api_key="bad_key", base_url=BASE)
            with pytest.raises((AuthenticationError, UploadInitError)):
                uploader.upload_file(file_path)
        finally:
            os.unlink(file_path)

    def test_file_not_found(self):
        uploader = CloudUploader(api_key=API_KEY, base_url=BASE)
        with pytest.raises(Exception, match="not found"):
            uploader.upload_file("/nonexistent/file.bin")

    def test_empty_api_key_rejected(self):
        with pytest.raises(ValueError, match="api_key"):
            CloudUploader(api_key="")


class TestStatusAndAbort:
    """Status check and abort endpoints."""

    @responses.activate
    def test_get_status(self):
        responses.add(
            responses.GET,
            f"{BASE}/api/upload/iaas/status/up_999",
            json={"success": True, "status": "completed", "upload_id": "up_999"},
        )
        uploader = CloudUploader(api_key=API_KEY, base_url=BASE)
        status = uploader.get_upload_status("up_999")
        assert status["status"] == "completed"

    @responses.activate
    def test_abort(self):
        responses.add(
            responses.POST,
            f"{BASE}/api/upload/iaas/abort",
            json={"success": True},
        )
        uploader = CloudUploader(api_key=API_KEY, base_url=BASE)
        result = uploader.abort_upload("up_999")
        assert result["success"] is True


class TestFolderUpload:
    """Test folder upload capabilities."""

    @responses.activate
    def test_flat_folder_success(self):
        with tempfile.TemporaryDirectory() as td:
            with open(os.path.join(td, "a.txt"), "w") as f:
                f.write("A")
            with open(os.path.join(td, "b.txt"), "w") as f:
                f.write("B")

            # Mock initialize and complete for a.txt and b.txt
            responses.add(
                responses.POST,
                f"{BASE}/api/upload/iaas/create",
                json={
                    "success": True,
                    "upload_id": "up_1",
                    "key": "x.txt",
                    "presigned_url": "https://example.com/put",
                },
            )
            responses.add(responses.PUT, "https://example.com/put", status=200)
            responses.add(
                responses.POST,
                f"{BASE}/api/upload/iaas/complete",
                json={"success": True, "storagePath": "r2://b/x"},
            )

            uploader = CloudUploader(api_key=API_KEY, base_url=BASE)
            res = uploader.upload_folder(td)

            assert res.total_files == 2
            assert res.succeeded == 2
            assert res.failed == 0
            assert len(res.results) == 2
            assert len(res.failures) == 0

    @responses.activate
    def test_nested_folder_and_skip_hidden(self):
        with tempfile.TemporaryDirectory() as td:
            os.makedirs(os.path.join(td, "sub"))
            os.makedirs(os.path.join(td, ".hidden_dir"))
            with open(os.path.join(td, "a.txt"), "w") as f:
                f.write("A")
            with open(os.path.join(td, "sub", "b.txt"), "w") as f:
                f.write("B")
            with open(os.path.join(td, "sub", ".hidden_file"), "w") as f:
                f.write("H")
            with open(os.path.join(td, ".hidden_dir", "c.txt"), "w") as f:
                f.write("C")

            responses.add(
                responses.POST,
                f"{BASE}/api/upload/iaas/create",
                json={
                    "success": True,
                    "upload_id": "up_1",
                    "key": "x",
                    "presigned_url": "https://example.com/put",
                },
            )
            responses.add(responses.PUT, "https://example.com/put", status=200)
            responses.add(
                responses.POST,
                f"{BASE}/api/upload/iaas/complete",
                json={"success": True},
            )

            uploader = CloudUploader(api_key=API_KEY, base_url=BASE)
            res = uploader.upload_folder(td)

            # Should skip hidden_file and hidden_dir/c.txt, leaving only a.txt and b.txt
            assert res.total_files == 2
            assert res.succeeded == 2

    def test_empty_folder(self):
        with tempfile.TemporaryDirectory() as td:
            uploader = CloudUploader(api_key=API_KEY, base_url=BASE)
            res = uploader.upload_folder(td)
            assert res.total_files == 0
            assert res.succeeded == 0

    def test_not_a_directory(self):
        uploader = CloudUploader(api_key=API_KEY, base_url=BASE)
        with pytest.raises(Exception, match="Folder not found"):
            uploader.upload_folder("/nonexistent/directory")

        # Test with file instead of folder
        file_path = _create_temp_file(10)
        try:
            with pytest.raises(Exception, match="not a directory"):
                uploader.upload_folder(file_path)
        finally:
            os.unlink(file_path)

    @responses.activate
    def test_partial_failure(self):
        with tempfile.TemporaryDirectory() as td:
            with open(os.path.join(td, "a.txt"), "w") as f:
                f.write("A")
            with open(os.path.join(td, "b.txt"), "w") as f:
                f.write("B")

            # First create fails (for A), second succeeds (for B)
            responses.add(
                responses.POST,
                f"{BASE}/api/upload/iaas/create",
                json={"success": False, "message": "Failed init"},
                status=400,
            )
            responses.add(
                responses.POST,
                f"{BASE}/api/upload/iaas/create",
                json={
                    "success": True,
                    "upload_id": "up_1",
                    "key": "x.txt",
                    "presigned_url": "https://example.com/put",
                },
            )
            responses.add(responses.PUT, "https://example.com/put", status=200)
            responses.add(
                responses.POST,
                f"{BASE}/api/upload/iaas/complete",
                json={"success": True},
            )

            uploader = CloudUploader(api_key=API_KEY, base_url=BASE)
            res = uploader.upload_folder(td)

            assert res.total_files == 2
            assert res.succeeded == 1
            assert res.failed == 1
            assert len(res.failures) == 1
            assert "Failed init" in res.failures[0].error

