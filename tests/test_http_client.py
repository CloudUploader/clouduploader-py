"""Tests for cloud_uploader.http_client."""

import pytest
import responses

from cloud_uploader.exceptions import AuthenticationError, CloudUploaderError
from cloud_uploader.http_client import HttpClient


BASE = "http://test-api.local"


class TestAuthHeaders:
    """Verify that all requests carry both auth headers."""

    @responses.activate
    def test_post_json_sends_headers(self):
        responses.add(
            responses.POST,
            f"{BASE}/api/upload/iaas/create",
            json={"success": True},
            status=200,
        )
        client = HttpClient("sk_test_key", BASE, max_retries=0)
        client.post_json("/api/upload/iaas/create", {"filename": "x"})

        req = responses.calls[0].request
        assert req.headers["Authorization"] == "Bearer sk_test_key"
        assert req.headers["X-Api-Key"] == "sk_test_key"

    @responses.activate
    def test_get_sends_headers(self):
        responses.add(
            responses.GET,
            f"{BASE}/api/upload/iaas/status/id1",
            json={"success": True},
            status=200,
        )
        client = HttpClient("sk_test_key", BASE, max_retries=0)
        client.get("/api/upload/iaas/status/id1")

        req = responses.calls[0].request
        assert "Bearer sk_test_key" in req.headers["Authorization"]


class TestErrorHandling:
    """JSON error extraction from non-2xx responses."""

    @responses.activate
    def test_401_raises_auth_error(self):
        responses.add(
            responses.POST,
            f"{BASE}/path",
            json={"error": "UNAUTHORIZED", "message": "Bad key"},
            status=401,
        )
        client = HttpClient("bad_key", BASE, max_retries=0)
        with pytest.raises(AuthenticationError) as exc_info:
            client.post_json("/path", {})
        assert exc_info.value.error_code == "UNAUTHORIZED"
        assert exc_info.value.status_code == 401

    @responses.activate
    def test_500_raises_cloud_error(self):
        responses.add(
            responses.GET,
            f"{BASE}/boom",
            json={"error": "INTERNAL", "message": "oops"},
            status=500,
        )
        client = HttpClient("key", BASE, max_retries=0)
        with pytest.raises(CloudUploaderError) as exc_info:
            client.get("/boom")
        assert exc_info.value.status_code == 500

    @responses.activate
    def test_non_json_error_body(self):
        responses.add(
            responses.GET,
            f"{BASE}/html",
            body="<h1>Error</h1>",
            status=503,
            content_type="text/html",
        )
        client = HttpClient("key", BASE, max_retries=0)
        with pytest.raises(CloudUploaderError):
            client.get("/html")


class TestPutBinary:
    """Binary PUT with retry behaviour."""

    @responses.activate
    def test_successful_put(self):
        responses.add(
            responses.PUT,
            "https://presigned.s3.example.com/part1",
            headers={"ETag": '"abc123"'},
            status=200,
        )
        client = HttpClient("key", BASE)
        resp = client.put_binary(
            "https://presigned.s3.example.com/part1",
            data=b"chunk-data",
        )
        assert resp.status_code == 200
        assert resp.headers["ETag"] == '"abc123"'

    @responses.activate
    def test_put_retries_on_503(self):
        # First call: 503, second call: 200
        responses.add(
            responses.PUT,
            "https://s3.example.com/part",
            status=503,
        )
        responses.add(
            responses.PUT,
            "https://s3.example.com/part",
            status=200,
            headers={"ETag": '"ok"'},
        )
        client = HttpClient("key", BASE, max_retries=2, backoff_factor=0.01)
        resp = client.put_binary(
            "https://s3.example.com/part",
            data=b"data",
            backoff=0.01,
        )
        assert resp.status_code == 200
        assert len(responses.calls) == 2

    @responses.activate
    def test_put_exhausts_retries(self):
        for _ in range(5):
            responses.add(
                responses.PUT,
                "https://s3.example.com/fail",
                status=503,
            )
        client = HttpClient("key", BASE, max_retries=2, backoff_factor=0.01)
        with pytest.raises(CloudUploaderError, match="failed after"):
            client.put_binary(
                "https://s3.example.com/fail",
                data=b"data",
                retries=2,
                backoff=0.01,
            )
