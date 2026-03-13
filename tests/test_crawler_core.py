from __future__ import annotations

import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

import crawler


class TestCrawlerCore(unittest.TestCase):
    def test_parse_optional_int_env_default(self) -> None:
        with patch.dict(os.environ, {}, clear=True):
            self.assertEqual(crawler.parse_optional_int_env("X_TEST_INT", 7), 7)

    def test_parse_optional_int_env_invalid(self) -> None:
        with patch.dict(os.environ, {"X_TEST_INT": "abc"}, clear=True):
            with self.assertRaises(crawler.CrawlError) as ctx:
                crawler.parse_optional_int_env("X_TEST_INT", 7)
            self.assertEqual(ctx.exception.kind, "CONFIG")

    def test_authenticate_http_401_is_auth_error(self) -> None:
        mock_response = Mock()
        mock_response.status_code = 401
        mock_response.text = "unauthorized"
        mock_response.json.return_value = {"message": "invalid credentials"}

        with patch("crawler.requests.post", return_value=mock_response):
            with self.assertRaises(crawler.CrawlError) as ctx:
                crawler.authenticate(
                    base_url="http://localhost:3000",
                    username="u",
                    password="p",
                    auth_timeout_seconds=1,
                )
            self.assertEqual(ctx.exception.kind, "AUTH")

    def test_get_json_retries_transient_then_succeeds(self) -> None:
        transient = Mock()
        transient.status_code = 500
        transient.text = "server error"

        success = Mock()
        success.status_code = 200
        success.json.return_value = {"ok": True}

        with patch("crawler.requests.get", side_effect=[transient, success]) as mock_get:
            payload, _, attempts = crawler.get_json(
                url="http://localhost:3000/api/test",
                headers={},
                timeout_seconds=1,
                max_retries=1,
                backoff_seconds=0.0,
            )
            self.assertEqual(payload, {"ok": True})
            self.assertEqual(attempts, 2)
            self.assertEqual(mock_get.call_count, 2)

    def test_get_json_non_transient_is_api_error(self) -> None:
        non_transient = Mock()
        non_transient.status_code = 404
        non_transient.text = "not found"

        with patch("crawler.requests.get", return_value=non_transient):
            with self.assertRaises(crawler.CrawlError) as ctx:
                crawler.get_json(
                    url="http://localhost:3000/api/test",
                    headers={},
                    timeout_seconds=1,
                    max_retries=2,
                    backoff_seconds=0.0,
                )
            self.assertEqual(ctx.exception.kind, "API")
            self.assertEqual(ctx.exception.status_code, 404)

    def test_write_json_creates_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            target = Path(tmpdir) / "nested" / "result.json"
            crawler.write_json(target, {"a": 1})
            self.assertTrue(target.exists())
            self.assertIn('"a": 1', target.read_text(encoding="utf-8"))


if __name__ == "__main__":
    unittest.main()
