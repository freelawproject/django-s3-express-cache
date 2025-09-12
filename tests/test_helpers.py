import unittest
from unittest.mock import patch

from django.core.cache.backends.base import DEFAULT_TIMEOUT

from django_s3_express_cache import (
    S3ExpressCacheBackend,
    parse_time_base_prefix,
    turn_key_into_directory_path,
)


class TestS3ExpressCacheBackendHelpers(unittest.TestCase):
    def setUp(self):
        self.bucket_name = "test-s3-express-bucket"
        self.default_timeout = 300  # 5 minutes

    def test_can_turn_keys_into_directory_paths(self):
        """Tests the internal key transformation to directory-like paths."""
        test_cases = {
            # Keys without a time-based prefix remain unchanged.
            "no_prefix_key": "no_prefix_key",
            "prefix:test_key": "prefix:test_key",
            # Helper can handle singular time prefixes.
            "0-day:test": "0-day/test",
            "1-day:my_key": "1-day/my_key",
            # It can also handle plural time prefixes.
            "10-days:another_key": "10-days/another_key",
            # Keys that already contain directory-like slashes are ignored
            # and remain as is.
            "1-day/already_slashed": "1-day/already_slashed",
            # Only the first part of a time-prefixed key is transformed,
            # subsequent colons or slashes are preserved.
            "35-days:another_key/sub": "35-days/another_key/sub",
            "1-day:key:with:colon": "1-day/key:with:colon",
        }
        for key, expected in test_cases.items():
            with self.subTest(key=key):
                self.assertEqual(turn_key_into_directory_path(key), expected)

    @patch("boto3.client")
    def test_can_get_backend_timeout(self, client_mock):
        """Tests the timeout calculation and handling of special timeout values."""
        test_cases = {
            "Persistent timeout (None)": (None, None),
            "Non-positive timeout (negative)": (-10, 0),
            "Non-positive timeout (zero)": (0, 0),
            "Standard positive integer timeout": (60, 60),
            "DEFAULT_TIMEOUT constant": (
                DEFAULT_TIMEOUT,
                self.default_timeout,
            ),
            "Float timeout should be cast to int": (30.5, 30),
        }
        cache = S3ExpressCacheBackend(
            bucket=self.bucket_name,
            params={
                "LOCATION": self.bucket_name,
                "TIMEOUT": self.default_timeout,
            },
        )

        for description, (
            input_timeout,
            expected_timeout,
        ) in test_cases.items():
            with self.subTest(description=description, input=input_timeout):
                actual_timeout = cache.get_backend_timeout(input_timeout)
                self.assertEqual(actual_timeout, expected_timeout)

    def test_raises_exception_when_parsing_time_prefix(self):
        """Ensures `parse_time_prefix` raises ValueError for invalid key formats."""
        invalid_keys = {
            "no_prefix": "Key with no time prefix",
            "day:test": "Key with missing number in time prefix",
            "abc-day:test": "Key not starting with a digit in the time prefix",
            "1-month:test": "Key with an invalid time unit (not '-day' or '-days')",
            "1-day-extra:test": "Key with malformed time prefix (extra characters)",
            "35-days_another": "Key that looks like a time prefix but uses underscore instead of slash/colon",
            "": "Empty key string",
        }
        for key, description in invalid_keys.items():
            with (
                self.subTest(
                    msg=f"Testing invalid key: '{key}' ({description})"
                ),
                self.assertRaisesRegex(
                    ValueError, "Key does not have a valid time prefix"
                ),
            ):
                parse_time_base_prefix(key)

    @patch("boto3.client")
    def test_can_make_key_with_versioning(self, client_mock):
        """Tests key generation with path transformation and optional versioning."""
        test_cases = {
            "Key without time prefix (no transformation)": (
                "my_raw_key",
                None,
                "my_raw_key_1",
            ),
            "Key with singular time prefix (transformed)": (
                "1-day:my_raw_key",
                None,
                "1-day/my_raw_key_1",
            ),
            "Key with plural time prefix (transformed)": (
                "10-days:another_key",
                None,
                "10-days/another_key_1",
            ),
            "Key with singular time prefix and versioning": (
                "3-days:another_key",
                1,
                "3-days/another_key_1",
            ),
            "Key with no time prefix but with versioning": (
                "simple_key",
                2,
                "simple_key_2",
            ),
        }
        cache = S3ExpressCacheBackend(
            bucket=self.bucket_name,
            params={
                "LOCATION": self.bucket_name,
                "TIMEOUT": self.default_timeout,
            },
        )

        for description, (
            input_key,
            version,
            expected_output,
        ) in test_cases.items():
            with self.subTest(
                msg=f"Scenario: {description}", key=input_key, version=version
            ):
                actual_output = cache.make_key(input_key, version=version)
                self.assertEqual(actual_output, expected_output)

    def test_can_parse_time_prefix(self):
        """check `parse_time_prefix` correctly extracts time values from keys."""
        valid_test_cases = {
            "Singular 'day:' prefix": ("", "1-day:test", 1),
            "Plural 'days:' prefix": ("", "35-days:another", 35),
            "Singular prefix with directory-like slashes": (
                "",
                "1-day/another",
                1,
            ),
            "Plural prefix with directory-like slashes": (
                "",
                "10-days/another",
                10,
            ),
            "Key with subpath and 'days/' prefix": (
                "",
                "90-days/path/to/item",
                90,
            ),
            "Key with subpath and 'day:' prefix": (
                "",
                "1-day:path:to:item",
                1,
            ),
            "Key with backend prefix and plural time base key": (
                "s3-cache",
                "s3-cache/2-days:path:to:item",
                2,
            ),
        }

        for description, (
            backend_prefix,
            key,
            expected_value,
        ) in valid_test_cases.items():
            with self.subTest(
                msg=f"Testing valid key: '{key}' ({description})"
            ):
                self.assertEqual(
                    parse_time_base_prefix(key, backend_prefix), expected_value
                )
