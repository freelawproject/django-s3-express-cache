import pickle
import struct
import unittest
from datetime import datetime, timedelta
from unittest.mock import MagicMock, Mock, patch

from django_s3_express_cache import S3ExpressCacheBackend


class TestS3ExpressCacheBackend(unittest.TestCase):
    DEFAULT_HEADER_FORMAT = "QHHQ"

    def setUp(self):
        """
        Set up mocks for boto3 S3 client and other dependencies before each test.
        """
        self.mock_s3_client = MagicMock()
        # Mocking the client's exceptions.NoSuchKey
        self.mock_s3_client.exceptions.NoSuchKey = type(
            "NoSuchKey", (Exception,), {}
        )

        # Patch boto3.client to return our mock S3 client
        self.boto3_patcher = patch(
            "boto3.client", return_value=self.mock_s3_client
        )
        self.boto3_patcher.start()

        self.bucket_name = "test-s3-express-bucket"
        self.cache = S3ExpressCacheBackend(
            self.bucket_name, {"LOCATION": self.bucket_name, "TIMEOUT": 300}
        )

    def tearDown(self):
        """Clean up mocks after each test."""
        self.boto3_patcher.stop()

    def test_set_timeout_exceeds_key_prefix(self):
        """Tests that a ValueError is raised when timeout exceeds key's time prefix."""
        # A key with a 1-day time prefix (its data should not persist longer
        # than 1 day.)
        test_key = "1-day:my_data"
        test_value = "some_value"
        # A timeout of 2 days, which explicitly violates the 1-day prefix.
        timeout_seconds = 60 * 60 * 24 * 2

        with self.assertRaisesRegex(
            ValueError,
            "The timeout must be less than or equal to the key's time prefix.",
        ):
            self.cache.set(test_key, test_value, timeout=timeout_seconds)
        # Assert that no attempt was made to write to S3, as the validation
        # failed beforehand.
        self.mock_s3_client.put_object.assert_not_called()

    def test_set_rejects_persistent_key_with_time_prefix(self):
        """Ensure persistent keys cannot use time-based prefixes."""
        test_key = "1-day:persistent"
        test_value = "some_value"

        # Persistent key (no expiration)
        timeout = None
        with self.assertRaisesRegex(
            ValueError,
            "Persistent keys cannot use a time-based prefix",
        ):
            self.cache.set(test_key, test_value, timeout=timeout)

        # Verify no S3 write was attempted due to validation failure
        self.mock_s3_client.put_object.assert_not_called()

    @patch("time.time_ns")
    def test_set_timeout_within_key_prefix(self, mock_time_ns):
        """Tests setting a key with a timeout within its defined time prefix."""
        # Mock time.time_ns to return a predictable timestamp in nanoseconds.
        fake_date = datetime.now().replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        mock_time_ns.return_value = int(fake_date.timestamp() * 1e9)

        # A key with a 10-day time prefix.
        test_key = "10-days:my_data"
        test_value = "some_value"
        # A timeout of 5 days, which is well within the 10-day prefix.
        timeout_seconds = 5 * 24 * 60 * 60

        self.cache.set(test_key, test_value, timeout=timeout_seconds)
        # Verify that the S3 put_object method was called exactly once,
        # indicating successful storage.
        self.mock_s3_client.put_object.assert_called_once()

    def test_set_persistent_timeout(self):
        """Tests setting a key with a persistent timeout (None)."""
        test_key = "persistent_data:test"
        test_value = "persistent_value"
        # Set timeout to None to indicate the key should not expire.
        timeout = None

        expected_s3_key = "persistent_data:test_1"
        # Persistent keys are stored with an expiration time of 0.
        expected_expiration_ns = 0
        expected_content_prefix = struct.pack(
            self.DEFAULT_HEADER_FORMAT, expected_expiration_ns, 1, 0, 0
        )
        expected_body = expected_content_prefix + pickle.dumps(
            test_value, pickle.HIGHEST_PROTOCOL
        )

        self.cache.set(test_key, test_value, timeout=timeout)

        self.mock_s3_client.put_object.assert_called_once_with(
            Bucket=self.bucket_name, Key=expected_s3_key, Body=expected_body
        )

    def test_set_zero_timeout(self):
        """Tests setting a key with timeout=0 (should not be cached)."""
        test_key = "zero_timeout:test"
        test_value = "should_not_be_cached"
        timeout = 0

        self.cache.set(test_key, test_value, timeout=timeout)

        # Since timeout=0 means "don't cache", no S3 put_object call should happen.
        self.mock_s3_client.put_object.assert_not_called()

    @patch("time.time_ns")
    def test_set_with_backend_default_timeout(self, mock_time_ns):
        """Test set uses the backend's default timeout when it's not provided."""
        # Mock time to a specific point for consistent expiration calculations.
        fake_date = datetime.now().replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        mock_time_ns.return_value = int(fake_date.timestamp() * 1e9)

        test_key = "1-day:default_timeout_data"
        test_value = {"data": [1, 2, 3]}

        self.mock_s3_client.put_object.return_value = {}
        # Calculate the expected expiration time based on the fixed_time and
        # the cache's default_timeout.
        expected_expiration_ns = int(
            (fake_date.timestamp() + self.cache.default_timeout) * 1e9
        )
        expected_content_prefix = struct.pack(
            self.DEFAULT_HEADER_FORMAT, expected_expiration_ns, 1, 0, 0
        )
        expected_body = expected_content_prefix + pickle.dumps(
            test_value, pickle.HIGHEST_PROTOCOL
        )

        expected_s3_key = "1-day/default_timeout_data_1"

        # Call set without explicitly providing a timeout, so the default
        # should be used.
        self.cache.set(test_key, test_value)

        # Assert that put_object was called exactly once with the correct
        # parameters.
        self.mock_s3_client.put_object.assert_called_once()
        self.mock_s3_client.put_object.assert_called_once_with(
            Bucket=self.bucket_name, Key=expected_s3_key, Body=expected_body
        )

    def test_has_key_for_non_existent_entry(self):
        """Verifies has_key returns False for a non-existent cache entry."""
        # Simulate a NoSuchKey exception from S3, indicating the key does
        # not exist.
        self.mock_s3_client.get_object.side_effect = (
            self.mock_s3_client.exceptions.NoSuchKey
        )

        self.assertFalse(self.cache.has_key("1-day:non_existent_key"))
        # Verify that get_object was called exactly once to check for the
        # key's existence.
        self.mock_s3_client.get_object.assert_called_once()

    def test_has_key_for_expired_entry(self):
        """Verifies has_key returns False for an expired cache entry."""
        # Simulate an S3 object with an expiration timestamp in the past.
        date_today = datetime.now().replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        date_ten_days_ago = date_today - timedelta(days=10)
        past_expiration_ns = int(date_ten_days_ago.timestamp() * 1e9)
        # Create the content prefix with the past expiration timestamp.
        content_prefix = struct.pack(
            self.DEFAULT_HEADER_FORMAT, past_expiration_ns, 1, 0, 0
        )

        # Configure the mock S3 client to return the expired content.
        mock_response_body = Mock()
        mock_response_body.read.side_effect = [content_prefix]
        self.mock_s3_client.get_object.return_value = {
            "Body": mock_response_body
        }

        self.assertFalse(self.cache.has_key("1-day:expired_key"))
        # Verify that get_object was called exactly once to retrieve
        # the entry.
        self.mock_s3_client.get_object.assert_called_once()

    def test_has_key_for_persistent_entry(self):
        """Verifies has_key returns True for a persistent cache entry."""
        # Simulate a persistent S3 object by setting its expiration time to 0.
        persistent_expiration_ns = 0
        content_prefix = struct.pack(
            self.DEFAULT_HEADER_FORMAT, persistent_expiration_ns, 1, 0, 0
        )

        # Configure the mock S3 client to return the persistent content.
        mock_response_body = Mock()
        mock_response_body.read.side_effect = [content_prefix]
        self.mock_s3_client.get_object.return_value = {
            "Body": mock_response_body
        }

        self.assertTrue(self.cache.has_key("1-day:persistent_key"))
        # Ensure that get_object was called exactly once to check for the key's
        # existence.
        self.mock_s3_client.get_object.assert_called_once()

    def test_has_key_for_existing_unexpired_entry(self):
        """Verifies has_key returns True for an existing, unexpired entry."""
        # Set an expiration date in the future to simulate an unexpired entry.
        date_today = datetime.now().replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        tomorrow = date_today + timedelta(days=1)
        future_expiration_ns = int(tomorrow.timestamp() * 1e9)

        # Pack the future expiration time into an 8-byte prefix.
        content_prefix = struct.pack(
            self.DEFAULT_HEADER_FORMAT, future_expiration_ns, 1, 0, 0
        )

        # Configure the mock S3 client to return the content prefix.
        mock_response_body = Mock()
        mock_response_body.read.side_effect = [content_prefix]
        self.mock_s3_client.get_object.return_value = {
            "Body": mock_response_body
        }

        self.assertTrue(self.cache.has_key("1-day:my_key"))

        # Ensure that get_object was called exactly once to retrieve the
        # entry's metadata.
        self.mock_s3_client.get_object.assert_called_once()

    def test_add_new_key(self):
        """Verifies that `add` successfully stores a new key-value pair."""
        # Mock `has_key` to simulate the scenario where the key doesn't exist
        # in the cache.
        with patch.object(
            self.cache, "has_key", return_value=False
        ) as mock_has_key:
            result = self.cache.add("1-day:new_key", "new_value", timeout=60)
            # Attempt to add the new key.
            self.assertTrue(result)

            # Verify that `has_key` was called to check for the key's
            # existence.
            mock_has_key.assert_called_once_with("1-day:new_key", version=None)

            # Ensure that `put_object` was called, indicating the key was
            # stored in S3.
            self.mock_s3_client.put_object.assert_called_once()

    def test_add_existing_key(self):
        """Ensures `add` does not overwrite an existing, unexpired key."""
        # Mock `has_key` to simulate the scenario where the key already
        # exists and is valid.
        with patch.object(
            self.cache, "has_key", return_value=True
        ) as mock_has_key:
            result = self.cache.add(
                "1-day:existing_key", "value_to_overwrite", timeout=60
            )
            # Assert that the add operation failed because the key already
            # exists.
            self.assertFalse(result)

            # Verify that `has_key` was called to check for the key's existence.
            mock_has_key.assert_called_once_with(
                "1-day:existing_key", version=None
            )

            # Ensure that `put_object` was NOT called, as the existing key
            # should not be overwritten.
            self.mock_s3_client.put_object.assert_not_called()

    def test_get_not_found(self):
        """Verifies `get` returns the default value when a key is not found."""
        # Simulate a NoSuchKey exception from S3, indicating the key does not
        # exist.
        self.mock_s3_client.get_object.side_effect = (
            self.mock_s3_client.exceptions.NoSuchKey
        )

        # Attempt to retrieve a key that does not exist.
        result = self.cache.get("1-day:non_existent", default="default_value")

        # Assert that the default value is returned, as the key was not found.
        self.assertEqual(result, "default_value")

        # Verify that `get_object` was called exactly once to attempt retrieval.
        self.mock_s3_client.get_object.assert_called_once()

    def test_get_expired_key_returns_default(self):
        """Verifies `get` returns the default value for an expired key."""
        # Set an expiration date in the past to simulate an expired cache entry.
        date_today = datetime.now().replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        date_two_days_ago = date_today - timedelta(days=2)
        past_expiration_ns = int(date_two_days_ago.timestamp() * 1e9)

        # Create the content prefix with the past expiration timestamp.
        content_prefix = struct.pack(
            self.DEFAULT_HEADER_FORMAT, past_expiration_ns, 1, 0, 0
        )

        # Configure the mock S3 client to return only the content prefix (no
        # actual data needed for an expired key).
        mock_response_body = Mock()
        mock_response_body.iter_chunks.return_value = iter([content_prefix])
        self.mock_s3_client.get_object.return_value = {
            "Body": mock_response_body
        }

        # Call the `get` method for the expired key.
        result = self.cache.get("1-day:expired_key", default="default_value")

        # Assert that the default value is returned, as the key is expired.
        self.assertEqual(result, "default_value")

        # Verify that `get_object` was called exactly once to retrieve the
        # key's metadata.
        self.mock_s3_client.get_object.assert_called_once()

    def test_get_persistent_key(self):
        """Verifies `get` retrieves the correct value for a persistent key."""
        # Simulate a persistent object by setting its expiration time to 0.
        persistent_expiration_ns = 0
        content_prefix = struct.pack(
            self.DEFAULT_HEADER_FORMAT, persistent_expiration_ns, 1, 0, 0
        )
        pickled_value = pickle.dumps("persistent_value")

        # Configure the mock S3 client to return the content for a persistent
        # key.
        mock_response_body = Mock()
        mock_response_body.iter_chunks.return_value = iter(
            [content_prefix, pickled_value]
        )
        self.mock_s3_client.get_object.return_value = {
            "Body": mock_response_body
        }

        # Attempt to retrieve the persistent key.
        result = self.cache.get(
            "no-prefix-day:persistent_key", default="default_value"
        )
        self.assertEqual(result, "persistent_value")

        # Verify that the S3 get_object method was called exactly once.
        self.mock_s3_client.get_object.assert_called_once()

    def test_get_unexpired_key(self):
        """Verifies `get` retrieves the correct value for an unexpired key."""
        # Set an expiration date in the future to simulate an unexpired cache
        # entry.
        date_today = datetime.now().replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        future_expiration_time = date_today + timedelta(days=2)
        future_expiration_ns = int(future_expiration_time.timestamp() * 1e9)

        # Prepare the content prefix and the pickled value that S3 would return
        content_prefix = struct.pack(
            self.DEFAULT_HEADER_FORMAT, future_expiration_ns, 1, 0, 0
        )
        pickled_value = pickle.dumps("valid_value")

        # Configure the mock S3 client to return the prefixed and pickled
        # content.
        mock_response_body = Mock()
        mock_response_body.iter_chunks.return_value = iter(
            [content_prefix, pickled_value]
        )
        self.mock_s3_client.get_object.return_value = {
            "Body": mock_response_body
        }

        # Call the `get` method and assert that it returns the unexpired cached
        # value.
        result = self.cache.get("2-day:expired_key", default="default_value")
        self.assertEqual(result, "valid_value")

        # Verify that `get_object` was called exactly once to retrieve the key
        # from S3.
        self.mock_s3_client.get_object.assert_called_once()

    def test_delete_success(self):
        """Verifies that `delete` successfully removes a key from the cache."""
        # Configure the mock S3 client to simulate a successful deletion.
        self.mock_s3_client.delete_object.return_value = {}

        self.assertTrue(self.cache.delete("1-day:key_to_delete"))

        # Verify that the S3 client's `delete_object` method was called exactly
        # once with the correct bucket and S3 key.
        self.mock_s3_client.delete_object.assert_called_once_with(
            Bucket=self.bucket_name, Key="1-day/key_to_delete_1"
        )
