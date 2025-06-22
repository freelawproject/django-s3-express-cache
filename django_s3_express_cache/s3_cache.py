import pickle
import re
import struct
import time
from datetime import datetime

import boto3
from django.core.cache.backends.base import DEFAULT_TIMEOUT, BaseCache


def turn_key_into_directory_path(key: str) -> str:
    """
    Transforms a cache key into an S3 object path for optimized write
    performance.

    This method converts keys like 'N-days:actual_key' into 'N-days/actual_key'.
    This transformation is intended to improve S3 write throughput by
    distributing objects across different logical prefixes, taking advantage of
    S3's internal partitioning mechanisms.

    Args:
        key (str): The full name of the S3 object.

    Returns:
        str: The transformed S3 object key with a slash and the time-based
            prefix, or the original key if no transformation is necessary or
            applicable.
    """
    pattern_with_colon = r"^(^\d+-days?):(.*)$"

    # Attempt to match the pattern at the beginning of the S3 object key.
    match = re.match(pattern_with_colon, key)
    if not match:
        return key
    # If a match is found, extract the prefix and the rest of the key,
    # then reformat with a slash for S3 partitioning.
    return f"{match.group(1)}/{match.group(2)}"


def parse_time_base_prefix(key: str) -> int:
    """
    Parses the numeric time component (days) from the cache key's prefix.

    This method expects the key to start with a time-based prefix in the
    format "N-day(s):" or "N-day(s)/". It extracts the integer value 'N'
    from this prefix. This numeric value represents the maximum lifespan
    (in days) of the cached item.

    Args:
        key (str): The cache key string.

    Raises:
        ValueError: If the key does not conform to the expected time-based
            prefix format (e.g., "N-day(s):" or "N-day(s)/").

    Returns:
        int: The integer value representing the number of days from the
            prefix.
    """
    pattern = r"^(^\d+)-days?[:/](.*)$"

    match = re.match(pattern, key)
    if not match:
        raise ValueError("Key does not have a valid time prefix")

    return int(match.group(1))


class S3ExpressCacheBackend(BaseCache):
    """
    A Django cache backend that leverages AWS S3 Express One Zone for
    high-throughput, low-latency caching.

    This backend stores cache items as objects in a specified S3 Express One
    Zone bucket. It provides custom key generation to align with S3's best
    practices for performance, including:

    - Supports Django's versioning and prefix mechanisms.
    - Transforms time-based key prefixes (e.g., "N-days:key") into
      directory-like paths(e.g., "N-days/key") to improve object distribution
      and write throughput for specific access patterns.
    - Manages cache item expiration by embedding timestamps within the S3
      object data, supporting both time-limited and persistent cache entries.
    - Enforces a validation rule ensuring that a cache item's specified
      `timeout` does not exceed the maximum lifespan implied by its "N-days"
      key prefix, preventing inconsistencies.

    Expired items are not automatically deleted by this backend.
    """

    def _s3_compatible_key_func(
        self, key: str, key_prefix: str, version: int | None
    ) -> str:
        """
        Constructs an S3-compatible cache key by applying versioning and a key prefix.
        """
        # Apply versioning to the key if a version is provided.
        _key = f"{key}_{version}" if version else key

        # Prepend the global key prefix if it exists.
        # This creates a directory-like structure in S3.
        return f"{key_prefix}/{_key}" if key_prefix else _key

    def __init__(self, bucket, params):
        super().__init__(params)
        self.bucket_name = bucket
        self.key_func = self._s3_compatible_key_func

        self.client = boto3.client("s3")
        # Use Session-based authentication to mitigate auth latency
        self.client.create_session(Bucket=self.bucket_name)

    def make_key(self, key, version=None):
        """
        Generates directory-like keys for storage in S3.
        """
        _key = turn_key_into_directory_path(key)
        return super().make_key(_key, version)

    def get_backend_timeout(self, timeout=DEFAULT_TIMEOUT):
        """
        Return the timeout value usable by this backend based upon the provided
        timeout.
        """
        if timeout == DEFAULT_TIMEOUT:
            timeout = self.default_timeout
        # The key will be made persistent if None used as a timeout.
        # Non-positive values will cause the key to be deleted.
        return None if timeout is None else max(0, int(timeout))

    def set(self, key, value, timeout=DEFAULT_TIMEOUT, version=None):
        """
        Set a value in the cache. If timeout is given, use that timeout for the
        key; otherwise use the default cache timeout.

        The value is serialized using pickle and stored along with its
        expiration time.

        Raises:
            ValueError: If the provided 'timeout' in days exceeds the maximum
                        lifespan implied by the key's time-based prefix
                        (e.g., trying to set a 10-day timeout on a '7-days:'
                        key)
        """
        key = self.make_and_validate_key(key, version=version)

        timeout = self.get_backend_timeout(timeout)
        # Validate timeout against key's time prefix for non-persistent items
        if timeout is not None:
            key_time_prefix = parse_time_base_prefix(key)
            timeout_in_days = timeout // (24 * 60 * 60)
            if timeout_in_days > key_time_prefix:
                raise ValueError(
                    "The timeout must be less than or equal to the key's time prefix."
                )

        expiration_time = time.time_ns() + timeout * 1e9 if timeout else 0

        content = struct.pack("d", expiration_time) + pickle.dumps(value)
        self.client.put_object(Bucket=self.bucket_name, Key=key, Body=content)

    def has_key(self, raw_key, version=None):
        """
        Return True if the key is in the cache and has not expired.
        """
        key = self.make_key(raw_key, version)
        try:
            response = self.client.get_object(Bucket=self.bucket_name, Key=key)
        except self.client.exceptions.NoSuchKey:
            return False

        expiration_timestamp = struct.unpack(
            "d", response["Body"].read(amt=8)
        )[0]
        # If expiration_timestamp is 0, it's a persistent object.
        if not expiration_timestamp:
            return True
        return expiration_timestamp > datetime.now().timestamp()

    def add(self, raw_key, value, timeout=None, version=None):
        """
        Adds a new item to the cache if it doesn't already exist.
        """
        if self.has_key(raw_key, version=version):
            return False
        self.set(raw_key, value, timeout, version)
        return True

    def get(self, raw_key, default=None, version=None):
        """
        Retrieves an item from the cache, returning a default
        if expired or not found.
        """
        key = self.make_key(raw_key, version)
        try:
            response = self.client.get_object(Bucket=self.bucket_name, Key=key)
        except self.client.exceptions.NoSuchKey:
            return default

        # Initialize a bytearray to store the cached object's content after
        # stripping the expiration timestamp.
        cached_object = bytearray()

        # Iterate over chunks of the S3 object's body.
        # The first 8 bytes (chunk_size=8) are expected to be the expiration timestamp.
        for i, chunk in enumerate(response["Body"].iter_chunks(chunk_size=8)):
            if not i:
                # For the first chunk, unpack the 8 bytes to get the expiration
                # timestamp.
                expiration_timestamp = struct.unpack("d", chunk)[0]
                # If expiration_timestamp is 0, it's a persistent object.
                if not expiration_timestamp:
                    continue
                # If the current time is past the expiration, the item is expired,
                # so return the default value.
                if datetime.now().timestamp() > expiration_timestamp:
                    return default
                # Continue to the next chunk (which will be the actual data)
                continue
            cached_object.extend(chunk)

        # After processing all chunks, if cached_object is empty, it means
        # there was no actual cached data (only an expiration timestamp or an
        # empty object). In this case, return the default value.
        if not cached_object:
            return default

        # If cached_object contains data, unpickle it to reconstruct the
        # original value and return it.
        return pickle.loads(bytes(cached_object))

    def delete(self, raw_key, version=None):
        """
        Removes an item from S3 bucket.
        """
        key = self.make_key(raw_key, version)
        self.client.delete_object(Bucket=self.bucket_name, Key=key)
        return True
