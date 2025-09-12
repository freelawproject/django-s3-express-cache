import pickle
import re
import struct
import time
from typing import Any

from django.core.cache.backends.base import DEFAULT_TIMEOUT, BaseCache
from django.utils.functional import cached_property


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
    pattern_with_colon = r"^(\d+-days?):(.*)$"

    # Attempt to match the pattern at the beginning of the S3 object key.
    match = re.match(pattern_with_colon, key)
    if not match:
        return key
    # If a match is found, extract the prefix and the rest of the key,
    # then reformat with a slash for S3 partitioning.
    return f"{match.group(1)}/{match.group(2)}"


def parse_time_base_prefix(
    key: str, key_prefix: str = "", is_persistent_object: bool = False
) -> int | None:
    """
    Parses the numeric time component (days) from the cache key's prefix.

    This method expects the key to start with a time-based prefix in the
    format "N-day(s):" or "N-day(s)/" (optionally preceded by `key_prefix`).
    It extracts the integer value 'N' from this prefix. This numeric value
    represents the maximum lifespan (in days) of the cached item.

    Args:
        key (str): The cache key string.
        key_prefix (str, optional): An optional prefix to strip before
            parsing. Defaults to "".
        is_persistent_object (bool, optional): If True, the key is expected
            to represent a persistent object (no expiration). In this case,
            using a time-based prefix is invalid and will raise a ValueError.
            Defaults to False.

    Raises:
        ValueError:
            - If a persistent object is configured with a time-based prefix.
            - If the key does not conform to the expected time-based prefix
            format (e.g., "N-day(s):" or "N-day(s)/").

    Returns:
        int: The integer value representing the number of days from the
            prefix.
    """
    _key = re.sub(f"{key_prefix}/", "", key) if key_prefix else key

    pattern = r"^(\d+)-days?[:/](.*)$"

    match = re.match(pattern, _key)

    if is_persistent_object:
        if match:
            raise ValueError("Persistent keys cannot use a time-based prefix")
        return None

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

    HEADER_FORMAT = "QHHQ"
    # Q: unsigned long long (8 bytes) expiration time in nano seconds
    # H: unsigned short (2 bytes) format version
    # H: unsigned short (2 byte) compression type
    # Q: unsigned long long (8 bytes) reserved/extra space

    pickle_protocol = pickle.HIGHEST_PROTOCOL

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

    @cached_property
    def client(self):
        import boto3

        return boto3.client("s3")

    def __init__(self, bucket: str, params: dict[str, Any]):
        super().__init__(params)
        self.bucket_name = bucket
        self.key_func = self._s3_compatible_key_func

        options = params.get("OPTIONS", {})
        self.header_version = params.get(
            "HEADER_VERSION", options.get("HEADER_VERSION", 1)
        )
        self.compression_type = params.get(
            "COMPRESSION_TYPE", options.get("COMPRESSION_TYPE", 0)
        )
        # Use Session-based authentication to mitigate auth latency
        self.client.create_session(Bucket=self.bucket_name)

    @property
    def _get_header_size(self) -> int:
        return struct.calcsize(self.HEADER_FORMAT)

    def make_header(self, expiration_time: int) -> bytes:
        """
        Build a binary header for cache storage.

        Layout:
        - Bytes 0-7   : expiration time (int, seconds since epoch in ns)
        - Byte 8-9    : header format version
        - Bytes 10-11 : compression type (0 = none, 1 = zlib, etc.)
        - Bytes 12-20 : reserved extra space (8 bytes)
        """
        return struct.pack(
            self.HEADER_FORMAT,
            expiration_time,
            self.header_version,
            self.compression_type,
            0,
        )

    def parse_header(self, header_bytes: bytes) -> tuple[int, ...]:
        """
        Unpack a binary header into its fields.

        Returns: (expiration_time, version, compression, extra_bytes)
        """
        return struct.unpack(self.HEADER_FORMAT, header_bytes)

    def make_key(self, key: str, version: int | None = None) -> str:
        """
        Generates directory-like keys for storage in S3.
        """
        _key = turn_key_into_directory_path(key)
        return super().make_key(_key, version)

    def get_backend_timeout(
        self, timeout: int | None = DEFAULT_TIMEOUT
    ) -> int | None:
        """
        Return the timeout value usable by this backend based upon the provided
        timeout.
        """
        if timeout == DEFAULT_TIMEOUT:
            timeout = self.default_timeout
        # The key will be made persistent if None used as a timeout.
        # Non-positive values will cause the key to be deleted.
        return None if timeout is None else max(0, int(timeout))

    def set(
        self,
        key: str,
        value: Any,
        timeout: int | None = DEFAULT_TIMEOUT,
        version: int | None = None,
    ) -> None:
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

        # Skip caching if timeout == 0
        if timeout == 0:
            return

        # Persistent objects are represented with timeout=None
        is_persistent_object = timeout is None

        # Parse and validate any time-based prefix in the key
        key_time_prefix = parse_time_base_prefix(
            key, self.key_prefix, is_persistent_object
        )

        # Validate timeout against key's time prefix for non-persistent items
        if not is_persistent_object:
            timeout_in_days = timeout // (24 * 60 * 60)
            if timeout_in_days > key_time_prefix:
                raise ValueError(
                    "The timeout must be less than or equal to the key's time prefix."
                )

        expiration_time = (
            int(time.time_ns() + timeout * 1e9)
            if not is_persistent_object
            else 0
        )

        # Pickle data
        serialized_data = pickle.dumps(value, self.pickle_protocol)

        # Pack header
        header = self.make_header(expiration_time)

        content = header + serialized_data
        self.client.put_object(Bucket=self.bucket_name, Key=key, Body=content)

    def has_key(self, raw_key: str, version: int | None = None) -> bool:
        """
        Return True if the key is in the cache and has not expired.
        """
        key = self.make_key(raw_key, version)
        try:
            # Request only the expiration timestamp bytes
            response = self.client.get_object(
                Bucket=self.bucket_name,
                Key=key,
                Range=f"bytes=0-{self._get_header_size - 1}",
            )
        except self.client.exceptions.NoSuchKey:
            return False

        expiration_timestamp, version, *_ = self.parse_header(
            response["Body"].read()
        )

        if version != self.header_version:
            raise ValueError(f"Unsupported cache entry version: {version}")

        # If expiration_timestamp is 0, it's a persistent object.
        if not expiration_timestamp:
            return True
        return expiration_timestamp > time.time_ns()

    def add(
        self,
        raw_key: str,
        value: Any,
        timeout: int | None = None,
        version: int | None = None,
    ) -> bool:
        """
        Adds a new item to the cache if it doesn't already exist.
        """
        if self.has_key(raw_key, version=version):
            return False
        self.set(raw_key, value, timeout, version)
        return True

    def get(
        self,
        raw_key: str,
        default: Any | None = None,
        version: int | None = None,
    ) -> Any:
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
        for i, chunk in enumerate(
            response["Body"].iter_chunks(chunk_size=self._get_header_size)
        ):
            if i == 0:
                # For the first chunk, unpack the header to get the expiration
                # timestamp.
                expiration_timestamp, version, *_ = self.parse_header(chunk)
                # If expiration_timestamp is 0, it's a persistent object.
                if not expiration_timestamp:
                    continue
                # If the current time is past the expiration, the item is expired,
                # so return the default value.
                if time.time_ns() > expiration_timestamp:
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

    def delete(self, raw_key: str, version: int | None = None) -> bool:
        """
        Removes an item from S3 bucket.
        """
        key = self.make_key(raw_key, version)

        try:
            self.client.head_object(Bucket=self.bucket_name, Key=key)
        except self.client.exceptions.NoSuchKey:
            return False

        self.client.delete_object(Bucket=self.bucket_name, Key=key)
        return True
