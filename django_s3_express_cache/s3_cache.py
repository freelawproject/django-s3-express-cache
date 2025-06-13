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


class S3ExpressCacheBackend(BaseCache):
    def __init__(self, bucket, params):
        super().__init__(params)
        self.bucket_name = bucket
        self.client = boto3.client("s3")

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
        """
        key = self.make_and_validate_key(key, version=version)

        timeout = self.get_backend_timeout(timeout)
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
