import pickle
import struct
import time

import boto3
from django.core.cache.backends.base import DEFAULT_TIMEOUT, BaseCache


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
