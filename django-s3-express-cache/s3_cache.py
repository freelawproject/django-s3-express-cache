import boto3
from django.core.cache.backends.base import BaseCache


class S3ExpressCacheBackend(BaseCache):
    def __init__(self, bucket, params):
        super().__init__(params)
        self.bucket_name = bucket
        self.client = boto3.client("s3")
