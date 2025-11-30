from django.utils.decorators import decorator_from_middleware_with_args

from django_s3_express_cache.middleware import CacheMiddlewareS3Compatible


def cache_page(timeout, *, cache=None, key_prefix=None):
    """
    Decorator for views that caches the response using `CacheMiddlewareS3Compatible`.

    Works like Django's built-in `cache_page` decorator but generates cache keys
    compatible with `S3ExpressCacheBackend`.
    """
    return decorator_from_middleware_with_args(CacheMiddlewareS3Compatible)(
        page_timeout=timeout,
        cache_alias=cache,
        key_prefix=key_prefix,
    )
