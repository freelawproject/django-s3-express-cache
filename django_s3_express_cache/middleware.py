import time

from django.conf import settings
from django.core.cache import caches
from django.middleware.cache import CacheMiddleware

from django.utils.cache import _generate_cache_key  # type: ignore[attr-defined] # isort: skip
from django.utils.cache import _generate_cache_header_key  # type: ignore[attr-defined] # isort: skip
from django.core.cache import BaseCache
from django.http import HttpRequest, HttpResponse
from django.utils.cache import (
    cc_delim_re,
    get_cache_key,
    get_max_age,
    has_vary_header,
    learn_cache_key,
    patch_response_headers,
)
from django.utils.http import parse_http_date_safe

from django_s3_express_cache import S3ExpressCacheBackend


def _generate_cache_key_s3_compatible(
    request: HttpRequest,
    method: str,
    headerlist: list[str],
    key_prefix: str,
    time_based_prefix: str,
) -> str:
    """
    Generate a cache key compatible with S3ExpressCacheBackend.

    Unlike Django's default `_generate_cache_key`, this function:
      • Places `time_based_prefix` at the start of the key for proper
        time-based directory structuring in S3.
      • Replaces all '/' characters with '.' to avoid creating nested
        S3 folders.
      • Preserves the rest of Django's key generation logic.
    """
    raw_key = f"{time_based_prefix}:" + _generate_cache_key(
        request, method, headerlist, key_prefix
    )
    return raw_key.replace("/", ".")


def _generate_cache_header_key_s3_compatible(
    key_prefix: str,
    request: HttpRequest,
    time_based_prefix: str,
) -> str:
    """
    Generate an S3-compatible cache key for storing the Vary header list.

    This ensures the key:
      • Begins with `time_based_prefix` for time-based cache segregation.
      • Replaces '/' with '.' to prevent nested folders in S3.
      • Remains deterministic based on the request URL.
    """
    raw_key = f"{time_based_prefix}:" + _generate_cache_header_key(
        key_prefix, request
    )
    return raw_key.replace("/", ".")


def get_cache_key_s3_compatible(
    request: HttpRequest,
    key_prefix: str | None = None,
    method: str = "GET",
    cache: BaseCache | None = None,
    time_based_prefix: str | None = None,
):
    """
    Mirrors Django's `get_cache_key` but generates a key compatible with
    S3ExpressCacheBackend by:

      • placing time_based_prefix at the beginning of the key
      • replacing '/' with '.' to avoid creating nested S3 folders

    If there isn't a headerlist stored, return None, indicating that the page
    needs to be rebuilt.
    """
    key_prefix = key_prefix or settings.CACHE_MIDDLEWARE_KEY_PREFIX
    cache = cache or caches[settings.CACHE_MIDDLEWARE_ALIAS]

    header_key = _generate_cache_header_key_s3_compatible(
        key_prefix, request, time_based_prefix
    )
    headerlist = cache.get(header_key)
    if headerlist is None:
        return None

    return _generate_cache_key_s3_compatible(
        request, method, headerlist, key_prefix
    )


def learn_cache_key_s3_compatible(
    request: HttpRequest,
    response: HttpResponse,
    cache_timeout: int | None = None,
    key_prefix: str | None = None,
    cache: BaseCache | None = None,
    time_based_prefix: str | None = None,
):
    """
    Store the list of headers from the response's Vary header and generate
    an S3-compatible cache key.

    This mirrors Django's `learn_cache_key` but ensures keys are compatible
    with S3ExpressCacheBackend:

      • time_based_prefix appears at the start of the key.
      • '/' characters are replaced with '.' to prevent nested folder.
      • all other behavior (Vary handling, sorting, i18n suffix) remains
        the same as Django
    """
    key_prefix = key_prefix or settings.CACHE_MIDDLEWARE_KEY_PREFIX
    cache_timeout = cache_timeout or settings.CACHE_MIDDLEWARE_SECONDS
    cache = cache or caches[settings.CACHE_MIDDLEWARE_ALIAS]

    cache_key = _generate_cache_header_key_s3_compatible(
        key_prefix, request, time_based_prefix
    )
    headerlist = []
    if response.has_header("Vary"):
        is_accept_language_redundant = settings.USE_I18N
        for header in cc_delim_re.split(response.headers["Vary"]):
            header = header.upper().replace("-", "_")
            if header != "ACCEPT_LANGUAGE" or not is_accept_language_redundant:
                headerlist.append("HTTP_" + header)
        headerlist.sort()

    # if there is no Vary header, we still need a cache key
    # for the request.build_absolute_uri()
    cache.set(cache_key, headerlist, cache_timeout)
    return _generate_cache_key_s3_compatible(
        request, request.method, headerlist, key_prefix, time_based_prefix
    )


class CacheMiddlewareS3Compatible(CacheMiddleware):
    """
    Drop-in replacement for Django's CacheMiddleware that produces
    cache keys compatible with S3ExpressCacheBackend.

    This middleware automatically switches to S3-compatible key generation
    when using `S3ExpressCacheBackend`.

    For non-S3 backends, the middleware falls back to Django’s standard
    cache key generation.
    """

    def __init__(
        self, get_response, cache_timeout=None, page_timeout=None, **kwargs
    ):
        # Extract and remove custom kwarg before calling super()
        self.time_based_prefix = kwargs.pop("time_based_prefix", None)
        super().__init__(get_response, cache_timeout, page_timeout, **kwargs)
        self._is_s3_backend = isinstance(self.cache, S3ExpressCacheBackend)
        if self._is_s3_backend and not self.time_based_prefix:
            raise ValueError(
                "CacheMiddlewareS3Compatible requires 'time_based_prefix' "
                "when used with S3ExpressCacheBackend."
            )

    @property
    def _get_cache_key_func(self):
        return (
            get_cache_key_s3_compatible
            if self._is_s3_backend
            else get_cache_key
        )

    @property
    def _learn_cache_key_func(self):
        return (
            learn_cache_key_s3_compatible
            if self._is_s3_backend
            else learn_cache_key
        )

    def process_request(self, request):
        if request.method not in ("GET", "HEAD"):
            request._cache_update_cache = False
            return None  # Don't bother checking the cache.

        # Use the appropriate cache key generator based on backend:
        #   • S3 backend -> use S3-compatible helpers.
        #   • default backend -> use Django's standard helpers.
        key_func = self._get_cache_key_func
        cache_key = key_func(request, self.key_prefix, "GET", cache=self.cache)
        if cache_key is None:
            # No cache information available, need to rebuild.
            request._cache_update_cache = True
            return None

        response = self.cache.get(cache_key)
        # if it wasn't found and we are looking for a HEAD, try looking just for that
        if response is None and request.method == "HEAD":
            cache_key = key_func(
                request, self.key_prefix, "HEAD", cache=self.cache
            )
            response = self.cache.get(cache_key)

        if response is None:
            # No cache information available, need to rebuild.
            request._cache_update_cache = True
            return None

        # Derive the age estimation of the cached response.
        max_age_seconds = get_max_age(response)
        expires_timestamp = parse_http_date_safe(response.get("Expires"))

        if max_age_seconds is not None and expires_timestamp is not None:
            now_timestamp = int(time.time())
            remaining_seconds = expires_timestamp - now_timestamp
            # Use Age: 0 if local clock got turned back.
            response["Age"] = max(0, max_age_seconds - remaining_seconds)

        # hit, return cached response
        request._cache_update_cache = False
        return response

    def process_response(self, request, response):
        """Store the response in cache when appropriate."""
        if not self._should_update_cache(request, response):
            # We don't need to update the cache, just return.
            return response

        if response.streaming or response.status_code not in (200, 304):
            return response

        # Don't cache responses that set a user-specific (and maybe security
        # sensitive) cookie in response to a cookie-less request.
        if (
            not request.COOKIES
            and response.cookies
            and has_vary_header(response, "Cookie")
        ):
            return response

        # Don't cache a response with 'Cache-Control: private'
        if "private" in response.get("Cache-Control", ()):
            return response

        # Page timeout takes precedence over the "max-age" and the default
        # cache timeout.
        timeout = (
            self.page_timeout or get_max_age(response) or self.cache_timeout
        )
        if timeout == 0:
            return response

        patch_response_headers(response, timeout)

        # Store or learn the cache key using the correct backend function.
        learn_func = self._learn_cache_key_func
        cache_key = learn_func(
            request, response, timeout, self.key_prefix, cache=self.cache
        )
        if timeout and response.status_code == 200:
            if hasattr(response, "render") and callable(response.render):
                response.add_post_render_callback(
                    lambda r: self.cache.set(cache_key, r, timeout)
                )
            else:
                self.cache.set(cache_key, response, timeout)
        return response
