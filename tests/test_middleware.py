from unittest.mock import MagicMock, patch

from django.conf import settings
from django.http import HttpResponse
from django.test import RequestFactory, SimpleTestCase, override_settings

from django_s3_express_cache import S3ExpressCacheBackend
from django_s3_express_cache.decorator import cache_page
from django_s3_express_cache.middleware import (
    CacheMiddlewareS3Compatible,
    get_cache_key_s3_compatible,
    learn_cache_key_s3_compatible,
)


def setup_mock_s3():
    """Return a patched mock boto3 client and start patching."""
    mock_client = MagicMock()
    mock_client.exceptions.NoSuchKey = type("NoSuchKey", (Exception,), {})

    patcher = patch("boto3.client", return_value=mock_client)
    patcher.start()

    return mock_client, patcher


@override_settings(
    CACHES={
        **settings.CACHES,
        "s3": {
            "BACKEND": "django_s3_express_cache.S3ExpressCacheBackend",
            "LOCATION": "test-s3-express-bucket",
        },
    }
)
class MiddlewareTest(SimpleTestCase):
    def setUp(self):
        self.factory = RequestFactory()
        self.mock_s3_client, self.boto3_patcher = setup_mock_s3()
        self.cache = S3ExpressCacheBackend(
            "test-s3-express-bucket",
            {"LOCATION": "test-s3-express-bucket", "TIMEOUT": 300},
        )

    def tearDown(self):
        self.boto3_patcher.stop()

    @patch("django_s3_express_cache.middleware.S3ExpressCacheBackend.set")
    @patch("django_s3_express_cache.middleware.S3ExpressCacheBackend.get")
    def test_can_generate_s3_compatible_key(
        self, mock_cache_get, mock_cache_set
    ):
        """Verify S3-compatible cache keys are generated correctly."""
        request = self.factory.get(path="/some-url/")
        key_prefix = "1-days"

        # Learn the cache key for the first time (triggers cache set)
        response = HttpResponse("Test")
        cache_key = learn_cache_key_s3_compatible(
            request,
            response,
            cache_timeout=60,
            key_prefix=key_prefix,
            cache=self.cache,
        )

        # Ensure cache set was called to store the key
        mock_cache_set.assert_called_once()

        # Key should start with the specified prefix
        self.assertTrue(cache_key.startswith(key_prefix))

        # Key should not contain slashes
        self.assertNotIn("/", cache_key)

        # Mock cache get to simulate fetching stored headers
        mock_cache_get.return_value = []
        fetched_key = get_cache_key_s3_compatible(
            request, key_prefix=key_prefix, cache=self.cache
        )

        # Ensure fetched key matches the learned key
        self.assertEqual(fetched_key, cache_key)

    @patch("django_s3_express_cache.middleware.S3ExpressCacheBackend.get")
    @patch(
        "django_s3_express_cache.middleware.get_cache_key_s3_compatible",
        wraps=get_cache_key_s3_compatible,
    )
    def test_cache_middleware_s3compatible_hits_cache(
        self, mock_get_cache_key, mock_cache_get
    ):
        """Check that CacheMiddlewareS3Compatible calls the S3 backend"""

        # Dummy view for middleware testing
        def dummy_view(request):
            return HttpResponse("Hello, World!")

        request = self.factory.get("/cached-url/")
        middleware = CacheMiddlewareS3Compatible(
            dummy_view, cache_timeout=60, cache_alias="s3"
        )

        # Simulate cache miss on first call
        mock_cache_get.return_value = None

        # Process request through middleware (should attempt cache fetch)
        response1 = middleware.process_request(request)
        # No response returned, cache miss
        self.assertEqual(response1, None)

        # Ensure S3 key function and cache get were called
        mock_get_cache_key.assert_called_once()
        mock_cache_get.assert_called_once()

    @patch("django_s3_express_cache.middleware.S3ExpressCacheBackend.get")
    @patch(
        "django_s3_express_cache.middleware.get_cache_key_s3_compatible",
        wraps=get_cache_key_s3_compatible,
    )
    def test_cache_middleware_s3compatible_can_use_default(
        self, mock_get_cache_key, mock_cache_get
    ):
        """Verify that middleware can use default Django cache"""

        # Dummy view for middleware testing
        def dummy_view(request):
            return HttpResponse("Hello, World!")

        request = self.factory.get("/cached-url/")

        # No cache_alias="s3" → uses default Django backend
        middleware = CacheMiddlewareS3Compatible(dummy_view, cache_timeout=60)

        # Simulate cache miss on first call
        mock_cache_get.return_value = None

        response1 = middleware.process_request(request)
        # Cache miss returns None
        self.assertEqual(response1, None)

        # Ensure S3-specific functions were not called
        mock_get_cache_key.assert_not_called()
        mock_cache_get.assert_not_called()


@override_settings(
    CACHES={
        **settings.CACHES,
        "s3": {
            "BACKEND": "django_s3_express_cache.S3ExpressCacheBackend",
            "LOCATION": "test-s3-express-bucket",
        },
    }
)
class CachePageDecoratorTest(SimpleTestCase):
    def setUp(self):
        self.factory = RequestFactory()
        self.mock_s3_client, self.boto3_patcher = setup_mock_s3()

    def tearDown(self):
        self.boto3_patcher.stop()

    @patch("django_s3_express_cache.middleware.S3ExpressCacheBackend.set")
    @patch("django_s3_express_cache.middleware.S3ExpressCacheBackend.get")
    def test_cache_page_decorator(self, mock_cache_get, mock_cache_set):
        """Ensure cache_page decorator interacts with S3ExpressCacheBackend correctly."""

        # Define a simple view wrapped with the cache_page decorator
        @cache_page(
            60 * 15,  # 15-minute cache timeout
            cache="s3",  # Use S3 backend
            key_prefix="1-days",  # S3-compatible cache key prefix
        )
        def my_view(request):
            return HttpResponse("Hello, World!")

        # Simulate cache miss
        mock_cache_get.return_value = None

        # Create a test GET request
        request = self.factory.get(path="/some-path/")

        # Call the decorated view (triggers caching)
        response = my_view(request)

        # Verify the response is correct
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.content, b"Hello, World!")

        # Verify cache get was called to check for an existing cached response
        mock_cache_get.assert_called_once()

        # Verify cache set was called to store the response
        mock_cache_set.assert_called()
