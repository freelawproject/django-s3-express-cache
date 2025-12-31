# Change Log

## Future

The following changes are not yet released, but are code complete:

### Features
None yet

### Changes
None yet

### Fixes
None yet

## Current

**0.2.0 - 2025-12-31**

- Adds `CacheMiddlewareS3Compatible`, a drop-in replacement for Django's `CacheMiddleware` that generates S3-compatible cache keys
- Adds `cache_page` decorator that wraps views with CacheMiddlewareS3Compatible for S3ExpressCacheBackend support.

## Past

**0.1.0 - 2025-09-21**

- First public release of the package.
- Provides core functionality
- Adds automated publishing to PyPI via GitHub Actions.
- Adds workflow to enforce changelog updates on pull requests.
