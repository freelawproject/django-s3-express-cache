# S3 Express Cache Backend for Django

An open source Django cache backend that stores entries in Amazon S3 Express One Zone, using a compact binary header with room for versioning, expiration, and optional compression.

Further development is planned, and contributions of all kinds (bug reports, improvements, and new features) are very welcome.

## Background

This backend was built to address performance limitations with Django’s database cache. It represents a shift toward a faster, more scalable, and more flexible cache implementation, which we believe to be the right fit for large-scale applications.

This backend was inspired by an issue raised in [CourtListener’s repository](https://github.com/freelawproject/courtlistener/issues/5304). In short:

- Django’s DB cache can become a performance bottleneck under heavy load, especially when culling expired rows. Queries like `SELECT COUNT(*) FROM django_cache` caused significant slowdowns once the cache table grows large.

- S3 is highly scalable, cost-effective, and capable of storing very large objects. Instead of relying on costly culling queries, we can use S3 lifecycle rules to automatically clean up stale entries, keeping performance stable without scripts or app-level logic.

This implementation builds on those ideas and delivers a production-ready, efficient, and extensible cache backend, designed to integrate naturally with Django’s caching framework.

## Design overview

### Serialization

This backend uses Python’s `pickle` with `HIGHEST_PROTOCOL`, providing fast serialization and broad support for Python object types.

- **Why pickle?**

  Django’s own [file-based](https://github.com/django/django/blob/8b229b4dbb6db08428348aeb7e5a536b64cf8ed8/django/core/cache/backends/filebased.py) and [database-backed](https://github.com/django/django/blob/8b229b4dbb6db08428348aeb7e5a536b64cf8ed8/django/core/cache/backends/db.py) cache backends both rely on pickle internally, each with their own write method. We chose to follow this pattern for consistency, compatibility, and flexibility—especially since our goal was a backend as capable as Django’s built-ins.

- **Why not JSON or other formats?**

  Alternatives like JSON (and faster variants such as [orjson](https://github.com/ijl/orjson) or [ujson](https://github.com/ultrajson/ultrajson)) are safer but limited to basic types. This prevents caching complex objects like templates or query results, which are common use cases for Django’s cache system. We also tested [msgpack](https://github.com/msgpack/msgpack-python), which offers more flexibility, but it failed to serialize some of the objects we needed.

- **Future extensibility**

  The cache header includes a compression_type field, leaving room for transparent compression (e.g., zlib, zstd) in future versions without breaking compatibility.

> [!CAUTION]
> Pickle should only be used with trusted data that your own application writes and reads. Never unpickle untrusted payloads. If your use case requires stricter, data-only serialization, formats like JSON or MessagePack are safer but keep in mind their type limitations.

### Header format (fixed-width, versioned)

We prepend a compact header to every object. Current layout (struct format: dHHQ):

| Field           | Type | Bytes | Notes                                                             |
|-----------------|------|-------|-------------------------------------------------------------------|
| expiration_time | d    | 8     | UNIX timestamp in seconds (float). `0` means persistent.           |
| header_version  | H    | 2     | Starts at `1`. Used for compatibility checks.                     |
| compression_type| H    | 2     | `0 = none`. Reserved for future use (e.g., zlib, zstd).           |
| extra (reserved)| Q    | 8     | Reserved for future metadata                                      |

Why fixed-width? It lets us Range-read only the header to early-decide on expiry/version without downloading the body.

> [!NOTE]
> The code is written to treat mismatched versions as unsupported (safe default). You can add backward parsers in the future if needed.


### Key design for S3 throughput

- S3 Express One Zone uses [directory buckets](https://docs.aws.amazon.com/AmazonS3/latest/userguide/directory-buckets-overview.html), which support [Lifecycle policies](https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-lifecycle-mgmt.html) but only with limited filters (prefix and size, no tags). To align with these constraints, our design requires explicit time-based key prefixes (e.g., `1-days/`, `7-days/`, `30-days/`). This ensures cache entries can be expired automatically using prefix-based lifecycle rules.

- Keys of the form `N-days:actual_key` are rewritten to `N-days/actual_key`. This spreads objects across prefixes, improving S3 partitioning and request throughput.

- The cache class validates timeouts against the chosen prefix. If a timeout exceeds the `N-days` limit, the write is rejected.  This prevents accidentally storing long-lived items under a short-lived namespace and keeps lifecycle cleanup predictable.

### Early Exits (“Early Abortions”)

To minimize unnecessary data transfer and improve performance, the backend implements early exits:

- **`has_key`**:  
  Uses an S3 `Range` request to fetch only the header bytes.  
  - If the item is expired → treated as a cache miss without downloading the full value.  
  - If the item is persistent or still valid → considered a hit.

- **`get`**:  
  Streams the object in header-sized chunks.  
  After reading the header (first chunk), expiry is evaluated.  
  - If expired → the operation exits immediately without fetching the remaining data.  
  - If valid → streaming continues to reconstruct the cached object.

### Lazy boto3 Client Initialization

Creating a boto3 client (and even importing boto3 itself) can be relatively expensive. To avoid adding this overhead to Django’s general startup time, the backend initializes the client **lazily** using a `@cached_property`.  

This means:
- The boto3 client is created only on first use.  
- Subsequent accesses reuse the cached client instance.  
- Application startup remains fast, while still ensuring efficient reuse of the client once it’s needed.

## Why this backend?

- **Faster reads & fewer bytes:** supports header-only Range reads to detect expiry and skip downloading the whole object on misses.

- **Simpler large-scale cleanup:** delegates stale object removal to S3 lifecycle rules, minimizing application-level logic.

- **Future-proof format:** compact binary header with versioning, reserved fields, and support for optional compression in future updates.

- **Optimized key distribution:** time-based key prefixes spread objects across S3 prefixes and align directly with lifecycle policies.

## Trade-offs

- **S3 Express specifics:** biggest wins come if you can use S3 Express One Zone (directory buckets); Lifecycle rules in directory buckets are prefix-based only, so prefixes must be carefully planned.

- **Lifecycle rule setup:** initial setup requires scripts to create rules, introducing a small implementation overhead. Once configured, cleanup is automatic, but planning and provisioning are required upfront.

- The backend does not currently implement touch or clear methods.

## Installation

Installing django-s3-express-cache is easy.

```sh
pip install django-s3-express-cache
```

Or install the latest dev version from github

```sh
pip install git+https://github.com/freelawproject/django-s3-express-cache.git@master
```


## Configuration

In your Django settings, define the CACHES and point to the S3 backend class. It can be used as the only cache or alongside other cache backends.

```python
CACHES = {
    "default": {
        "BACKEND": "django-s3-express-cache.S3ExpressCacheBackend",
        "LOCATION": "S3_CACHE_BUCKET_NAME",
        "OPTIONS": {
            "HEADER_VERSION": 1,
        }
    }
}
```

### Using S3 Express Cache Alongside Other Caches

```python
CACHES = {
    "default": {
        "BACKEND": "django_redis.cache.RedisCache",
        "LOCATION": REDIS_URL,
    },
    "s3": {
        "BACKEND": "django-s3-express-cache.S3ExpressCacheBackend",
        "LOCATION": S3_CACHE_BUCKET_NAME,
    }
}
```

### Lifecycle rules

Create lifecycle rules to auto-expire objects by prefix. For example:
- Objects under 7-days/ expire after 7 days
- Objects under 30-days/ expire after 30 days

```json
{
  "Rules": [
    {
      "ID": "Expire-7-days-prefix",
      "Filter": { "Prefix": "7-days/" },
      "Status": "Enabled",
      "Expiration": { "Days": 7 }
    },
    {
      "ID": "Expire-30-days-prefix",
      "Filter": { "Prefix": "30-days/" },
      "Status": "Enabled",
      "Expiration": { "Days": 30 }
    }
  ]
}
```

These lifecycle rules complement the cache’s in-object header expiration. The header allows our implementation to short-circuit reads (treating expired items as misses), while S3 lifecycle policies ensure expired data is eventually deleted from the bucket.

The following script demonstrates how to configure up to 1,000 lifecycle rules in a bucket.
To run it, your IAM must have at least the following permissions:

- `s3:PutLifecycleConfiguratio`
- `s3:GetLifecycleConfiguration`


```python
import boto3

# Replace with your bucket name
BUCKET_NAME = "your-bucket-name"

s3 = boto3.client("s3")

rules = []
for i in range(1, 1000):
    # Handle pluralization
    suffix = "days" if i > 1 else "day"
    prefix = f"{i}-{suffix}"

    rules.append({
        "ID": f"expire-{i}-{suffix}",
        "Filter": {"Prefix": prefix},
        "Status": "Enabled",
        "Expiration": {"Days": i},
    })

lifecycle_config = {"Rules": rules}

response = s3.put_bucket_lifecycle_configuration(
    Bucket=BUCKET_NAME,
    LifecycleConfiguration=lifecycle_config
)
```

## License

This repository is available under the permissive BSD license, making it easy and safe to incorporate in your own libraries.

Pull and feature requests welcome. Online editing in GitHub is possible (and easy!)
