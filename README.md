# S3 Express Cache Backend for Django

A scalable, open source Django cache backend powered by [Amazon S3 Express One Zone](1z) — cheaper, durable, and ready for production.

## Why

Django ships with two main distributed cache backends, but neither is a great fit for many or large objects:

| Backend            | Pros                                    | Cons                                                                 |
|--------------------|-----------------------------------------|----------------------------------------------------------------------|
| **Database cache** | Easy to set up                          | Does a `COUNT(*)` on every `get`, `set`, or `touch`, which [does not perform][1] on large cache tables. |
| **Redis/Memcached**| Fast, widely used                       | Expensive to run at scale (large RAM bills, cluster management)      |

On the other hand, [S3 Express One Zone][1z] provides an S3 bucket with single-digit-millisecond latency that is cheap, durable, and can scale to millions of objects, large and small.

S3 Express does not support automatic item expiration, so we use [S3 lifecycle rules][lifecycle], a fixed-width header prepended to each item, and clever key names to manage and cull the cache as needed.

## Features

- **Scalable & cost-effective** - cache huge datasets without memory overhead. By using S3, you can scale to virtually unlimited capacity at a fraction of the cost.

- **Simpler large-scale cleanup** - delegates stale object removal to [S3 Lifecycle Rules][lifecycle], minimizing application-level logic.

- **Faster reads & fewer bytes** - supports header-only [range requests][rr] to detect expiry and skip downloading full objects on misses.

- **Future-proof format** - compact binary header with versioning and reserved fields inspired by TCP frames for future functionality..

- **Easy integration** — configure your Django CACHES settings, add the necessary S3 Lifecycle Rules, and you're ready to go.

## Trade-offs

- **S3 Express specifics:** biggest wins come if you can use S3 Express One Zone (directory buckets); Lifecycle rules in directory buckets are prefix-based only, so prefixes must be carefully planned.

- **Lifecycle rule setup:** initial setup requires scripts to create rules, introducing a small implementation overhead. Once configured, cleanup is automatic, but planning and provisioning are required upfront.

## Requirements

- **Django 5.x**  
- **Python ≥ 3.13**  
- **boto3 v1.38.36+**  
- Works in any AWS region where S3 Express One Zone is available
- Best used in the same Availability Zone as your application  

## Design overview


### Motivation

This backend was inspired by an issue raised in [CourtListener’s repository](https://github.com/freelawproject/courtlistener/issues/5304). In short:

- Django’s DB cache [can become a performance bottleneck under heavy load][1], especially when culling expired rows. Queries like `SELECT COUNT(*) FROM django_cache` caused significant slowdowns once the cache table grows large. In our experience running CourtListener, the DB cache is one of the heaviest consumers of database resources.

- Django's in-memory caches do not scale well when caching large objects or many small ones.

- S3 is highly scalable, cost-effective, and capable of storing very large objects. Instead of relying on costly culling queries (like the DB cache), we can use S3 lifecycle rules to automatically clean up stale entries, keeping performance stable without scripts or app-level logic.

This implementation builds on those ideas and delivers a production-ready, efficient, and extensible cache backend, designed to integrate naturally with Django’s caching framework.


### Key design for Maximal S3 Throughput and Automatic Culling

- S3 Express One Zone uses [directory buckets](dirbuck), which support [Lifecycle policies](https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-lifecycle-mgmt.html) but only with limited filters (prefix and size, no tags).

  To work within these constraints, our design relies on explicit time-based key prefixes (e.g., `1-days/`, `7-days/`, `30-days/`) that reflect the expiration period of each item. Expirations are supported for up to 1,000 days, and each cache key must use the prefix corresponding to the next whole day beyond the item’s expiration. For example:

  - An item expiring today should use a key like `1-days:foo`.
  - An item expiring in 25 hours should have a key like `2-days:bar`.

  This approach allows cache entries to be automatically removed using simple prefix-based lifecycle rules.

- Keys of the form `N-days:actual_key` are rewritten to `N-days/actual_key`(with a slash instead of a colon). This spreads objects across S3 key prefixes, improving S3 partitioning and request throughput.

- When adding something to the cache, the key name is validated against the expiration date for the item. If the expiration exceeds the `N-days` limit, the write is rejected. This prevents accidentally storing long-lived items under a short-lived namespace and keeps lifecycle-based culling predictable. Such errors will generally be caught during development.


### Header format (fixed-width, versioned)

We prepend a compact header to every object. Current layout ([struct](https://docs.python.org/3/library/struct.html) format: `QHHQ`):

| Field           | Type | Bytes | Notes                                                             |
|-----------------|------|-------|-------------------------------------------------------------------|
| expiration_time | Q    | 8     | UNIX timestamp in seconds (int). `0` means persistent.           |
| header_version  | H    | 2     | Starts at `1`. Used for compatibility checks.                     |
| compression_type| H    | 2     | `0 = none`. Reserved for future use (e.g., zlib, zstd).           |
| extra (reserved)| Q    | 8     | Reserved for future metadata                                      |

Using a fixed-width header allows the cache to [Range-read](rr) only the header. Items remain in the cache until [S3 Lifecyle rules][lifecycle] complete, so this allows your application to check the expiration of an object before downloading it. If the item is expired, that's a cache miss. If not, the entire object is downloaded and returned.

> [!NOTE]
> The code is written to treat mismatched versions as unsupported (safe default). You can add backward parsers in the future if needed.


### Performance Optimizations

To optimize data transfer and improve performance, the backend implements early exits:

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


### Security

This backend uses Python’s `pickle` with `HIGHEST_PROTOCOL`, providing fast serialization and broad support for Python object types.

- **Why pickle?**

  Django’s own [file-based](https://github.com/django/django/blob/8b229b4dbb6db08428348aeb7e5a536b64cf8ed8/django/core/cache/backends/filebased.py) and [database-backed](https://github.com/django/django/blob/8b229b4dbb6db08428348aeb7e5a536b64cf8ed8/django/core/cache/backends/db.py) cache backends both rely on pickle internally, each with their own write method. We chose to follow this pattern for consistency, compatibility, and flexibility—especially since our goal was a backend as capable as Django’s built-ins.

- **Why not JSON or other formats?**

  Alternatives like JSON (and faster variants such as [orjson](https://github.com/ijl/orjson) or [ujson](https://github.com/ultrajson/ultrajson)) are safer but limited to basic types. This prevents caching complex objects like templates or query results, which are common use cases for Django’s cache system. We also tested [msgpack](https://github.com/msgpack/msgpack-python), which offers more flexibility, but it failed to serialize some of the objects we needed.

> [!CAUTION]
> Pickle should only be used with trusted data that your own application writes and reads. Never unpickle untrusted payloads. If your use case requires stricter, data-only serialization, formats like JSON or MessagePack are safer but keep in mind their type limitations.

---

## Usage

There are five steps to using this cache:

1. Install it

2. Configure it in your django settings

3. Set up the S3 Express bucket

4. Configure lifecycle rules for automatic cache culling

5. Use it!



### Installation

From PyPI:

```sh
pip install django-s3-express-cache
```

From GitHub (latest dev):

```sh
pip install git+https://github.com/freelawproject/django-s3-express-cache.git@master
```


### Configuration

We do not recommend this cache as your primary, default cache. Instead, it should be used as a secondary cache for larger or longer-living objects by putting something like the following in your Django settings:

```python
CACHES = {
    "default": {
        "BACKEND": "django_redis.cache.RedisCache",
        "LOCATION": REDIS_URL,
    },
    "s3": {
        "BACKEND": "django_s3_express_cache.S3ExpressCacheBackend",
        "LOCATION": "S3_CACHE_BUCKET_NAME",
        "OPTIONS": {
            "HEADER_VERSION": 1,
        }
    }
}
```


### Bucket Set Up

You must use an S3 Express One Zone (Directory bucket). Directory bucket names must follow this format and comply with the rules for directory bucket naming:

```bash
bucket-base-name--zone-id--x-s3
```

For example, the following directory bucket name contains the Availability Zone ID usw2-az1:


```bash
bucket-base-name--usw2-az1--x-s3
```

When you create a directory bucket you must also provide configuration details:

```bash
aws s3api create-bucket --bucket test-cache-personal-express--usw2-az1--x-s3 --create-bucket-configuration 'Location={Type=AvailabilityZone,Name=usw2-az1},Bucket={DataRedundancy=SingleAvailabilityZone,Type=Directory}' --region us-west-2
```


### Lifecycle Rule Set Up

A timestamp stored in the item's fixed-width header is used to ensure that items expire at the correct time.

Lifecycle rules are used to cull stale items from the cache. Rules should be configured to cull objects by prefix.

For example, without a `KEY_PREFIX`:
- Objects under 7-days/ expire after 7 days
- Objects under 30-days/ expire after 30 days

```json
{
  "Rules": [
    {
      "ID": "Expire-7-days-prefix",      (1)
      "Filter": { "Prefix": "7-days/" }, (2)
      "Status": "Enabled",               (3)
      "Expiration": { "Days": 7 }        (4)
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

① Give the rule a name

② Set the rule to the "7-days" directory

③ Enable the rule

④ Set the expiration time to match the directory name

> [!NOTE]
> If you configure `KEY_PREFIX` in your Django settings, this prefix is prepended to all keys.
> Your S3 Lifecycle rules must include the `KEY_PREFIX` when defining the filter. For example, if `KEY_PREFIX = "cache-v1"` then the `7-days` rule should filter `cache-v1/7-days/` instead of just `7-days/`.

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

### Use It!

Once your backend is configured and lifecycle rules are in place, you can start using it like any other Django cache client.

#### 1. Basic set/get operations

```python
from django.core.cache import caches

client = caches["s3"]

# Store a value for 60 seconds
client.set("1-days:example-key", {"foo": "bar"}, timeout=60)

# Retrieve the value
value = client.get("1-days:example-key")
print(value)  # {"foo": "bar"}

# Check existence
exists = client.has_key("1-days:example-key")
print(exists)  # True
```

#### 2. Time-based prefixes

```python
# Allowed: timeout <= 1 day
client.set("1-days:short-lived", "value", timeout=60 * 60)  # 1 hour

# Not allowed: timeout exceeds prefix
client.set("1-days:too-long", "value", timeout=7 * 24 * 60 * 60)
# Raises ValueError
```

#### 3. Expiration checks

The backend embeds an expiration timestamp in the object header. Expired objects still exist in S3 until lifecycle rules delete them, but reads will return None automatically.

```python
import time

client.set("1-days:temp", "hello", timeout=5)

time.sleep(10)
print(client.get("1-days:temp"))  # None
```

#### 4. Deleting a value

```python
client.delete("1-days:example-key")
```

5. Persistent objects (never expire)
You can store a persistent object by passing `timeout=None`. These objects are never considered expired by the backend, and their header expiration timestamp is set to 0. Be careful not to use a time-based prefix (N-days:) for persistent items, as that will raise a `ValueError`.

```python
# Persistent key (never expires)
client.set("persistent:config", {"feature_flag": True}, timeout=None)

# Retrieve persistent object
value = client.get("persistent:config")
print(value)  # {"feature_flag": True}

# Check existence
exists = client.has_key("persistent:config")
print(exists)  # True

# Deleting persistent object
client.delete("persistent:config")

# Attempting to store a persistent object under a time-based prefix
client.set("1-days:persistent_config", {"feature_flag": True}, timeout=None)
# Raises ValueError
```

## Roadmap

 - Reserved header fields allow future compression support (zlib/zstd).
 - `clear()` and `touch()` methods open for contribution.
 - Performance benchmarks welcome.

## Testing

```bash
python -m django test --settings 'tests.settings'
```

## License

This repository is available under the permissive BSD license, making it easy and safe to incorporate in your own libraries.

Pull and feature requests are welcome.

## Acknowledgements

Inspired by [CourtListener issue #5304](https://github.com/freelawproject/courtlistener/issues/5304) and [Django issue 32785](https://code.djangoproject.com/ticket/32785).


[dirbuck]: https://docs.aws.amazon.com/AmazonS3/latest/userguide/directory-buckets-overview.html
[1z]: https://aws.amazon.com/s3/storage-classes/express-one-zone/
[lifecycle]: https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-lifecycle-mgmt.html
[rr]: https://developer.mozilla.org/en-US/docs/Web/HTTP/Guides/Range_requests
[1]: https://code.djangoproject.com/ticket/32785
