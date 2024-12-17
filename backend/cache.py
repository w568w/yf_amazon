import hashlib
import inspect
import json
from functools import wraps

import redis

redis_client = redis.Redis(host="localhost", port=6379, db=0)


def cache(cache_keys=None, expire_time=3600):
    """
    Redis cache decorator that caches function results based on specified keyword parameters.

    Args:
        cache_keys (list): List of keyword argument names to use for cache key generation.
                          If None, all kwargs will be used.
        expire_time (int): Time in seconds before the cache expires. Defaults to 1 hour.
    """

    def decorator(func):
        # Get the function's signature
        sig = inspect.signature(func)

        @wraps(func)
        def wrapper(*args, **kwargs):
            # Convert args to kwargs
            bound_args = sig.bind(*args, **kwargs)
            bound_args.apply_defaults()
            all_kwargs = bound_args.arguments

            # Generate cache key based on function name and specified kwargs
            cache_dict = {
                "func_name": func.__name__,
                "kwargs": {k: all_kwargs[k] for k in (cache_keys or all_kwargs.keys())},
            }
            cache_key = hashlib.md5(
                json.dumps(cache_dict, sort_keys=True).encode()
            ).hexdigest()

            # Try to get cached result
            cached_result = redis_client.get(cache_key)
            if cached_result:
                print(f"Method {func.__name__}. Cache hit.")
                return json.loads(cached_result)

            # Calculate result if not cached
            result = func(*args, **kwargs)
            print(f"Method {func.__name__}. Cache miss.")

            # Cache the result
            redis_client.setex(cache_key, expire_time, json.dumps(result))

            return result

        return wrapper

    return decorator
