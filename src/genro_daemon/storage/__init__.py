import os
from urllib.parse import parse_qs, urlparse

from .base import StorageBackend
from .memory import InMemoryBackend

# Single environment variable for the storage backend.
# Format: "memory:" or a Redis URL "redis://[user:password@]host[:port][/db][?prefix=...]"
_ENV_STORE = "GNR_DAEMON_STORE"


def _parse_store_url(url: str) -> dict:
    """Parse a store URL string into a config dict.

    Supported schemes
    -----------------
    ``memory:``
        In-process memory backend (default, no persistence).

    ``redis://[user:password@]host[:port][/db][?prefix=<prefix>]``
        Redis backend.  ``user`` is accepted but ignored (Redis does not use
        usernames in AUTH; use ``default`` or omit it).  ``password`` maps to
        the Redis AUTH password.  ``db`` is the database index (default ``0``).
        The optional ``prefix`` query-string parameter sets the key prefix
        (default ``gnrd:``).

    Examples::

        memory:
        redis://localhost:6379/0
        redis://:secret@localhost:6379/0
        redis://default:secret@redis-host:6380/1?prefix=prod:

    Raises :class:`ValueError` for unknown schemes.
    """
    parsed = urlparse(url)
    scheme = parsed.scheme.lower()

    if scheme == "memory":
        return {"storage_backend": "memory"}

    if scheme == "redis":
        redis_cfg: dict = {}
        if parsed.hostname:
            redis_cfg["host"] = parsed.hostname
        if parsed.port:
            redis_cfg["port"] = parsed.port
        db_str = parsed.path.lstrip("/")
        if db_str:
            redis_cfg["db"] = int(db_str)
        if parsed.password:
            redis_cfg["password"] = parsed.password
        # 'default' is the conventional placeholder username; skip it
        if parsed.username and parsed.username != "default":
            redis_cfg["username"] = parsed.username
        qs = parse_qs(parsed.query)
        if "prefix" in qs:
            redis_cfg["prefix"] = qs["prefix"][0]
        return {"storage_backend": "redis", "redis": redis_cfg}

    raise ValueError(
        f"Unsupported GNR_DAEMON_STORE scheme {scheme!r}. "
        "Use 'memory:' or 'redis://[user:password@]host[:port][/db]'."
    )


def get_backend(config: dict, sitename: str | None = None) -> StorageBackend:
    """Factory: return a :class:`StorageBackend` instance.

    Resolution order (highest priority first):

    1. ``GNR_DAEMON_STORE`` environment variable (URL form, e.g. ``redis://localhost:6379/0``)
    2. ``config`` dict (populated from site / environment XML)
    3. Default: in-memory backend

    Backend names (as resolved from the URL scheme or ``config['storage_backend']``):

    - ``"memory"`` (default) – :class:`InMemoryBackend`
    - ``"redis"``             – :class:`~genro_daemon.storage.redis.RedisBackend`

    Per-site namespace isolation is handled by :class:`~genro_daemon.siteregister.BaseRegister`
    via its ``_ns`` attribute (``"{sitename}:{ClassName}"``), so the Redis prefix stays
    as-is (default ``gnrd:``). The *sitename* parameter is accepted for API compatibility
    but unused.
    """
    store_url = os.environ.get(_ENV_STORE)
    if store_url:
        effective = _parse_store_url(store_url)
    else:
        effective = dict(config or {})

    backend = effective.get("storage_backend", "memory")

    if backend == "redis":
        from .redis import RedisBackend

        redis_kwargs = dict(effective.get("redis", {}))
        if sitename:
            base_prefix = redis_kwargs.get("prefix", "gnrd:")
            redis_kwargs["prefix"] = f"{base_prefix}{sitename}:"
        return RedisBackend(**redis_kwargs)

    return InMemoryBackend()


__all__ = ["StorageBackend", "InMemoryBackend", "get_backend", "_parse_store_url"]
