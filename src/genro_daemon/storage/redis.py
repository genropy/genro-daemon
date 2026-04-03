import pickle

from .base import StorageBackend


class RedisBackend(StorageBackend):
    """Redis-backed storage backend.

    Values are serialised with :mod:`pickle` so that arbitrary Python objects
    (Bag instances, sets, datetimes …) survive round-trips.

    Requires the ``redis`` extra::

        pip install "genro-daemon[redis]"

    Constructor arguments are forwarded to :class:`redis.Redis`.

    The optional *prefix* parameter (default ``"gnrd:"``) is prepended to
    every key so that multiple daemon instances can share one Redis database.
    """

    def __init__(self, host="localhost", port=6379, db=0, prefix="gnrd:", **kwargs):
        import redis  # lazy import so the package stays optional

        self._prefix = prefix
        self._r = redis.Redis(host=host, port=port, db=db, **kwargs)
        try:
            self._r.ping()
        except redis.exceptions.ConnectionError as e:
            raise ConnectionError(
                f"Cannot connect to Redis at {host}:{port} db={db}: {e}"
            ) from e

    # --- helpers ---

    def _k(self, key: str) -> str:
        return f"{self._prefix}{key}"

    def _pack(self, value) -> bytes:
        return pickle.dumps(value, protocol=pickle.HIGHEST_PROTOCOL)

    def _unpack(self, raw) -> object:
        return pickle.loads(raw) if raw is not None else None

    # --- key/value ---

    def get(self, key):
        return self._unpack(self._r.get(self._k(key)))

    def set(self, key, value):
        self._r.set(self._k(key), self._pack(value))

    def delete(self, key):
        self._r.delete(self._k(key))

    def keys(self, prefix=""):
        p_len = len(self._prefix)
        return [k.decode()[p_len:] for k in self._r.keys(self._k(prefix) + "*")]

    # --- hash ---

    def hget(self, hkey, field):
        return self._unpack(self._r.hget(self._k(hkey), field))

    def hset(self, hkey, field, value):
        self._r.hset(self._k(hkey), field, self._pack(value))

    def hdel(self, hkey, field):
        self._r.hdel(self._k(hkey), field)

    def hgetall(self, hkey):
        return {
            k.decode(): self._unpack(v)
            for k, v in self._r.hgetall(self._k(hkey)).items()
        }

    def hkeys(self, hkey):
        return [k.decode() for k in self._r.hkeys(self._k(hkey))]

    # --- distributed locking ---

    def acquire_lock(self, lock_key, timeout):
        key = self._k(f"lock:{lock_key}")
        return bool(self._r.set(key, "1", px=int(timeout * 1000), nx=True))

    def release_lock(self, lock_key):
        self._r.delete(self._k(f"lock:{lock_key}"))
