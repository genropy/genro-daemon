import threading
import time

from .base import StorageBackend


class InMemoryBackend(StorageBackend):
    """Thread-safe in-memory storage backend.

    All data is lost when the process exits.  Suitable for single-process
    deployments and for testing.
    """

    def __init__(self):
        self._data = {}
        self._hdata = {}
        self._locks = {}
        self._lock = threading.RLock()

    # --- key/value ---

    def get(self, key):
        with self._lock:
            return self._data.get(key)

    def set(self, key, value):
        with self._lock:
            self._data[key] = value

    def delete(self, key):
        with self._lock:
            self._data.pop(key, None)

    def keys(self, prefix=""):
        with self._lock:
            return [k for k in self._data.keys() if k.startswith(prefix)]

    # --- hash ---

    def hget(self, hkey, field):
        with self._lock:
            return self._hdata.get(hkey, {}).get(field)

    def hset(self, hkey, field, value):
        with self._lock:
            if hkey not in self._hdata:
                self._hdata[hkey] = {}
            self._hdata[hkey][field] = value

    def hdel(self, hkey, field):
        with self._lock:
            if hkey in self._hdata:
                self._hdata[hkey].pop(field, None)

    def hgetall(self, hkey):
        with self._lock:
            return dict(self._hdata.get(hkey, {}))

    def hkeys(self, hkey):
        with self._lock:
            return list(self._hdata.get(hkey, {}).keys())

    # --- distributed locking ---

    def acquire_lock(self, lock_key, timeout):
        with self._lock:
            entry = self._locks.get(lock_key)
            now = time.time()
            if entry and (now - entry["ts"]) < timeout:
                return False
            self._locks[lock_key] = {"ts": now}
            return True

    def release_lock(self, lock_key):
        with self._lock:
            self._locks.pop(lock_key, None)
