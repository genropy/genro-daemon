from abc import ABC, abstractmethod
from typing import Any


class StorageBackend(ABC):
    """Pluggable backend for register data persistence and distributed locking.

    Implementations must be thread-safe. Two backends are provided:
    - InMemoryBackend: plain dict, single-process, no persistence across restarts.
    - RedisBackend:    Redis-backed, supports multi-process and persistence.
    """

    # --- key/value (lightweight scalars or small blobs) ---

    @abstractmethod
    def get(self, key: str) -> Any:
        """Return the value stored at *key*, or None if absent."""

    @abstractmethod
    def set(self, key: str, value: Any) -> None:
        """Store *value* at *key*."""

    @abstractmethod
    def delete(self, key: str) -> None:
        """Remove *key* (no-op if absent)."""

    @abstractmethod
    def keys(self, prefix: str = "") -> list:
        """Return all keys that start with *prefix*."""

    # --- hash (per-namespace item store) ---

    @abstractmethod
    def hget(self, hkey: str, field: str) -> Any:
        """Return field value from hash *hkey*, or None."""

    @abstractmethod
    def hset(self, hkey: str, field: str, value: Any) -> None:
        """Set *field* in hash *hkey* to *value*."""

    @abstractmethod
    def hdel(self, hkey: str, field: str) -> None:
        """Delete *field* from hash *hkey*."""

    @abstractmethod
    def hgetall(self, hkey: str) -> dict:
        """Return all field→value pairs from hash *hkey*."""

    @abstractmethod
    def hkeys(self, hkey: str) -> list:
        """Return all field names in hash *hkey*."""

    # --- distributed locking ---

    @abstractmethod
    def acquire_lock(self, lock_key: str, timeout: float) -> bool:
        """Try to acquire an exclusive lock on *lock_key*.

        Returns True if the lock was acquired, False if it is already held
        by another holder and has not expired (*timeout* seconds).
        """

    @abstractmethod
    def release_lock(self, lock_key: str) -> None:
        """Release the lock on *lock_key* (no-op if not held)."""
