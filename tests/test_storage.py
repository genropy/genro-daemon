"""Tests for genro_daemon.storage – InMemoryBackend, RedisBackend, and factory."""

import threading

import pytest

from genro_daemon.storage import _parse_store_url, get_backend
from genro_daemon.storage.memory import InMemoryBackend

# ===========================================================================
# Helpers – Redis availability
# ===========================================================================


def _redis_available() -> bool:
    """Return True if a password-less Redis is reachable at localhost:6379."""
    try:
        import redis

        r = redis.Redis(host="localhost", port=6379, db=0, socket_connect_timeout=1)
        r.ping()
        return True
    except Exception:
        return False


REDIS_AVAILABLE = _redis_available()
_REDIS_SKIP_REASON = "Redis not available at localhost:6379 (no password)"
_TEST_PREFIX = "gnrd:test:"


# ===========================================================================
# InMemoryBackend
# ===========================================================================


class TestInMemoryBackendKeyValue:
    def setup_method(self):
        self.b = InMemoryBackend()

    def test_set_and_get(self):
        self.b.set("k", "v")
        assert self.b.get("k") == "v"

    def test_get_missing_returns_none(self):
        assert self.b.get("nope") is None

    def test_set_overwrite(self):
        self.b.set("k", "first")
        self.b.set("k", "second")
        assert self.b.get("k") == "second"

    def test_delete(self):
        self.b.set("k", "v")
        self.b.delete("k")
        assert self.b.get("k") is None

    def test_delete_missing_is_noop(self):
        self.b.delete("ghost")  # must not raise

    def test_keys_empty(self):
        assert self.b.keys("pfx") == []

    def test_keys_with_prefix(self):
        self.b.set("pfx:a", 1)
        self.b.set("pfx:b", 2)
        self.b.set("other:c", 3)
        result = self.b.keys("pfx")
        assert sorted(result) == ["pfx:a", "pfx:b"]

    def test_keys_prefix_not_full_match(self):
        self.b.set("abc:1", "x")
        self.b.set("abcdef:2", "y")
        result = self.b.keys("abc")
        assert sorted(result) == ["abc:1", "abcdef:2"]

    def test_store_complex_value(self):
        value = {"nested": [1, 2, 3], "ts": 42}
        self.b.set("k", value)
        assert self.b.get("k") == value


class TestInMemoryBackendHash:
    def setup_method(self):
        self.b = InMemoryBackend()

    def test_hset_and_hget(self):
        self.b.hset("h", "f", "v")
        assert self.b.hget("h", "f") == "v"

    def test_hget_missing_field_returns_none(self):
        assert self.b.hget("h", "nope") is None

    def test_hget_missing_hash_returns_none(self):
        assert self.b.hget("no-hash", "f") is None

    def test_hdel(self):
        self.b.hset("h", "f", "v")
        self.b.hdel("h", "f")
        assert self.b.hget("h", "f") is None

    def test_hdel_missing_is_noop(self):
        self.b.hdel("h", "ghost")  # must not raise

    def test_hgetall(self):
        self.b.hset("h", "a", 1)
        self.b.hset("h", "b", 2)
        assert self.b.hgetall("h") == {"a": 1, "b": 2}

    def test_hgetall_missing_hash_returns_empty(self):
        assert self.b.hgetall("no-hash") == {}

    def test_hkeys(self):
        self.b.hset("h", "x", 10)
        self.b.hset("h", "y", 20)
        assert sorted(self.b.hkeys("h")) == ["x", "y"]

    def test_hkeys_missing_hash_returns_empty(self):
        assert self.b.hkeys("no-hash") == []


class TestInMemoryBackendLock:
    def setup_method(self):
        self.b = InMemoryBackend()

    def test_acquire_succeeds(self):
        assert self.b.acquire_lock("lk", timeout=1000) is True

    def test_acquire_same_key_twice_fails(self):
        self.b.acquire_lock("lk", timeout=1000)
        assert self.b.acquire_lock("lk", timeout=1000) is False

    def test_release_allows_reacquire(self):
        self.b.acquire_lock("lk", timeout=1000)
        self.b.release_lock("lk")
        assert self.b.acquire_lock("lk", timeout=1000) is True

    def test_release_missing_lock_is_noop(self):
        self.b.release_lock("ghost")  # must not raise

    def test_different_keys_are_independent(self):
        assert self.b.acquire_lock("a", timeout=1000) is True
        assert self.b.acquire_lock("b", timeout=1000) is True

    def test_thread_safety(self):
        """Exactly one thread wins a race for the same lock."""
        winners = []

        def try_acquire():
            if self.b.acquire_lock("shared", timeout=500):
                winners.append(1)

        threads = [threading.Thread(target=try_acquire) for _ in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert len(winners) == 1


# ===========================================================================
# RedisBackend – real server (skipped when Redis is not available)
# ===========================================================================


@pytest.fixture()
def redis_backend():
    """RedisBackend connected to localhost:6379/0 with an isolated test prefix.

    Skipped automatically when Redis is not reachable.
    All keys written under *_TEST_PREFIX* are flushed before and after each test.
    """
    if not REDIS_AVAILABLE:
        pytest.skip(_REDIS_SKIP_REASON)

    import redis as _redis

    from genro_daemon.storage.redis import RedisBackend

    r = _redis.Redis(host="localhost", port=6379, db=0)

    def _flush():
        for key in r.keys(f"{_TEST_PREFIX}*"):
            r.delete(key)

    _flush()
    backend = RedisBackend(host="localhost", port=6379, db=0, prefix=_TEST_PREFIX)
    yield backend
    _flush()


@pytest.mark.skipif(not REDIS_AVAILABLE, reason=_REDIS_SKIP_REASON)
class TestRedisBackendKeyValue:
    def test_set_and_get(self, redis_backend):
        redis_backend.set("k", "hello")
        assert redis_backend.get("k") == "hello"

    def test_get_missing_returns_none(self, redis_backend):
        assert redis_backend.get("no-such-key") is None

    def test_set_overwrite(self, redis_backend):
        redis_backend.set("k", "first")
        redis_backend.set("k", "second")
        assert redis_backend.get("k") == "second"

    def test_delete(self, redis_backend):
        redis_backend.set("k", "v")
        redis_backend.delete("k")
        assert redis_backend.get("k") is None

    def test_delete_missing_is_noop(self, redis_backend):
        redis_backend.delete("ghost")  # must not raise

    def test_store_complex_value(self, redis_backend):
        value = {"nested": [1, 2, 3], "flag": True}
        redis_backend.set("k", value)
        assert redis_backend.get("k") == value

    def test_keys_with_prefix(self, redis_backend):
        redis_backend.set("pfx:a", 1)
        redis_backend.set("pfx:b", 2)
        redis_backend.set("other:c", 3)
        result = redis_backend.keys("pfx")
        assert sorted(result) == ["pfx:a", "pfx:b"]

    def test_keys_empty(self, redis_backend):
        assert redis_backend.keys("no-such-prefix") == []


@pytest.mark.skipif(not REDIS_AVAILABLE, reason=_REDIS_SKIP_REASON)
class TestRedisBackendHash:
    def test_hset_and_hget(self, redis_backend):
        redis_backend.hset("h", "f", "val")
        assert redis_backend.hget("h", "f") == "val"

    def test_hget_missing_field_returns_none(self, redis_backend):
        redis_backend.hset("h", "f", "val")
        assert redis_backend.hget("h", "nope") is None

    def test_hget_missing_hash_returns_none(self, redis_backend):
        assert redis_backend.hget("no-hash", "f") is None

    def test_hdel(self, redis_backend):
        redis_backend.hset("h", "f", "val")
        redis_backend.hdel("h", "f")
        assert redis_backend.hget("h", "f") is None

    def test_hdel_missing_is_noop(self, redis_backend):
        redis_backend.hdel("h", "ghost")  # must not raise

    def test_hgetall(self, redis_backend):
        redis_backend.hset("h", "a", 1)
        redis_backend.hset("h", "b", 2)
        assert redis_backend.hgetall("h") == {"a": 1, "b": 2}

    def test_hgetall_missing_hash_returns_empty(self, redis_backend):
        assert redis_backend.hgetall("no-hash") == {}

    def test_hkeys(self, redis_backend):
        redis_backend.hset("h", "x", 10)
        redis_backend.hset("h", "y", 20)
        assert sorted(redis_backend.hkeys("h")) == ["x", "y"]

    def test_hkeys_missing_hash_returns_empty(self, redis_backend):
        assert redis_backend.hkeys("no-hash") == []


@pytest.mark.skipif(not REDIS_AVAILABLE, reason=_REDIS_SKIP_REASON)
class TestRedisBackendLock:
    def test_acquire_succeeds(self, redis_backend):
        assert redis_backend.acquire_lock("lk", timeout=2) is True

    def test_acquire_same_key_twice_fails(self, redis_backend):
        redis_backend.acquire_lock("lk", timeout=2)
        assert redis_backend.acquire_lock("lk", timeout=2) is False

    def test_release_allows_reacquire(self, redis_backend):
        redis_backend.acquire_lock("lk", timeout=2)
        redis_backend.release_lock("lk")
        assert redis_backend.acquire_lock("lk", timeout=2) is True

    def test_release_missing_lock_is_noop(self, redis_backend):
        redis_backend.release_lock("ghost")  # must not raise

    def test_different_keys_are_independent(self, redis_backend):
        assert redis_backend.acquire_lock("a", timeout=2) is True
        assert redis_backend.acquire_lock("b", timeout=2) is True

    def test_thread_safety(self, redis_backend):
        """Exactly one thread wins a race for the same lock key."""
        winners = []

        def try_acquire():
            if redis_backend.acquire_lock("shared", timeout=2):
                winners.append(1)

        threads = [threading.Thread(target=try_acquire) for _ in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert len(winners) == 1


# ===========================================================================
# _parse_store_url
# ===========================================================================


class TestParseStoreUrl:
    def test_memory_scheme(self):
        assert _parse_store_url("memory:") == {"storage_backend": "memory"}

    def test_redis_minimal(self):
        result = _parse_store_url("redis://localhost:6379/0")
        assert result["storage_backend"] == "redis"
        r = result["redis"]
        assert r["host"] == "localhost"
        assert r["port"] == 6379
        assert r["db"] == 0

    def test_redis_with_password(self):
        r = _parse_store_url("redis://:secret@localhost:6379/0")["redis"]
        assert r["password"] == "secret"

    def test_redis_default_user_is_ignored(self):
        r = _parse_store_url("redis://default:secret@localhost:6379/0")["redis"]
        assert r["password"] == "secret"
        assert "username" not in r

    def test_redis_non_default_user_is_kept(self):
        r = _parse_store_url("redis://myuser:secret@localhost:6379/0")["redis"]
        assert r["username"] == "myuser"
        assert r["password"] == "secret"

    def test_redis_custom_prefix_via_query(self):
        r = _parse_store_url("redis://localhost:6379/0?prefix=prod:")["redis"]
        assert r["prefix"] == "prod:"

    def test_redis_non_zero_db(self):
        r = _parse_store_url("redis://localhost:6379/3")["redis"]
        assert r["db"] == 3

    def test_redis_custom_port(self):
        r = _parse_store_url("redis://myhost:6380/0")["redis"]
        assert r["port"] == 6380
        assert r["host"] == "myhost"

    def test_unknown_scheme_raises(self):
        with pytest.raises(ValueError, match="Unsupported"):
            _parse_store_url("postgres://localhost/mydb")


# ===========================================================================
# get_backend factory
# ===========================================================================


class TestGetBackend:
    def test_default_returns_in_memory(self):
        assert isinstance(get_backend({}, sitename="site1"), InMemoryBackend)

    def test_none_config_returns_in_memory(self):
        assert isinstance(get_backend(None, sitename="site1"), InMemoryBackend)

    def test_memory_url_env(self, monkeypatch):
        monkeypatch.setenv("GNR_DAEMON_STORE", "memory:")
        assert isinstance(get_backend({}, sitename="s"), InMemoryBackend)

    def test_memory_config_dict(self):
        assert isinstance(
            get_backend({"storage_backend": "memory"}, sitename="s"), InMemoryBackend
        )

    def test_redis_url_env_returns_redis_backend(self, monkeypatch):
        pytest.importorskip("redis")
        monkeypatch.setenv("GNR_DAEMON_STORE", "redis://localhost:6379/0")
        from genro_daemon.storage.redis import RedisBackend

        backend = get_backend({}, sitename="mysite")
        assert isinstance(backend, RedisBackend)
        assert backend._prefix == "gnrd:mysite:"

    def test_redis_url_env_custom_prefix(self, monkeypatch):
        pytest.importorskip("redis")
        monkeypatch.setenv("GNR_DAEMON_STORE", "redis://localhost:6379/0?prefix=prod:")
        from genro_daemon.storage.redis import RedisBackend

        backend = get_backend({}, sitename="mysite")
        assert isinstance(backend, RedisBackend)
        assert backend._prefix == "prod:mysite:"

    def test_redis_config_dict_returns_redis_backend(self):
        pytest.importorskip("redis")
        from genro_daemon.storage.redis import RedisBackend

        backend = get_backend({"storage_backend": "redis"}, sitename="mysite")
        assert isinstance(backend, RedisBackend)
        assert backend._prefix == "gnrd:mysite:"

    def test_env_takes_precedence_over_config(self, monkeypatch):
        monkeypatch.setenv("GNR_DAEMON_STORE", "memory:")
        # config says redis, env says memory — env wins
        result = get_backend({"storage_backend": "redis"}, sitename="s")
        assert isinstance(result, InMemoryBackend)


# ===========================================================================
# RedisBackend – connection failure
# ===========================================================================


class TestRedisBackendConnectionFailure:
    def test_init_raises_connection_error_when_ping_fails(self):
        """RedisBackend.__init__ raises ConnectionError when Redis is unreachable."""
        pytest.importorskip("redis")
        from genro_daemon.storage.redis import RedisBackend

        with pytest.raises(ConnectionError, match="Cannot connect to Redis"):
            RedisBackend(host="127.0.0.1", port=19999, db=0)  # nothing listening there
