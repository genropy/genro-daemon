"""Tests for genro_daemon.client – GnrDaemonClient and related helpers."""

import socket
from unittest.mock import MagicMock, patch

import msgpack
import pytest

from genro_daemon import exceptions
from genro_daemon.client import (
    GnrDaemonClient,
    _ConnectionPool,
    _SiteRegisterProxyContext,
)
from genro_daemon.codec import _msgpack_default

# ---------------------------------------------------------------------------
# _get_exception_by_name
# ---------------------------------------------------------------------------


class TestGetExceptionByName:
    def setup_method(self):
        self.client = GnrDaemonClient()

    def test_builtin_exception(self):
        cls = self.client._get_exception_by_name("TypeError")
        assert cls is TypeError

    def test_value_error(self):
        cls = self.client._get_exception_by_name("ValueError")
        assert cls is ValueError

    def test_runtime_error(self):
        cls = self.client._get_exception_by_name("RuntimeError")
        assert cls is RuntimeError

    def test_attribute_error(self):
        cls = self.client._get_exception_by_name("AttributeError")
        assert cls is AttributeError

    def test_custom_daemon_exception(self):
        cls = self.client._get_exception_by_name("GnrDaemonLocked")
        assert cls is exceptions.GnrDaemonLocked

    def test_custom_proto_error(self):
        cls = self.client._get_exception_by_name("GnrDaemonProtoError")
        assert cls is exceptions.GnrDaemonProtoError

    def test_unknown_name_falls_back_to_exception(self):
        cls = self.client._get_exception_by_name("CompletelyUnknown")
        assert cls is Exception


# ---------------------------------------------------------------------------
# _invoke_method (mocked _send)
# ---------------------------------------------------------------------------


class TestInvokeMethod:
    def setup_method(self):
        self.client = GnrDaemonClient("gnr://127.0.0.1:40404", timeout=1)

    def _patch_send(self, return_value):
        return patch.object(self.client, "_send", return_value=return_value)

    def test_success_returns_result(self):
        with self._patch_send([1, 1, None, "pong"]):
            result = self.client._invoke_method("ping")
        assert result == "pong"

    def test_success_with_none_result(self):
        with self._patch_send([1, 1, None, None]):
            result = self.client._invoke_method("do_something")
        assert result is None

    def test_error_raises_builtin_exception(self):
        with self._patch_send([-1, 1, ["TypeError", "bad arg"], None]):
            with pytest.raises(TypeError, match="bad arg"):
                self.client._invoke_method("some_method")

    def test_error_raises_value_error(self):
        with self._patch_send([-1, 1, ["ValueError", "invalid"], None]):
            with pytest.raises(ValueError, match="invalid"):
                self.client._invoke_method("some_method")

    def test_error_with_none_r2_raises_generic_exception(self):
        """The r[2]=None bug fix: should raise Exception, not crash with TypeError."""
        with self._patch_send([-1, 1, None, None]):
            with pytest.raises(Exception, match="no details"):
                self.client._invoke_method("some_method")

    def test_send_returns_none_invoke_returns_none(self):
        """When _send returns None (connection error), _invoke_method returns None."""
        with self._patch_send(None):
            result = self.client._invoke_method("ping")
        assert result is None

    def test_sitename_injected_when_set(self):
        self.client._sitename = "mysite"
        captured = {}
        with patch.object(
            self.client,
            "_send",
            side_effect=lambda d: captured.update({"req": d}) or [1, 1, None, None],
        ):
            self.client._invoke_method("ping")
        sent_kwargs = captured["req"][4]
        assert sent_kwargs["_sitename"] == "mysite"

    def test_sitename_not_injected_when_none(self):
        self.client._sitename = None
        captured = {}
        with patch.object(
            self.client,
            "_send",
            side_effect=lambda d: captured.update({"req": d}) or [1, 1, None, None],
        ):
            self.client._invoke_method("ping")
        sent_kwargs = captured["req"][4]
        assert "_sitename" not in sent_kwargs

    def test_sitename_not_overwritten_if_already_in_kwargs(self):
        self.client._sitename = "default_site"
        captured = {}
        with patch.object(
            self.client,
            "_send",
            side_effect=lambda d: captured.update({"req": d}) or [1, 1, None, None],
        ):
            self.client._invoke_method("ping", _sitename="explicit_site")
        sent_kwargs = captured["req"][4]
        assert sent_kwargs["_sitename"] == "explicit_site"

    def test_args_forwarded(self):
        captured = {}
        with patch.object(
            self.client,
            "_send",
            side_effect=lambda d: captured.update({"req": d}) or [1, 1, None, 10],
        ):
            result = self.client._invoke_method("add", 3, 7)
        assert result == 10
        assert captured["req"][3] == (3, 7)

    def test_kwargs_forwarded(self):
        captured = {}
        with patch.object(
            self.client,
            "_send",
            side_effect=lambda d: captured.update({"req": d}) or [1, 1, None, None],
        ):
            self.client._invoke_method("notify", key="value")
        sent_kwargs = captured["req"][4]
        assert sent_kwargs.get("key") == "value"

    def test_send_called_once_per_invocation(self):
        """_send is called exactly once for each _invoke_method call."""
        with patch.object(
            self.client, "_send", return_value=[1, 1, None, None]
        ) as mock_send:
            self.client._invoke_method("ping")
            self.client._invoke_method("ping")
        assert mock_send.call_count == 2

    def test_getattr_returns_callable(self):
        bound = self.client.ping
        assert callable(bound)

    def test_getattr_calls_invoke_method(self):
        with patch.object(self.client, "_invoke_method", return_value="ok") as mock:
            result = self.client.ping(a=1)
        mock.assert_called_once_with("ping", a=1)
        assert result == "ok"


# ---------------------------------------------------------------------------
# GnrDaemonClient constructor
# ---------------------------------------------------------------------------


class TestClientConstructor:
    def test_default_host_and_port(self):
        c = GnrDaemonClient()
        assert c._host == "127.0.0.1"
        assert c._port == 40404

    def test_url_parsing(self):
        c = GnrDaemonClient("gnr://192.168.1.10:9999")
        assert c._host == "192.168.1.10"
        assert c._port == 9999

    def test_sitename_stored(self):
        c = GnrDaemonClient(sitename="mysite")
        assert c._sitename == "mysite"

    def test_no_sitename_is_none(self):
        c = GnrDaemonClient()
        assert c._sitename is None

    def test_custom_timeout(self):
        c = GnrDaemonClient(timeout=10)
        assert c._timeout == 10


# ---------------------------------------------------------------------------
# _SiteRegisterProxyContext
# ---------------------------------------------------------------------------


class TestSiteRegisterProxyContext:
    def test_enter_creates_client_with_sitename(self):
        ctx = _SiteRegisterProxyContext("127.0.0.1", 40404, "mysite", timeout=5)
        client = ctx.__enter__()
        assert isinstance(client, GnrDaemonClient)
        assert client._sitename == "mysite"
        assert client._host == "127.0.0.1"
        assert client._port == 40404
        assert client._timeout == 5

    def test_exit_clears_client(self):
        ctx = _SiteRegisterProxyContext("127.0.0.1", 40404, "mysite")
        ctx.__enter__()
        ctx.__exit__(None, None, None)
        assert ctx._client is None

    def test_context_manager_usage(self):
        ctx = _SiteRegisterProxyContext("127.0.0.1", 40404, "mysite")
        with ctx as client:
            assert isinstance(client, GnrDaemonClient)
            assert client._sitename == "mysite"
        assert ctx._client is None


# ---------------------------------------------------------------------------
# siteRegisterProxy
# ---------------------------------------------------------------------------


class TestSiteRegisterProxy:
    def test_returns_context_manager(self):
        c = GnrDaemonClient("gnr://127.0.0.1:40404")
        ctx = c.siteRegisterProxy("mysite")
        assert isinstance(ctx, _SiteRegisterProxyContext)

    def test_proxy_carries_same_host_port(self):
        c = GnrDaemonClient("gnr://10.0.0.1:9999")
        ctx = c.siteRegisterProxy("s")
        assert ctx._host == "10.0.0.1"
        assert ctx._port == 9999


# ---------------------------------------------------------------------------
# _ConnectionPool
# ---------------------------------------------------------------------------


class TestConnectionPool:
    def _make_mock_socket(self):
        s = MagicMock(spec=socket.socket)
        return s

    def test_init_stores_params(self):
        pool = _ConnectionPool("localhost", 1234, timeout=3.0, max_idle=4)
        assert pool._host == "localhost"
        assert pool._port == 1234
        assert pool._timeout == 3.0
        assert pool._max_idle == 4

    def test_acquire_creates_new_socket_when_idle_empty(self):
        pool = _ConnectionPool("127.0.0.1", 40404, timeout=1.0)
        mock_sock = self._make_mock_socket()
        with patch.object(pool, "_new_socket", return_value=mock_sock) as mock_new:
            sock = pool.acquire()
        assert sock is mock_sock
        mock_new.assert_called_once()

    def test_acquire_returns_idle_socket_when_available(self):
        pool = _ConnectionPool("127.0.0.1", 40404, timeout=1.0)
        mock_sock = self._make_mock_socket()
        pool._idle.put_nowait(mock_sock)
        sock = pool.acquire()
        assert sock is mock_sock

    def test_release_puts_socket_back_in_pool(self):
        pool = _ConnectionPool("127.0.0.1", 40404, timeout=1.0, max_idle=8)
        mock_sock = self._make_mock_socket()
        pool.release(mock_sock)
        assert pool._idle.qsize() == 1

    def test_release_closes_socket_when_pool_full(self):
        pool = _ConnectionPool("127.0.0.1", 40404, timeout=1.0, max_idle=0)
        mock_sock = self._make_mock_socket()
        pool.release(mock_sock)
        mock_sock.close.assert_called_once()

    def test_release_closes_socket_on_oserror(self):
        pool = _ConnectionPool("127.0.0.1", 40404, timeout=1.0, max_idle=0)
        mock_sock = self._make_mock_socket()
        mock_sock.close.side_effect = OSError("already closed")
        # Should not raise
        pool.release(mock_sock)

    def test_discard_closes_socket(self):
        pool = _ConnectionPool("127.0.0.1", 40404, timeout=1.0)
        mock_sock = self._make_mock_socket()
        pool.discard(mock_sock)
        mock_sock.close.assert_called_once()

    def test_discard_ignores_oserror(self):
        pool = _ConnectionPool("127.0.0.1", 40404, timeout=1.0)
        mock_sock = self._make_mock_socket()
        mock_sock.close.side_effect = OSError("already closed")
        pool.discard(mock_sock)  # must not raise

    def test_close_empties_all_idle_sockets(self):
        pool = _ConnectionPool("127.0.0.1", 40404, timeout=1.0)
        socks = [self._make_mock_socket() for _ in range(3)]
        for s in socks:
            pool._idle.put_nowait(s)
        pool.close()
        assert pool._idle.empty()
        for s in socks:
            s.close.assert_called_once()

    def test_new_socket_connects(self):
        pool = _ConnectionPool("127.0.0.1", 40404, timeout=1.0)
        mock_sock = self._make_mock_socket()
        with patch("socket.socket", return_value=mock_sock):
            sock = pool._new_socket()
        mock_sock.settimeout.assert_called_with(1.0)
        mock_sock.connect.assert_called_with(("127.0.0.1", 40404))
        assert sock is mock_sock


# ---------------------------------------------------------------------------
# GnrDaemonClient with pool_size (exercises _send_pooled)
# ---------------------------------------------------------------------------


class TestClientWithPool:
    def _make_packed_response(self, result):
        """Create a valid msgpack-encoded response."""
        response = [1, 1, None, result]
        return msgpack.packb(response, default=_msgpack_default, use_bin_type=True)

    def test_client_with_pool_size_creates_pool(self):
        c = GnrDaemonClient("gnr://127.0.0.1:40404", pool_size=4)
        assert c._pool is not None

    def test_send_pooled_success(self, daemon_runner):
        """Test _send_pooled via a client with a connection pool."""
        c = daemon_runner.client()
        c._pool = _ConnectionPool(c._host, c._port, timeout=5, max_idle=4)
        result = c.ping()
        assert result == "pong"

    def test_send_pooled_returns_none_on_exception(self):
        """_send_pooled returns None when communication fails."""
        c = GnrDaemonClient("gnr://127.0.0.1:40404", pool_size=4)
        mock_pool = MagicMock()
        mock_pool.acquire.side_effect = OSError("connection refused")
        c._pool = mock_pool
        result = c._send(
            msgpack.packb(
                [0, 1, "ping", [], {}], default=_msgpack_default, use_bin_type=True
            )
        )
        assert result is None

    def test_send_pooled_discards_sock_on_sendall_exception(self):
        """When acquire succeeds but sendall raises, sock is discarded (line 201)."""
        c = GnrDaemonClient("gnr://127.0.0.1:40404", pool_size=4)
        mock_pool = MagicMock()
        mock_sock = MagicMock(spec=socket.socket)
        mock_pool.acquire.return_value = mock_sock
        mock_sock.sendall.side_effect = OSError("broken pipe")
        c._pool = mock_pool
        packed = msgpack.packb(
            [0, 1, "ping", [], {}], default=_msgpack_default, use_bin_type=True
        )
        result = c._send_pooled(packed)
        assert result is None
        mock_pool.discard.assert_called_once_with(mock_sock)

    def test_send_pooled_retries_on_none_response(self, daemon_runner):
        """When first recv returns None, pool should retry with a new socket."""
        c = daemon_runner.client()
        pool = _ConnectionPool(c._host, c._port, timeout=5, max_idle=4)
        c._pool = pool
        # Pack a valid request
        packed = msgpack.packb(
            [0, 1, "ping", [], {}], default=_msgpack_default, use_bin_type=True
        )
        # First _recv returns None, second returns real response
        real_response = self._make_packed_response("pong")
        mock_sock = MagicMock(spec=socket.socket)
        call_count = [0]

        def fake_recv(n):
            call_count[0] += 1
            if call_count[0] == 1:
                return b""  # triggers None from _recv
            return real_response

        mock_sock.recv.side_effect = fake_recv
        mock_sock2 = MagicMock(spec=socket.socket)
        mock_sock2.recv.return_value = real_response

        with (
            patch.object(pool, "acquire", return_value=mock_sock),
            patch.object(pool, "_new_socket", return_value=mock_sock2),
            patch.object(pool, "discard") as mock_discard,
            patch.object(pool, "release") as mock_release,
        ):
            c._send_pooled(packed)

        mock_discard.assert_called_once_with(mock_sock)
        mock_release.assert_called_once_with(mock_sock2)


# ---------------------------------------------------------------------------
# GnrDaemonClient._recv edge cases
# ---------------------------------------------------------------------------


class TestClientRecv:
    def test_recv_returns_none_when_empty_bytes(self):
        """When socket returns empty bytes immediately, _recv returns None."""
        c = GnrDaemonClient()
        mock_sock = MagicMock(spec=socket.socket)
        mock_sock.recv.return_value = b""
        result = c._recv(mock_sock)
        assert result is None

    def test_recv_returns_none_on_timeout_no_data(self):
        """When socket times out with no data, _recv returns None."""
        c = GnrDaemonClient()
        mock_sock = MagicMock(spec=socket.socket)
        mock_sock.recv.side_effect = TimeoutError("timed out")
        result = c._recv(mock_sock)
        assert result is None

    def test_recv_returns_partial_then_timeout(self):
        """When partial data received then timeout, _recv unpacks from chunks."""
        c = GnrDaemonClient()
        mock_sock = MagicMock(spec=socket.socket)
        # Build valid response in one chunk, timeout on second
        response = [1, 1, None, "pong"]
        packed = msgpack.packb(response, default=_msgpack_default, use_bin_type=True)
        mock_sock.recv.side_effect = [packed, TimeoutError("timed out")]
        result = c._recv(mock_sock)
        assert result[3] == "pong"

    def test_recv_handles_chunked_message(self):
        """Data arriving in multiple chunks is reassembled correctly."""
        c = GnrDaemonClient()
        mock_sock = MagicMock(spec=socket.socket)
        response = [1, 1, None, 42]
        packed = msgpack.packb(response, default=_msgpack_default, use_bin_type=True)
        # Split into 2 chunks
        half = len(packed) // 2
        chunk1, chunk2 = packed[:half], packed[half:]
        mock_sock.recv.side_effect = [chunk1, chunk2, TimeoutError()]
        result = c._recv(mock_sock)
        assert result[3] == 42

    def test_recv_outer_unpack_after_inner_unpackvalue_error_then_timeout(self):
        """Inner unpackb raises UnpackValueError, then timeout: outer unpack at line 160 fires."""
        c = GnrDaemonClient()
        mock_sock = MagicMock(spec=socket.socket)
        packed = msgpack.packb(
            [1, 1, None, "ok"], default=_msgpack_default, use_bin_type=True
        )
        mock_sock.recv.side_effect = [packed, TimeoutError()]
        # Make inner unpackb raise UnpackValueError so the outer one runs
        original = msgpack.unpackb
        call_count = [0]

        def patched_unpackb(*args, **kwargs):
            call_count[0] += 1
            if call_count[0] == 1:
                raise msgpack.exceptions.UnpackValueError("forced partial")
            return original(*args, **kwargs)

        with patch("genro_daemon.client.msgpack.unpackb", side_effect=patched_unpackb):
            result = c._recv(mock_sock)
        assert result[3] == "ok"


# ---------------------------------------------------------------------------
# GnrDaemonClient._send one-shot (no pool) path
# ---------------------------------------------------------------------------


class TestClientSendOneShot:
    def test_send_connect_failure_returns_none(self):
        """When socket.connect raises, _send returns None without crashing."""
        c = GnrDaemonClient("gnr://127.0.0.1:40404", timeout=1)
        # No pool -> one-shot path
        assert c._pool is None
        mock_sock = MagicMock(spec=socket.socket)
        mock_sock.connect.side_effect = ConnectionRefusedError("refused")
        mock_sock.__enter__ = lambda s: s
        mock_sock.__exit__ = MagicMock(return_value=False)
        with patch("genro_daemon.client.socket.socket", return_value=mock_sock):
            result = c._send([0, 1, "ping", [], {}])
        assert result is None

    def test_send_success_returns_response(self):
        """One-shot _send with successful connect returns parsed response."""
        c = GnrDaemonClient("gnr://127.0.0.1:40404", timeout=1)
        packed_response = msgpack.packb(
            [1, 1, None, "pong"], default=_msgpack_default, use_bin_type=True
        )
        mock_sock = MagicMock(spec=socket.socket)
        mock_sock.connect.return_value = None
        mock_sock.recv.side_effect = [packed_response, TimeoutError()]
        mock_sock.__enter__ = lambda s: s
        mock_sock.__exit__ = MagicMock(return_value=False)
        with patch("genro_daemon.client.socket.socket", return_value=mock_sock):
            result = c._send([0, 1, "ping", [], {}])
        assert result[3] == "pong"


# ---------------------------------------------------------------------------
# GnrDaemonClient with use_environment=True
# ---------------------------------------------------------------------------


class TestClientUseEnvironment:
    def test_use_environment_calls_load_daemon_options(self):
        """When use_environment=True, constructor calls load_daemon_options."""
        mock_options = {"host": "envhost", "port": "9999"}
        with patch(
            "genro_daemon.client.load_daemon_options", return_value=mock_options
        ) as mock_load:
            c = GnrDaemonClient(use_environment=True)
        mock_load.assert_called_once()
        assert c._host == "envhost"
        assert c._port == 9999
