"""Tests for genro_daemon.ars – ArsConnection and the Ars protocol base class."""

import asyncio
import socket
import time
from unittest.mock import AsyncMock, MagicMock

import msgpack
import pytest

from genro_daemon.ars import Ars, ArsConnection
from genro_daemon.codec import _msgpack_default, _msgpack_object_hook
from genro_daemon.exceptions import GnrDaemonMethodNotFound, GnrDaemonProtoError

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

PACK = dict(default=_msgpack_default, use_bin_type=True)
UNPACK = dict(raw=False, object_hook=_msgpack_object_hook)


def pack(data):
    return msgpack.packb(data, **PACK)


def unpack(data):
    return msgpack.unpackb(data, **UNPACK)


def find_free_port():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


# ---------------------------------------------------------------------------
# ArsConnection unit tests (mocked streams)
# ---------------------------------------------------------------------------


def _make_writer():
    """Return a mock that mimics asyncio StreamWriter (write sync, drain async)."""
    writer = MagicMock()
    writer.write = MagicMock()
    writer.drain = AsyncMock(return_value=None)
    writer.close = MagicMock()
    writer.wait_closed = AsyncMock(return_value=None)
    writer.get_extra_info.return_value = ("127.0.0.1", 12345)
    return writer


def _make_reader():
    """Return a mock that mimics asyncio StreamReader (read is async)."""
    reader = MagicMock()
    reader.read = AsyncMock()
    reader.feed_eof = MagicMock()
    reader.set_exception = MagicMock()
    return reader


class TestArsConnectionSend:
    @pytest.mark.asyncio
    async def test_send_writes_to_channel_out(self):
        reader = _make_reader()
        writer = _make_writer()
        unpacker = MagicMock()
        conn = ArsConnection(reader, writer, unpacker)

        payload = b"hello"
        await conn.send(payload, timeout=1)

        writer.write.assert_called_once_with(payload)
        writer.drain.assert_called_once()

    @pytest.mark.asyncio
    async def test_close_feeds_eof_and_closes(self):
        reader = _make_reader()
        writer = _make_writer()
        unpacker = MagicMock()
        conn = ArsConnection(reader, writer, unpacker)

        assert not conn.is_closed
        conn.close()
        assert conn.is_closed
        reader.feed_eof.assert_called_once()
        writer.close.assert_called_once()

    @pytest.mark.asyncio
    async def test_recv_feeds_unpacker_and_returns_messages(self):
        packed = pack([0, 1, "ping", [], {}])

        reader = _make_reader()
        reader.read.side_effect = [packed, TimeoutError()]
        writer = _make_writer()

        unpacker = msgpack.Unpacker(raw=False, object_hook=_msgpack_object_hook)
        conn = ArsConnection(reader, writer, unpacker)

        reqs = await conn.recv(timeout=1)
        assert len(reqs) == 1
        assert reqs[0][2] == "ping"


# ---------------------------------------------------------------------------
# Ars._req_parse unit tests
# ---------------------------------------------------------------------------


class ConcreteArs(Ars):
    """Minimal concrete Ars with a public method for testing."""

    def echo(self, msg, **kwargs):
        return msg

    def fail(self, **kwargs):
        raise ValueError("boom")


class TestArsReqParse:
    def setup_method(self):
        self.ars = ConcreteArs()

    def test_valid_request_returns_method(self):
        req = [0, 42, "echo", ["hi"], {}]
        call_id, method, args, kwargs, name, sitename = self.ars._req_parse(req)
        assert call_id == 42
        assert method == self.ars.echo
        assert args == ["hi"]
        assert name == "echo"

    def test_wrong_type_raises_proto_error(self):
        req = [1, 42, "echo", [], {}]  # type 1 = RES, not REQ
        with pytest.raises(GnrDaemonProtoError):
            self.ars._req_parse(req)

    def test_wrong_length_raises_proto_error(self):
        req = [0, 42, "echo"]  # too short
        with pytest.raises(GnrDaemonProtoError):
            self.ars._req_parse(req)

    def test_private_method_raises_method_not_found(self):
        req = [0, 42, "_private", [], {}]
        with pytest.raises(GnrDaemonMethodNotFound):
            self.ars._req_parse(req)

    def test_unknown_method_raises_method_not_found(self):
        req = [0, 42, "nonexistent", [], {}]
        with pytest.raises(GnrDaemonMethodNotFound):
            self.ars._req_parse(req)


# ---------------------------------------------------------------------------
# Ars._send_answer / _send_error unit tests
# ---------------------------------------------------------------------------


class TestArsResponseBuilding:
    def setup_method(self):
        self.ars = ConcreteArs()

    def _make_connection(self):
        return ArsConnection(_make_reader(), _make_writer(), MagicMock())

    @pytest.mark.asyncio
    async def test_send_answer_packs_result(self):
        conn = self._make_connection()
        await self.ars._send_answer(conn, "hello", call_id=7)
        assert conn.channel_out.write.called
        written = conn.channel_out.write.call_args[0][0]
        decoded = unpack(written)
        assert decoded[0] == Ars.RES
        assert decoded[1] == 7
        assert decoded[2] is None
        assert decoded[3] == "hello"

    @pytest.mark.asyncio
    async def test_send_error_packs_exception_info(self):
        conn = self._make_connection()
        await self.ars._send_error(conn, "TypeError", "bad arg", call_id=3)
        written = conn.channel_out.write.call_args[0][0]
        decoded = unpack(written)
        assert decoded[0] == Ars.ERR
        assert decoded[1] == 3
        assert decoded[2][0] == "TypeError"
        assert decoded[2][1] == "bad arg"

    @pytest.mark.asyncio
    async def test_send_error_with_none_call_id(self):
        conn = self._make_connection()
        await self.ars._send_error(conn, "GnrDaemonProtoError", "oops", call_id=None)
        written = conn.channel_out.write.call_args[0][0]
        decoded = unpack(written)
        assert decoded[0] == Ars.ERR
        assert decoded[1] is None


# ---------------------------------------------------------------------------
# Full request-response integration via real asyncio sockets
# ---------------------------------------------------------------------------


class SimpleArs(Ars):
    """Ars subclass with a couple of test methods."""

    def ping(self, **kwargs):
        return "pong"

    def add(self, a, b, **kwargs):
        return a + b

    def boom(self, **kwargs):
        raise RuntimeError("deliberate error")


def _sync_call(host, port, request, timeout=3):
    """Send one msgpack request, return the decoded response."""
    import socket as _socket

    with _socket.socket(_socket.AF_INET, _socket.SOCK_STREAM) as s:
        s.settimeout(timeout)
        s.connect((host, port))
        s.sendall(pack(request))
        chunks = []
        while True:
            try:
                chunk = s.recv(4096)
                if not chunk:
                    break
                chunks.append(chunk)
                try:
                    return unpack(b"".join(chunks))
                except msgpack.exceptions.UnpackValueError:
                    continue
            except TimeoutError:
                break
    return unpack(b"".join(chunks))


@pytest.fixture()
def ars_runner():
    import threading

    ars = SimpleArs()
    port = find_free_port()
    started = threading.Event()
    loop = asyncio.new_event_loop()

    async def serve():
        server = await asyncio.start_server(ars._serve_request, "127.0.0.1", port)
        ars._server = server
        started.set()
        async with server:
            await server.serve_forever()

    def run_loop():
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(serve())
        except (RuntimeError, asyncio.CancelledError):
            pass

    t = threading.Thread(target=run_loop, daemon=True)
    t.start()
    started.wait(timeout=5)
    time.sleep(0.05)
    yield "127.0.0.1", port
    # Stop server_forever from inside the loop, then stop the loop
    loop.call_soon_threadsafe(ars._server.close)
    futures = asyncio.run_coroutine_threadsafe(ars._server.wait_closed(), loop)
    try:
        futures.result(timeout=2)
    except Exception:
        pass
    loop.call_soon_threadsafe(loop.stop)
    t.join(timeout=2)


class TestArsIntegration:
    def test_ping(self, ars_runner):
        host, port = ars_runner
        resp = _sync_call(host, port, [Ars.REQ, 1, "ping", [], {}])
        assert resp[0] == Ars.RES
        assert resp[3] == "pong"

    def test_add_args(self, ars_runner):
        host, port = ars_runner
        resp = _sync_call(host, port, [Ars.REQ, 2, "add", [3, 4], {}])
        assert resp[3] == 7

    def test_method_raises_returns_error(self, ars_runner):
        host, port = ars_runner
        resp = _sync_call(host, port, [Ars.REQ, 3, "boom", [], {}])
        assert resp[0] == Ars.ERR
        assert resp[2][0] == "RuntimeError"
        assert "deliberate error" in resp[2][1]

    def test_unknown_method_returns_error(self, ars_runner):
        host, port = ars_runner
        resp = _sync_call(host, port, [Ars.REQ, 4, "no_such", [], {}])
        assert resp[0] == Ars.ERR
        assert "no_such" in resp[2][1]

    def test_private_method_returns_error(self, ars_runner):
        host, port = ars_runner
        resp = _sync_call(host, port, [Ars.REQ, 5, "_internal", [], {}])
        assert resp[0] == Ars.ERR

    def test_malformed_request_returns_error(self, ars_runner):
        host, port = ars_runner
        # Wrong message type (RES instead of REQ)
        resp = _sync_call(host, port, [Ars.RES, 6, "ping", [], {}])
        assert resp[0] == Ars.ERR

    def test_sequential_calls(self, ars_runner):
        host, port = ars_runner
        for i in range(5):
            resp = _sync_call(host, port, [Ars.REQ, i, "ping", [], {}])
            assert resp[3] == "pong"
