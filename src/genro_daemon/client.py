import builtins
import os
import queue
import socket
from urllib.parse import urlparse

import msgpack
from gnr.web import logger

from . import exceptions
from .codec import _msgpack_default, _msgpack_object_hook
from .utils import load_daemon_options


class _ConnectionPool:
    """Thread-safe pool of persistent TCP connections to the daemon.

    Sockets are created on demand and returned to the pool after each use.
    A broken socket is discarded rather than returned.  The pool has no fixed
    upper bound on live connections — it is bounded by the number of concurrent
    callers — but idle sockets beyond *max_idle* are closed on return.
    """

    def __init__(self, host: str, port: int, timeout: float, max_idle: int = 8):
        self._host = host
        self._port = port
        self._timeout = timeout
        self._max_idle = max_idle
        self._idle: queue.Queue = queue.Queue()

    def _new_socket(self) -> socket.socket:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
        sock.settimeout(self._timeout)
        sock.connect((self._host, self._port))
        return sock

    def acquire(self) -> socket.socket:
        """Return an open socket, creating one if the pool is empty."""
        try:
            return self._idle.get_nowait()
        except queue.Empty:
            return self._new_socket()

    def release(self, sock: socket.socket) -> None:
        """Return a healthy socket to the pool, closing it if pool is full."""
        if self._idle.qsize() < self._max_idle:
            self._idle.put_nowait(sock)
        else:
            try:
                sock.close()
            except OSError:
                pass

    def discard(self, sock: socket.socket) -> None:
        """Close and drop a broken socket without returning it to the pool."""
        try:
            sock.close()
        except OSError:
            pass

    def close(self) -> None:
        """Close all idle sockets in the pool."""
        while True:
            try:
                self._idle.get_nowait().close()
            except queue.Empty:
                break


class _SiteRegisterProxyContext:
    """Context manager that yields a site-specific GnrDaemonClient.

    Returned by GnrDaemonClient.siteRegisterProxy() to replace the old
    Pyro4 ``with daemon.siteRegisterProxy(sitename) as proxy:`` pattern.
    """

    def __init__(self, host, port, sitename, timeout=3):
        self._host = host
        self._port = port
        self._sitename = sitename
        self._timeout = timeout
        self._client = None

    def __enter__(self):
        self._client = GnrDaemonClient(
            f"gnr://{self._host}:{self._port}",
            timeout=self._timeout,
            sitename=self._sitename,
        )
        return self._client

    def __exit__(self, *args):
        self._client = None


class GnrDaemonClient:
    def __init__(
        self,
        url="gnr://127.0.0.1:40404",
        timeout=3,
        sitename=None,
        pool_size=8,
        **kwargs,
    ):
        options = kwargs
        if kwargs.get("use_environment", False):
            options = load_daemon_options(options=options)
        parsed_url = urlparse(url)
        self._host = (
            options.get("host")
            or os.environ.get("GNR_DAEMON_HOST")
            or parsed_url.hostname
        )
        self._port = int(
            options.get("port") or os.environ.get("GNR_DAEMON_PORT") or parsed_url.port
        )
        self._timeout = timeout
        self._req_counter = 0
        self._sitename = sitename  # namespace sent with every call when set
        self._pool = _ConnectionPool(
            self._host, self._port, timeout, max_idle=pool_size or 8
        )

    def __getattr__(self, method):
        return lambda *args, **kw: self._invoke_method(method, *args, **kw)

    def siteRegisterProxy(self, sitename):
        """Return a context manager yielding a site-specific client.

        Replaces the old ``with daemon_proxy.siteRegisterProxy(sitename) as p:``
        pattern from Pyro4.
        """
        return _SiteRegisterProxyContext(
            self._host, self._port, sitename, self._timeout
        )

    def _get_exception_by_name(self, exception_name):
        exception_class = getattr(builtins, exception_name, None)
        if exception_class is None:
            exception_class = getattr(exceptions, exception_name, None)
        return exception_class if exception_class else Exception

    def _invoke_method(self, method, *args, **kw):
        if self._sitename is not None and "_sitename" not in kw:
            kw["_sitename"] = self._sitename
        r = self._send([0, self._req_counter, method, args, kw])
        if r is None:
            return None
        if r[0] == -1:
            error_info = r[2]
            if (
                not error_info
                or not isinstance(error_info, (list, tuple))
                or len(error_info) < 2
            ):
                raise Exception(
                    f"Daemon returned an error for '{method}' with no details: {error_info!r}"
                )
            logger.error(f"Error: {error_info[0]}: {error_info[1]}")
            raise self._get_exception_by_name(error_info[0])(error_info[1])
        return r[3]

    def _recv(self, sock: socket.socket):
        chunks = []
        while True:
            try:
                chunk = sock.recv(4096)
                if not chunk:
                    break
                chunks.append(chunk)
                try:
                    return msgpack.unpackb(
                        b"".join(chunks),
                        raw=False,
                        object_hook=_msgpack_object_hook,
                    )
                except msgpack.exceptions.UnpackValueError:
                    continue
            except TimeoutError:
                break
        if chunks:
            return msgpack.unpackb(
                b"".join(chunks),
                raw=False,
                object_hook=_msgpack_object_hook,
            )
        return None

    def _send(self, data):
        self._req_counter += 1
        packed_data = msgpack.packb(data, default=_msgpack_default, use_bin_type=True)
        sock = None
        try:
            sock = self._pool.acquire()
            sock.sendall(packed_data)
            response = self._recv(sock)
            if response is None:
                # Connection closed by server; discard and retry once with a fresh socket
                self._pool.discard(sock)
                sock = self._pool._new_socket()
                sock.sendall(packed_data)
                response = self._recv(sock)
            self._pool.release(sock)
            return response
        except Exception as e:
            if sock is not None:
                self._pool.discard(sock)
            logger.error(
                f"Error communicating with daemon at {self._host}:{self._port}: {e}"
            )
            return None
