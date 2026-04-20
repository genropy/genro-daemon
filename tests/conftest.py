"""Shared fixtures for the genro-daemon test suite."""

import asyncio
import socket
import threading
import time

import pytest

from genro_daemon.client import GnrDaemonClient
from genro_daemon.handler import GnrDaemon

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def find_free_port() -> int:
    """Bind to port 0 to let the OS choose an available port."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class _DaemonRunner:
    """Starts GnrDaemon on a background thread using a plain asyncio loop
    (bypassing uvloop/do_start so tests stay portable and fast)."""

    def __init__(self, host: str = "127.0.0.1"):
        self.host = host
        self.port = find_free_port()
        self._daemon = GnrDaemon()
        self._loop: asyncio.AbstractEventLoop | None = None
        self._thread: threading.Thread | None = None
        self._started = threading.Event()

    # -- lifecycle ----------------------------------------------------------

    def start(self):
        self._loop = asyncio.new_event_loop()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()
        assert self._started.wait(timeout=5), "Daemon did not start in time"

    def _run(self):
        asyncio.set_event_loop(self._loop)
        try:
            self._loop.run_until_complete(self._serve())
        except (RuntimeError, asyncio.CancelledError):
            pass  # expected during teardown when the loop is stopped externally

    async def _serve(self):
        server = await asyncio.start_server(
            self._daemon._serve_request, self.host, self.port
        )
        self._daemon._server = server
        self._started.set()
        async with server:
            await server.serve_forever()

    def stop(self):
        if self._loop and not self._loop.is_closed():
            future = asyncio.run_coroutine_threadsafe(
                self._async_shutdown(), self._loop
            )
            try:
                future.result(timeout=5)
            except Exception:
                pass
        if self._thread:
            self._thread.join(timeout=3)
        if self._loop and not self._loop.is_closed():
            self._loop.close()

    async def _async_shutdown(self):
        if self._daemon._server:
            self._daemon._server.close()
        tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        self._loop.stop()

    # -- convenience --------------------------------------------------------

    def client(self, sitename: str | None = None) -> GnrDaemonClient:
        return GnrDaemonClient(
            f"gnr://{self.host}:{self.port}", timeout=5, sitename=sitename
        )


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def free_port() -> int:
    return find_free_port()


@pytest.fixture()
def daemon_runner():
    """Yields a running _DaemonRunner; tears it down after the test."""
    runner = _DaemonRunner()
    runner.start()
    # small grace period so the server loop is fully serving
    time.sleep(0.05)
    yield runner
    runner.stop()


@pytest.fixture()
def daemon_client(daemon_runner) -> GnrDaemonClient:
    """A GnrDaemonClient connected to the test daemon (no sitename)."""
    return daemon_runner.client()
