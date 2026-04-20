import asyncio
import signal
import sys
import time

import msgpack
import uvloop
from gnr.web import logger

from . import metrics
from .codec import _msgpack_default, _msgpack_object_hook
from .exceptions import GnrDaemonMethodNotFound, GnrDaemonProtoError
from .utils import load_daemon_options

SCK_PAYLOAD_SIZE = 1024
DEFAULT_PORT = 40404


class ArsConnection:
    def __init__(self, channel_in, channel_out, unpacker):
        self.channel_in = channel_in
        self.channel_out = channel_out
        self.unpacker = unpacker
        self._is_closed = False
        self.peer = self.channel_out.get_extra_info("peername")

    async def send(self, req, timeout):
        self.channel_out.write(req)
        await asyncio.wait_for(self.channel_out.drain(), timeout)

    async def recv(self, timeout):
        reqs = []
        while True:
            data = await asyncio.wait_for(
                self.channel_in.read(SCK_PAYLOAD_SIZE), timeout
            )
            if not data:
                raise OSError(f"Connection from {self.peer} closed")
            self.unpacker.feed(data)
            reqs = [*self.unpacker]
            if len(reqs) == 0:
                continue
            break
        return reqs

    def close(self):
        self.channel_in.feed_eof()
        self.channel_out.close()
        self._is_closed = True

    @property
    def is_closed(self):
        return self._is_closed


class Ars:
    ERR = -1
    REQ = 0
    RES = 1

    _timeout = 3  # coroutine / idle timeout (seconds)
    _recv_timeout = 10  # TCP receive timeout per chunk (seconds)
    _send_timeout = 60  # drain timeout for outgoing responses (seconds)
    _packer_parms = dict(default=_msgpack_default, use_bin_type=True)

    def __init__(self, *args, **kwargs):
        self._loop = kwargs.get("loop")
        if self._loop:
            self._loop.add_signal_handler(
                signal.SIGTERM,
                lambda: asyncio.create_task(self._stop_handler(reason="SIGTERM")),
            )
        self._server = None
        self._daemon = None

    def _record_request_metrics(self, m, method_name, sitename, status, elapsed=None):
        """Increment the requests counter and, when *elapsed* is given, the histogram."""
        if not m:
            return
        m.requests_total.labels(
            method=method_name or "", sitename=sitename, status=status
        ).inc()
        if elapsed is not None:
            m.request_duration_seconds.labels(
                method=method_name, sitename=sitename
            ).observe(elapsed)

    async def _serve_request(self, channel_in, channel_out):
        """Handle one TCP connection for its full lifetime.

        Reads msgpack-framed requests in a loop, dispatches each to the
        appropriate handler via :meth:`_req_parse`, and writes back either a
        success response or an error frame.  Prometheus metrics (when enabled)
        are updated for every request.
        """
        m = metrics.get()
        if m:
            m.active_tcp_connections.inc()
        connection = ArsConnection(
            channel_in,
            channel_out,
            msgpack.Unpacker(raw=False, object_hook=_msgpack_object_hook),
        )
        try:
            while not connection.is_closed:
                reqs = []
                try:
                    reqs = await connection.recv(self._recv_timeout)
                except TimeoutError:
                    logger.debug(
                        "Idle connection from %s closed after timeout.", connection.peer
                    )
                    connection.close()
                    await channel_out.wait_closed()
                    continue
                except OSError:
                    break
                except Exception as e:
                    connection.channel_in.set_exception(e)
                    raise

                for req in reqs:
                    if not isinstance(req, (tuple, list)):
                        try:
                            await self._send_error(
                                connection, "Invalid protocol", -1, None
                            )
                            continue
                        except Exception as e:
                            logger.error("Error when receiving req: %s", e)

                    method = None
                    call_id = None
                    args = None
                    method_name = None
                    sitename = ""
                    try:
                        (
                            call_id,
                            method,
                            args,
                            kwargs,
                            method_name,
                            sitename,
                        ) = self._req_parse(req)
                    except Exception as e:
                        self._record_request_metrics(m, method_name, sitename, "error")
                        logger.error("Error during request %s parse: %s", req, e)
                        await self._send_error(
                            connection, type(e).__name__, str(e), call_id
                        )
                        continue

                    t0 = time.perf_counter()
                    try:
                        ret = method.__call__(*args, **kwargs)
                        if asyncio.iscoroutine(ret):
                            ret = await asyncio.wait_for(ret, self._timeout)
                    except Exception as e:
                        self._record_request_metrics(
                            m, method_name, sitename, "error", time.perf_counter() - t0
                        )
                        await self._send_error(
                            connection, type(e).__name__, str(e), call_id
                        )
                    else:
                        self._record_request_metrics(
                            m, method_name, sitename, "ok", time.perf_counter() - t0
                        )
                        await self._send_answer(connection, ret, call_id)
        finally:
            if m:
                m.active_tcp_connections.dec()
            channel_out.close()
            await channel_out.wait_closed()

    async def _send_answer(self, connection, result, call_id):
        response = (self.RES, call_id, None, result)
        try:
            ret = msgpack.packb(response, **self._packer_parms)
            await connection.send(ret, self._send_timeout)
        except TimeoutError:
            logger.error(
                "Timeout when sending to %s",
                connection.channel_out.get_extra_info("peername"),
            )
        except Exception as e:
            logger.error(
                "Error %s during send to %s: %s",
                e,
                connection.channel_out.get_extra_info("peername"),
                result,
            )

    def _req_parse(self, req):
        if req[0] != self.REQ or len(req) != 5:
            raise GnrDaemonProtoError(
                f"Invalid protocol: expected [REQ, call_id, method, args, kwargs] "
                f"(REQ={self.REQ}), got type={req[0]!r} len={len(req)}"
            )
        request_type, call_id, method_name, args, kwargs = req
        method = getattr(self, method_name, None)
        if not method or method_name.startswith("_"):
            raise GnrDaemonMethodNotFound(f"No such method '{method_name}' on daemon")
        return call_id, method, args, kwargs, method_name, ""

    async def _send_error(self, connection, exception, error, call_id):
        response = (self.ERR, call_id, (exception, error), None)
        try:
            await connection.send(
                msgpack.packb(response, **self._packer_parms),
                self._send_timeout,
            )
        except TimeoutError:
            logger.error(
                "Timeout when _send_error %s to %s",
                error,
                connection.channel_out.get_extra_info("peername"),
            )
        except Exception as e:
            logger.error(
                "Exception %s raised when _send_error %s to %s",
                e,
                error,
                connection.channel_out.get_extra_info("peername"),
            )

    async def _stop_handler(self, *args, **kw):
        self.stop(**kw)

    def start(self, *args, **kwargs):
        logger.info("Daemon initial start up")
        options = {"host": "localhost", "port": DEFAULT_PORT}
        options.update(kwargs)
        if kwargs.get("use_environment", False):
            options = load_daemon_options(options=options)
        self.do_start(*args, **options)

    async def _start_server(self, *args, **kwargs):
        self._server = await asyncio.start_server(
            self._serve_request,
            kwargs.get("bind") or kwargs.get("host"),
            kwargs.get("port"),
        )
        async with self._server:
            await self._server.serve_forever()

    def do_start(self, *args, **kwargs):
        logger.info("Daemon Starting")
        if sys.version_info[:3] >= (3, 12):
            try:
                uvloop.run(self._start_server(*args, **kwargs))
            except KeyboardInterrupt:
                self.stop(reason="Keyboard interrupt")
        else:
            uvloop.install()
            if not self._loop:
                self._loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self._loop)
            try:
                self._daemon = self._loop.create_task(
                    self._start_server(*args, **kwargs)
                )
                self._loop.run_until_complete(self._daemon)
                self._loop.run_forever()
            except KeyboardInterrupt:
                self.stop(reason="keyboard interrupt")
            finally:
                self._loop.close()

    def stop(self, reason="SIGTERM", **kw):
        logger.info(f"Daemon Stopping via {reason}")
        if self._server:
            self._server.close()
            try:
                loop = asyncio.get_running_loop()
                loop.run_until_complete(self._server.wait_closed())
            except RuntimeError:
                asyncio.run(self._server.wait_closed())
        self._finalize_event_loop()

    def _finalize_event_loop(self):
        try:
            loop = asyncio.get_running_loop()
            tasks = [
                t for t in asyncio.all_tasks(loop) if t is not asyncio.current_task()
            ]
            if tasks:
                loop.run_until_complete(asyncio.gather(*tasks))
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            loop.run_until_complete(self._shutdown())
            loop.close()

    async def _shutdown(self):
        tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
        if tasks:
            logger.info("Waiting for pending tasks to complete...")
            await asyncio.gather(*tasks)
