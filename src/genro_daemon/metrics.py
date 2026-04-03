"""Optional Prometheus metrics exposition for genro-daemon.

Install the optional dependency to enable:

    pip install "genro-daemon[prometheus]"

Then start the daemon with the ``GNR_DAEMON_METRICS_PORT`` environment variable (or
the ``metrics_port`` key in ``environment.xml``) set to the port you want the
HTTP metrics server to listen on::

    GNR_DAEMON_METRICS_PORT=9090 gnrdaemon start

Prometheus can then scrape ``http://<host>:9090/metrics``.

When ``prometheus-client`` is not installed **or** no metrics port is
configured, all instrumentation calls in the daemon code are no-ops — there is
zero runtime cost.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from gnr.web import logger

if TYPE_CHECKING:
    from prometheus_client import Counter, Gauge, Histogram


class _Metrics:
    """Container for all Prometheus metric objects.

    Created once by :func:`setup`; retrieved everywhere else via :func:`get`.
    """

    def __init__(self) -> None:
        from prometheus_client import Counter, Gauge, Histogram

        self.requests_total: Counter = Counter(
            "gnrd_requests_total",
            "Total RPC method invocations received via TCP",
            ["method", "sitename", "status"],
        )
        self.request_duration_seconds: Histogram = Histogram(
            "gnrd_request_duration_seconds",
            "RPC method execution time in seconds",
            ["method", "sitename"],
            buckets=[
                0.0005,
                0.001,
                0.005,
                0.01,
                0.025,
                0.05,
                0.1,
                0.25,
                0.5,
                1.0,
                2.5,
                5.0,
            ],
        )
        self.active_tcp_connections: Gauge = Gauge(
            "gnrd_active_tcp_connections",
            "Number of currently active TCP connections to the daemon",
        )
        self.sites_total: Gauge = Gauge(
            "gnrd_sites_total",
            "Number of site registers hosted by this daemon",
        )
        self.register_pages: Gauge = Gauge(
            "gnrd_register_pages",
            "Active pages tracked in the site register",
            ["sitename"],
        )
        self.register_connections: Gauge = Gauge(
            "gnrd_register_connections",
            "Active connections tracked in the site register",
            ["sitename"],
        )
        self.register_users: Gauge = Gauge(
            "gnrd_register_users",
            "Active users tracked in the site register",
            ["sitename"],
        )
        self.cleanup_evictions_total: Counter = Counter(
            "gnrd_cleanup_evictions_total",
            "Items evicted by the periodic cleanup loop",
            ["sitename", "register"],
        )


_metrics: _Metrics | None = None


def setup(port: int) -> None:
    """Initialise all metrics and start the Prometheus HTTP server on *port*.

    Raises :exc:`ImportError` if ``prometheus-client`` is not installed.
    """
    global _metrics
    try:
        from prometheus_client import start_http_server
    except ImportError as exc:
        raise ImportError(
            "prometheus-client is not installed. "
            "Run: pip install 'genro-daemon[prometheus]'"
        ) from exc

    _metrics = _Metrics()
    start_http_server(port)

    logger.info("Prometheus metrics available at http://0.0.0.0:%d/metrics", port)


def get() -> _Metrics | None:
    """Return the active metrics container, or *None* if not initialised."""
    return _metrics
