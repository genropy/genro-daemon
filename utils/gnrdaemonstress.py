#!/usr/bin/python3
"""
Benchmark tool for the Genro Daemon.

Runs multiple stress-test scenarios varying:
  - total number of requests
  - number of parallel processes
  - threads per process (concurrent requests within each process)

Each scenario sends a mix of ping and echo calls with randomly generated
payloads (strings, ints, floats, nested dicts).  Results are collected per
request (latency, success/error) and summarised into a report showing
throughput and latency percentiles.

Reports are printed to stdout and optionally saved as JSON, CSV, or TXT.

Usage examples
--------------
# Run the default scenario matrix and save all report formats:
    python utils/gnrdaemonstress.py

# Single custom scenario, print only (no files saved):
    python utils/gnrdaemonstress.py -r 5000 -p 4 -t 8 --no-save

# Only the custom scenario, skipping the built-in matrix:
    python utils/gnrdaemonstress.py -r 2000 -p 2 -t 4 --no-matrix

# Save JSON + CSV reports to a specific directory:
    python utils/gnrdaemonstress.py --output-dir /tmp/bench --formats json,csv
"""

import argparse
import json
import os
import random
import statistics
import string
import sys
import time
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

try:
    from genro_daemon.client import GnrDaemonClient
except ModuleNotFoundError:
    _src = os.path.join(os.path.dirname(os.path.dirname(__file__)), "src")
    sys.path.insert(0, _src)
    from genro_daemon.client import GnrDaemonClient


TOTAL_CORES = os.cpu_count() or 1


# ---------------------------------------------------------------------------
# Backend abstraction — lets the tool drive the old Pyro4 daemon or the new
# msgpack daemon with the same scenario code.
# ---------------------------------------------------------------------------


def _read_gnr_hmac_key() -> str | None:
    """Try to read the hmac_key from the gnr environment config, return None if unavailable."""
    try:
        from gnr.core.gnrbag import Bag
        from gnr.core.gnrconfig import gnrConfigPath

        cfg_path = gnrConfigPath()
        env_xml = os.path.join(cfg_path, "environment.xml")
        if not os.path.exists(env_xml):
            return None
        attrs = Bag(env_xml).getAttr("gnrdaemon") or {}
        return attrs.get("hmac_key")
    except Exception:
        return None


class _Pyro4Client:
    """Thin wrapper around Pyro4.Proxy that matches GnrDaemonClient's call interface."""

    def __init__(
        self,
        host: str,
        port: int,
        timeout: float = 10.0,
        object_name: str = "GnrDaemon",
        hmac_key: str | None = None,
        **kwargs,
    ):
        try:
            import Pyro4
        except ImportError as exc:
            raise RuntimeError(
                "Pyro4 is not installed. Install it with: pip install Pyro4"
            ) from exc
        if hasattr(Pyro4.config, "SERIALIZER"):
            Pyro4.config.SERIALIZER = "pickle"
        if hasattr(Pyro4.config, "METADATA"):
            Pyro4.config.METADATA = False
        if hasattr(Pyro4.config, "REQUIRE_EXPOSE"):
            Pyro4.config.REQUIRE_EXPOSE = False
        # hmac_key: explicit arg wins, then gnr config, then old-daemon default
        key = hmac_key or _read_gnr_hmac_key() or "supersecretkey"
        self._proxy = Pyro4.Proxy(f"PYRO:{object_name}@{host}:{port}")
        self._proxy._pyroHmacKey = key
        self._proxy._pyroTimeout = timeout

    def ping(self):
        return self._proxy.ping()

    def echo(self, payload):
        return self._proxy.echo(payload)

    def __getattr__(self, name):
        return getattr(self._proxy, name)


def _make_client(
    backend: str, host: str, port: int, timeout: float, pool_size: int = 1, **kwargs
) -> Any:
    """Return a client instance for the chosen backend."""
    if backend == "pyro4":
        return _Pyro4Client(host, port, timeout=timeout, **kwargs)
    # Default: new msgpack-based daemon
    return GnrDaemonClient(
        f"gnr://{host}:{port}",
        timeout=timeout,
        pool_size=pool_size if pool_size > 1 else None,
    )


# ---------------------------------------------------------------------------
# Random payload generators
# ---------------------------------------------------------------------------


def _rnd_string(min_len: int = 10, max_len: int = 500) -> str:
    length = random.randint(min_len, max_len)
    return "".join(
        random.choices(
            string.ascii_letters + string.digits + string.punctuation, k=length
        )
    )


def _rnd_payload() -> Any:
    """Return a random payload: string, int, float, or nested dict."""
    kind = random.randint(0, 3)
    if kind == 0:
        return _rnd_string(1, 2000)
    elif kind == 1:
        return random.randint(-(2**31), 2**31 - 1)
    elif kind == 2:
        return round(random.uniform(-1e9, 1e9), 6)
    else:
        return {
            "id": random.randint(1, 10_000),
            "label": _rnd_string(4, 24),
            "score": round(random.uniform(0, 100), 4),
            "tags": [_rnd_string(3, 10) for _ in range(random.randint(1, 6))],
            "active": random.choice([True, False]),
            "nested": {"value": random.randint(0, 255), "name": _rnd_string(2, 8)},
        }


# ---------------------------------------------------------------------------
# Result data classes
# ---------------------------------------------------------------------------


@dataclass
class RequestResult:
    latency: float  # wall-clock seconds for the full request
    success: bool
    error: str | None = None


@dataclass
class ScenarioResult:
    label: str
    total_requests: int
    processes: int
    threads_per_process: int
    wall_time: float
    results: list[RequestResult] = field(default_factory=list)

    @property
    def success_count(self) -> int:
        return sum(1 for r in self.results if r.success)

    @property
    def error_count(self) -> int:
        return len(self.results) - self.success_count

    @property
    def error_rate(self) -> float:
        return self.error_count / len(self.results) if self.results else 0.0

    @property
    def throughput(self) -> float:
        return len(self.results) / self.wall_time if self.wall_time > 0 else 0.0

    @property
    def _latencies(self) -> list[float]:
        return [r.latency for r in self.results if r.success]

    def percentile(self, p: float) -> float:
        ls = sorted(self._latencies)
        if not ls:
            return 0.0
        return ls[min(int(len(ls) * p / 100), len(ls) - 1)]

    def summary(self) -> dict[str, Any]:
        ls = self._latencies
        ms = lambda v: round(v * 1000, 3) if v is not None else None  # noqa: E731
        return {
            "label": self.label,
            "total_requests": len(self.results),
            "processes": self.processes,
            "threads_per_process": self.threads_per_process,
            "wall_time_s": round(self.wall_time, 4),
            "throughput_rps": round(self.throughput, 2),
            "success_count": self.success_count,
            "error_count": self.error_count,
            "error_rate_pct": round(self.error_rate * 100, 2),
            "latency_min_ms": ms(min(ls)) if ls else None,
            "latency_max_ms": ms(max(ls)) if ls else None,
            "latency_mean_ms": ms(statistics.mean(ls)) if ls else None,
            "latency_median_ms": ms(statistics.median(ls)) if ls else None,
            "latency_p95_ms": ms(self.percentile(95)) if ls else None,
            "latency_p99_ms": ms(self.percentile(99)) if ls else None,
            "latency_stddev_ms": ms(statistics.stdev(ls)) if len(ls) > 1 else 0.0,
        }


# ---------------------------------------------------------------------------
# Worker functions (called inside subprocess workers)
# ---------------------------------------------------------------------------


def _single_request(
    host: str,
    port: int,
    timeout: float,
    client=None,
    backend: str = "new",
    client_kwargs: dict = None,
) -> tuple:
    """Execute one ping (+ echo for 'new' backend) round-trip; return (latency, success, error_str)."""
    _client = client or _make_client(
        backend, host, port, timeout, **(client_kwargs or {})
    )
    t0 = time.perf_counter()
    try:
        r = _client.ping()
        if not r:
            raise AssertionError(f"ping returned empty response: {r!r}")
        if backend == "new":
            payload = _rnd_payload()
            r = _client.echo(payload)
            if r != payload:
                raise AssertionError("echo payload mismatch")
        return (time.perf_counter() - t0, True, None)
    except Exception as exc:
        return (time.perf_counter() - t0, False, str(exc))


def _threaded_batch(
    host: str,
    port: int,
    timeout: float,
    req_count: int,
    threads: int,
    backend: str = "new",
    client_kwargs: dict = None,
) -> list[tuple]:
    """Run *req_count* requests using *threads* concurrent threads.

    For the 'new' backend a single pooled client is shared across threads.
    For 'pyro4' each request gets its own proxy (Pyro4 proxies are not thread-safe).
    """
    ckw = client_kwargs or {}
    if backend == "pyro4":
        # Pyro4 proxies are NOT thread-safe — each submitted task must create its own.
        shared_client = None
    else:
        shared_client = _make_client(
            backend, host, port, timeout, pool_size=threads, **ckw
        )
    with ThreadPoolExecutor(max_workers=threads) as pool:
        futures = [
            pool.submit(
                _single_request, host, port, timeout, shared_client, backend, ckw
            )
            for _ in range(req_count)
        ]
        return [f.result() for f in as_completed(futures)]


# ---------------------------------------------------------------------------
# Scenario orchestration
# ---------------------------------------------------------------------------


def run_scenario(
    label: str,
    host: str,
    port: int,
    timeout: float,
    total_requests: int,
    processes: int,
    threads_per_process: int,
    backend: str = "new",
    client_kwargs: dict = None,
) -> ScenarioResult:
    req_per_proc = max(1, total_requests // processes)
    total_threads = processes * threads_per_process
    ckw = client_kwargs or {}

    t0 = time.perf_counter()
    raw: list[tuple] = []

    # Try multi-process mode; fall back to a single thread pool when the
    # runtime environment restricts semaphore creation (e.g. containers).
    if processes > 1:
        try:
            with ProcessPoolExecutor(max_workers=processes) as executor:
                futures = [
                    executor.submit(
                        _threaded_batch,
                        host,
                        port,
                        timeout,
                        req_per_proc,
                        threads_per_process,
                        backend,
                        ckw,
                    )
                    for _ in range(processes)
                ]
                for f in as_completed(futures):
                    raw.extend(f.result())
        except (PermissionError, OSError):
            raw = _threaded_batch(
                host,
                port,
                timeout,
                req_per_proc * processes,
                total_threads,
                backend,
                ckw,
            )
    else:
        raw = _threaded_batch(
            host, port, timeout, req_per_proc, threads_per_process, backend, ckw
        )

    wall_time = time.perf_counter() - t0

    results = [
        RequestResult(latency=lat, success=ok, error=err) for lat, ok, err in raw
    ]
    return ScenarioResult(
        label=label,
        total_requests=req_per_proc * processes,
        processes=processes,
        threads_per_process=threads_per_process,
        wall_time=wall_time,
        results=results,
    )


# ---------------------------------------------------------------------------
# Report rendering
# ---------------------------------------------------------------------------

_W = dict(label=36, reqs=6, proc=5, thr=4, time=8, rps=8, err=6, ms=8)

_HEADER = (
    f"{'Scenario':<{_W['label']}} "
    f"{'Reqs':>{_W['reqs']}} "
    f"{'Proc':>{_W['proc']}} "
    f"{'Thr':>{_W['thr']}} "
    f"{'Time(s)':>{_W['time']}} "
    f"{'RPS':>{_W['rps']}} "
    f"{'Err%':>{_W['err']}} "
    f"{'Min ms':>{_W['ms']}} "
    f"{'Median':>{_W['ms']}} "
    f"{'P95':>{_W['ms']}} "
    f"{'P99':>{_W['ms']}} "
    f"{'Max ms':>{_W['ms']}}"
)
_SEP = "-" * len(_HEADER)


def _fmt_row(s: dict[str, Any]) -> str:
    def ms(v) -> str:
        return f"{v:.1f}" if v is not None else "N/A"

    return (
        f"{s['label']:<{_W['label']}} "
        f"{s['total_requests']:>{_W['reqs']}} "
        f"{s['processes']:>{_W['proc']}} "
        f"{s['threads_per_process']:>{_W['thr']}} "
        f"{s['wall_time_s']:>{_W['time']}.2f} "
        f"{s['throughput_rps']:>{_W['rps']}.1f} "
        f"{s['error_rate_pct']:>{_W['err']}.1f} "
        f"{ms(s['latency_min_ms']):>{_W['ms']}} "
        f"{ms(s['latency_median_ms']):>{_W['ms']}} "
        f"{ms(s['latency_p95_ms']):>{_W['ms']}} "
        f"{ms(s['latency_p99_ms']):>{_W['ms']}} "
        f"{ms(s['latency_max_ms']):>{_W['ms']}}"
    )


def _report_lines(
    summaries: list[dict[str, Any]], timestamp: str, backend: str = "new"
) -> list[str]:
    return [
        "=" * len(_HEADER),
        f"BENCHMARK REPORT — Genro Daemon  [backend: {backend}]",
        f"Generated : {timestamp}",
        "=" * len(_HEADER),
        "",
        _HEADER,
        _SEP,
        *[_fmt_row(s) for s in summaries],
        _SEP,
        "",
    ]


def print_report(summaries: list[dict[str, Any]], backend: str = "new") -> None:
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print("\n" + "\n".join(_report_lines(summaries, ts, backend=backend)))


def save_report(
    summaries: list[dict[str, Any]],
    output_dir: str,
    formats: list[str],
    backend: str = "new",
) -> None:
    ts_label = datetime.now().strftime("%Y%m%d_%H%M%S")
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    tag = f"{backend}_{ts_label}"

    if "json" in formats:
        path = out / f"benchmark_{tag}.json"
        with open(path, "w") as f:
            json.dump(
                {
                    "generated": datetime.now().isoformat(),
                    "backend": backend,
                    "scenarios": summaries,
                },
                f,
                indent=2,
            )
        print(f"  JSON report: {path}")

    if "csv" in formats:
        import csv

        path = out / f"benchmark_{tag}.csv"
        with open(path, "w", newline="") as f:
            if summaries:
                writer = csv.DictWriter(f, fieldnames=summaries[0].keys())
                writer.writeheader()
                writer.writerows(summaries)
        print(f"  CSV  report: {path}")

    if "txt" in formats:
        path = out / f"benchmark_{tag}.txt"
        path.write_text(
            "\n".join(
                _report_lines(summaries, datetime.now().isoformat(), backend=backend)
            )
        )
        print(f"  TXT  report: {path}")


# ---------------------------------------------------------------------------
# Default scenario matrix
# (total_requests, processes, threads_per_process)
# ---------------------------------------------------------------------------

DEFAULT_MATRIX = [
    (100, 1, 1),  # baseline: single thread, tiny load
    (500, 1, 4),  # light load, modest concurrency
    (1_000, 1, 8),  # medium load, single process
    (1_000, 2, 4),  # medium load, two processes
    (5_000, 2, 8),  # heavy load, two processes
    (5_000, 4, 8),  # heavy load, four processes
    (10_000, 4, 16),  # stress, four processes
    (10_000, 8, 16),  # stress, eight processes
]


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Register data scenario: load connections/pages, query, verify consistency
# ---------------------------------------------------------------------------


def _make_connection_id() -> str:
    return "".join(random.choices(string.ascii_lowercase + string.digits, k=16))


def _make_page_id() -> str:
    return "".join(random.choices(string.ascii_lowercase + string.digits, k=12))


def run_register_scenario(
    host: str,
    port: int,
    timeout: float,
    sitename: str,
    num_connections: int,
    pages_per_connection: int,
    num_users: int = 10,
    teardown: bool = True,
    backend: str = "new",
) -> dict[str, Any]:
    """Create connections and pages in the site register, then query and verify.

    Returns a summary dict with counts and any consistency errors detected.
    Only supported with ``backend='new'``.
    """
    if backend != "new":
        print(f"  [register scenario] skipped — not supported for backend={backend!r}")
        return {"sitename": sitename, "skipped": True, "consistency_errors": []}
    client = GnrDaemonClient(f"gnr://{host}:{port}", sitename=sitename, timeout=timeout)

    t_setup_start = time.perf_counter()

    # Register the site (idempotent if already present)
    client.addSiteRegister(sitename)
    client.setConfiguration()

    # --- Load phase ---
    connection_ids: list[str] = []
    page_ids: list[str] = []
    users: list[str] = [f"user_{i:04d}" for i in range(max(1, num_users))]

    # Register all users explicitly so they appear in the UserRegister
    # regardless of how many connections each user receives.
    for user in users:
        client.new_user(user, user_id=f"uid_{user}", user_tags="guest")

    for _ in range(num_connections):
        conn_id = _make_connection_id()
        user = random.choice(users)
        client.new_connection(
            conn_id,
            user=user,
            user_id=f"uid_{user}",
            user_tags="guest",
        )
        connection_ids.append(conn_id)

        for _ in range(pages_per_connection):
            page_id = _make_page_id()
            client.new_page(
                page_id,
                pagename=f"page_{random.randint(1, 100)}.py",
                connection_id=conn_id,
                user=user,
            )
            page_ids.append(page_id)

    t_load = time.perf_counter() - t_setup_start

    # --- Query phase ---
    t_query_start = time.perf_counter()

    counters = client.counters()

    t_query = time.perf_counter() - t_query_start

    # --- Consistency checks ---
    errors: list[str] = []

    expected_pages = num_connections * pages_per_connection

    if counters.get("users") != num_users:
        errors.append(f"counters.users={counters.get('users')} expected={num_users}")
    if counters.get("connections") != num_connections:
        errors.append(
            f"counters.connections={counters.get('connections')} "
            f"expected={num_connections}"
        )
    if counters.get("pages") != expected_pages:
        errors.append(
            f"counters.pages={counters.get('pages')} expected={expected_pages}"
        )

    # Spot-check: get_item returns correct data for a sample of items
    sample_size = min(10, len(connection_ids))
    for conn_id in random.sample(connection_ids, sample_size):
        item = client.get_item(conn_id, register_name="connection")
        if item is None:
            errors.append(f"get_item(connection={conn_id!r}) returned None")
        elif item.get("register_item_id") != conn_id:
            errors.append(f"get_item id mismatch for {conn_id!r}")

    sample_size = min(10, len(page_ids))
    for page_id in random.sample(page_ids, sample_size):
        item = client.get_item(page_id, register_name="page")
        if item is None:
            errors.append(f"get_item(page={page_id!r}) returned None")
        elif item.get("register_item_id") != page_id:
            errors.append(f"get_item id mismatch for {page_id!r}")

    # --- Teardown: drop everything (skipped when teardown=False) ---
    t_drop_start = time.perf_counter()
    if teardown:
        for page_id in page_ids:
            client.drop_page(page_id)
        for conn_id in connection_ids:
            client.drop_connection(conn_id)
        for user in users:
            client.drop_user(user)

        counters_after = client.counters()
        if counters_after.get("users", -1) != 0:
            errors.append(
                f"counters.users after drop={counters_after.get('users')} expected=0"
            )
        if counters_after.get("pages", -1) != 0:
            errors.append(
                f"counters.pages after drop={counters_after.get('pages')} expected=0"
            )
        if counters_after.get("connections", -1) != 0:
            errors.append(
                f"counters.connections after drop={counters_after.get('connections')} expected=0"
            )
    t_drop = time.perf_counter() - t_drop_start

    return {
        "sitename": sitename,
        "connections": num_connections,
        "users": num_users,
        "pages_per_connection": pages_per_connection,
        "total_pages": expected_pages,
        "load_time_s": round(t_load, 4),
        "query_time_s": round(t_query, 4),
        "drop_time_s": round(t_drop, 4),
        "consistency_errors": errors,
    }


def print_register_report(results: list[dict[str, Any]]) -> None:
    print("\n" + "=" * 70)
    print("REGISTER DATA SCENARIO — Genro Daemon")
    print("=" * 70)
    for r in results:
        status = (
            "OK"
            if not r["consistency_errors"]
            else f"ERRORS({len(r['consistency_errors'])})"
        )
        print(
            f"  site={r['sitename']!r:<20}  "
            f"conns={r['connections']:>4}  "
            f"users={r['users']:>4}  "
            f"pages={r['total_pages']:>5}  "
            f"load={r['load_time_s']:.2f}s  "
            f"query={r['query_time_s']:.2f}s  "
            f"drop={r['drop_time_s']:.2f}s  "
            f"[{status}]"
        )
        for e in r["consistency_errors"]:
            print(f"    !! {e}")
    print("=" * 70 + "\n")


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Benchmark the Genro Daemon across multiple load scenarios.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    conn = p.add_argument_group("connection")
    conn.add_argument("--host", default="127.0.0.1", help="Daemon host")
    conn.add_argument("--port", default=40404, type=int, help="Daemon port")
    conn.add_argument(
        "--timeout", default=10.0, type=float, help="Per-request timeout in seconds"
    )
    conn.add_argument(
        "--backend",
        default="new",
        choices=["new", "pyro4"],
        help="Client backend: 'new' (msgpack/genro-daemon) or 'pyro4' (old Pyro4 daemon)",
    )
    conn.add_argument(
        "--pyro4-object",
        default="GnrDaemon",
        metavar="NAME",
        help="Pyro4 object name registered in the old daemon (default: GnrDaemon)",
    )
    conn.add_argument(
        "--hmac-key",
        default=None,
        metavar="KEY",
        help=(
            "HMAC key for the Pyro4 backend. If omitted, auto-read from the gnr "
            "environment config (gnrdaemon.hmac_key), falling back to 'supersecretkey'."
        ),
    )

    single = p.add_argument_group("single scenario")
    single.add_argument(
        "-r",
        "--requests",
        type=int,
        default=None,
        help="Total requests for a custom single scenario",
    )
    single.add_argument(
        "-p",
        "--processes",
        type=int,
        default=None,
        help="Processes for single scenario (default: cpu_count)",
    )
    single.add_argument(
        "-t",
        "--threads",
        type=int,
        default=4,
        help="Threads per process for single scenario",
    )

    reg = p.add_argument_group("register scenario")
    reg.add_argument(
        "--register-scenario",
        action="store_true",
        help="Run the register data scenario (load/query/verify)",
    )
    reg.add_argument(
        "--reg-sitename",
        default="stress_site",
        help="Site name to use for the register scenario",
    )
    reg.add_argument(
        "--reg-connections",
        type=int,
        default=20,
        help="Number of connections to create in the register scenario",
    )
    reg.add_argument(
        "--reg-pages-per-connection",
        type=int,
        default=5,
        help="Pages per connection in the register scenario",
    )
    reg.add_argument(
        "--reg-users",
        type=int,
        default=10,
        help="Number of distinct users in the register scenario",
    )
    reg.add_argument(
        "--reg-no-teardown",
        action="store_true",
        help="Skip teardown (leave data in the register/Redis after the scenario)",
    )
    reg.add_argument(
        "--no-benchmark",
        action="store_true",
        help="Skip the ping/echo benchmark (run register scenario only)",
    )

    output = p.add_argument_group("output")
    output.add_argument(
        "--no-matrix", action="store_true", help="Skip the built-in scenario matrix"
    )
    output.add_argument(
        "--output-dir",
        default="benchmark_reports",
        help="Directory for saved report files",
    )
    output.add_argument(
        "--formats",
        default="json,csv,txt",
        help="Comma-separated report formats: json,csv,txt",
    )
    output.add_argument(
        "--no-save", action="store_true", help="Do not save any report files to disk"
    )
    return p


def main() -> None:
    args = _build_parser().parse_args()

    formats = [f.strip().lower() for f in args.formats.split(",")]
    backend = args.backend
    pyro4_kwargs = (
        {"object_name": args.pyro4_object, "hmac_key": args.hmac_key}
        if backend == "pyro4"
        else {}
    )

    if backend == "pyro4":
        effective_key = args.hmac_key or _read_gnr_hmac_key() or "supersecretkey"
        print(f"Backend: pyro4  object={args.pyro4_object}  hmac_key={effective_key!r}")
    else:
        print("Backend: new")

    # -----------------------------------------------------------------------
    # Register data scenario
    # -----------------------------------------------------------------------
    if args.register_scenario:
        print(
            f"Register scenario: site={args.reg_sitename!r}  "
            f"connections={args.reg_connections}  "
            f"pages/conn={args.reg_pages_per_connection}  "
            f"users={args.reg_users}"
        )
        reg_result = run_register_scenario(
            host=args.host,
            port=args.port,
            timeout=args.timeout,
            sitename=args.reg_sitename,
            num_connections=args.reg_connections,
            pages_per_connection=args.reg_pages_per_connection,
            num_users=args.reg_users,
            teardown=not args.reg_no_teardown,
            backend=backend,
        )
        if not reg_result.get("skipped"):
            print_register_report([reg_result])
            if reg_result["consistency_errors"]:
                sys.exit(2)

    if args.no_benchmark:
        return

    # -----------------------------------------------------------------------
    # Ping/echo benchmark
    # -----------------------------------------------------------------------
    scenarios: list[dict[str, Any]] = []

    if not args.no_matrix:
        for i, (reqs, procs, threads) in enumerate(DEFAULT_MATRIX, 1):
            scenarios.append(
                {
                    "label": f"S{i:02d}_r{reqs}_p{procs}_t{threads}",
                    "total_requests": reqs,
                    "processes": procs,
                    "threads_per_process": threads,
                }
            )

    if args.requests is not None:
        scenarios.append(
            {
                "label": "custom",
                "total_requests": args.requests,
                "processes": args.processes or TOTAL_CORES,
                "threads_per_process": args.threads,
            }
        )

    if not scenarios:
        if not args.register_scenario:
            print(
                "Nothing to run. Provide --requests, --register-scenario, or remove --no-matrix."
            )
            sys.exit(1)
        return

    print(
        f"Benchmarking daemon at {args.host}:{args.port} — {len(scenarios)} scenario(s)"
    )
    print()

    summaries: list[dict[str, Any]] = []
    for sc in scenarios:
        print(
            f"  [{sc['label']}]  "
            f"{sc['total_requests']} reqs, "
            f"{sc['processes']} proc × {sc['threads_per_process']} threads … ",
            end="",
            flush=True,
        )
        result = run_scenario(
            label=sc["label"],
            host=args.host,
            port=args.port,
            timeout=args.timeout,
            total_requests=sc["total_requests"],
            processes=sc["processes"],
            threads_per_process=sc["threads_per_process"],
            backend=backend,
            client_kwargs=pyro4_kwargs,
        )
        s = result.summary()
        summaries.append(s)
        print(
            f"{s['throughput_rps']:.0f} rps  "
            f"p50={s['latency_median_ms']} ms  "
            f"p99={s['latency_p99_ms']} ms  "
            f"errors={s['error_count']}"
        )

    print_report(summaries, backend=backend)

    if not args.no_save:
        print("Saving reports…")
        save_report(summaries, args.output_dir, formats, backend=backend)
        print()


if __name__ == "__main__":
    main()
