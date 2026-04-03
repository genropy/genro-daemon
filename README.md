# genro-daemon

A high-performance TCP daemon for the [Genropy](https://genropy.org)
web framework.  It replaces the legacy Pyro4-based daemon with a
modern asyncio + uvloop stack using msgpack for efficient binary
serialisation.

## Features

- **Single-port multi-tenant** — one daemon hosts all site registers on one TCP port
- **Pluggable storage** — in-memory (default) or Redis for distributed / persistent deployments
- **Optional Prometheus metrics** — zero-cost when disabled, full histogram + counter suite when enabled
- **Process management** — cron and worker pools per site, with automatic restart on failure
- **Production-ready** — SIGTERM handling, connection pooling, configurable cleanup

## Requirements

- Python 3.11+
- [genropy](https://genropy.org)
- `msgpack`, `uvloop`

## Installation

```bash
pip install genro-daemon
```

With optional extras:

```bash
# Redis backend
pip install "genro-daemon[redis]"

# Prometheus metrics
pip install "genro-daemon[prometheus]"

# Both
pip install "genro-daemon[redis,prometheus]"
```

## Quickstart

Start the daemon on the default host/port (`localhost:40404`):

```bash
gnr web daemon
```

Start with custom host and port:

```bash
gnr web daemon -H 0.0.0.0 -P 9000
```

Start with Redis storage backend:

```bash
GNR_DAEMON_STORE=redis://localhost:6379/0 gnr web daemon
```

Start with Prometheus metrics:

```bash
GNR_DAEMON_METRICS_PORT=9090 gnr web daemon
```

## Configuration

The daemon resolves configuration from these sources, in order of precedence:

1. **CLI flags** (`-H`, `-P`, `-K`, …)
2. **Environment variables** (`GNR_DAEMON_HOST`, `GNR_DAEMON_PORT`, `GNR_DAEMON_BIND`, `GNR_DAEMON_STORE`, …)
3. **`environment.xml`** — Genropy config file, `<gnrdaemon>` node
4. **Built-in defaults** (`localhost:40404`, in-memory backend)

See [`docs/environment-variables.md`](docs/environment-variables.md) for the full reference.

## Storage backends

| Backend | URL format | Use case |
|---------|-----------|----------|
| In-memory (default) | `memory:` | Single-process, no persistence |
| Redis | `redis://host:port/db` | Multi-process, persistence, HA |

Set via the `GNR_DAEMON_STORE` environment variable or the `store` key in `environment.xml`.

## Prometheus metrics

Install `genro-daemon[prometheus]` and set `GNR_DAEMON_METRICS_PORT`:

```bash
GNR_DAEMON_METRICS_PORT=9090 gnr web daemon
```

Prometheus scrapes `http://<host>:9090/metrics`.
A ready-made Grafana dashboard is documented in [`docs/grafana-dashboard.md`](docs/grafana-dashboard.md).

## Performance

Reference benchmarks on a single process with in-memory backend:

| Scenario | Throughput | p99 latency |
|----------|-----------|-------------|
| 1 client, sequential | ~4,500 req/s | ~0.4 ms |
| 10 clients, concurrent | ~6,300 req/s | ~3 ms |

See [`docs/benchmarking.md`](docs/benchmarking.md) for full methodology and results.

## Development

```bash
# Install in editable mode with all extras
pip install -e ".[redis,prometheus]"

# Run tests (requires a Redis instance on localhost:6379)
pytest

# Lint
ruff check src/ tests/
ruff format --check src/ tests/
```

## Documentation

- [`docs/environment-variables.md`](docs/environment-variables.md) — all configuration options
- [`docs/security.md`](docs/security.md) — HMAC authentication, network hardening, deployment checklist
- [`docs/benchmarking.md`](docs/benchmarking.md) — stress-test tool and reference results
- [`docs/grafana-dashboard.md`](docs/grafana-dashboard.md) — Prometheus + Grafana setup

## License

Apache License 2.0 — see [LICENSE](LICENSE).
