# Environment Variables

All `GNR_DAEMON_*` variables take precedence over values read from the site XML
configuration.  They are useful in containerised or twelve-factor deployments
where external config files are not available.

---

## Network / connection configuration

| Variable | Default | Description |
|---|---|---|
| `GNR_DAEMON_HOST` | `localhost` | Hostname or IP that **clients** use to connect to the daemon. |
| `GNR_DAEMON_PORT` | `40404` | TCP port the daemon listens on and that clients connect to. |
| `GNR_DAEMON_BIND` | _(same as `host`)_ | Address the daemon's TCP server binds to. Set to `0.0.0.0` to accept connections from all interfaces while keeping `GNR_DAEMON_HOST` at the actual IP clients should use. |

### Examples

```sh
# Bind to all interfaces; clients connect via the machine's public IP
GNR_DAEMON_BIND=0.0.0.0 GNR_DAEMON_HOST=192.168.1.10 GNR_DAEMON_PORT=40404 gnr web daemon

# Non-default port
GNR_DAEMON_PORT=9000 gnr web daemon
```

---

## Storage backend

| Variable | Default | Description |
|---|---|---|
| `GNR_DAEMON_STORE` | `memory:` | Storage backend URL. Use `memory:` for in-process storage or a Redis URL for persistence and multi-process sharing. |

### URL format

```
memory:
redis://[user:password@]host[:port][/db][?prefix=<prefix>]
```

| Component | Description |
|---|---|
| `memory:` | In-process backend. Suitable for single-process deployments. No persistence. |
| `redis://…` | Redis backend. Requires `pip install "genro-daemon[redis]"`. |
| `user` | Optional. The conventional placeholder `default` is ignored; any other value is passed as the Redis username. |
| `password` | Optional Redis AUTH password. |
| `host` | Redis hostname or IP (default `localhost`). |
| `port` | Redis TCP port (default `6379`). |
| `db` | Redis database index (default `0`). |
| `prefix` | Query-string parameter. Key prefix prepended to every key (default `gnrd:`). The site name is appended automatically, e.g. `gnrd:mysite:`, so multiple sites can share one Redis instance without collisions. |

### Examples

```sh
# In-memory (default)
GNR_DAEMON_STORE=memory:

# Redis, no authentication
GNR_DAEMON_STORE=redis://localhost:6379/0

# Redis with password
GNR_DAEMON_STORE=redis://:secret@redis-host:6379/0

# Redis with password and custom key prefix
GNR_DAEMON_STORE=redis://:secret@redis-host:6379/0?prefix=prod:
```

---

## Prometheus metrics

| Variable | Default | Description |
|---|---|---|
| `GNR_DAEMON_METRICS_PORT` | _(none)_ | TCP port for the Prometheus HTTP metrics server. When unset, no server is started and no metrics are collected. Requires `pip install "genro-daemon[prometheus]"`. |

Metrics are exposed at `http://<host>:<GNR_DAEMON_METRICS_PORT>/metrics` in the standard Prometheus text format.

### Exposed metrics

| Metric | Type | Labels | Description |
|---|---|---|---|
| `gnrd_requests_total` | Counter | `method`, `sitename`, `status` | Total RPC requests processed. `status` is `ok` or `error`. |
| `gnrd_request_duration_seconds` | Histogram | `method`, `sitename` | RPC request processing time. |
| `gnrd_active_tcp_connections` | Gauge | — | Active TCP connections to the daemon. |
| `gnrd_sites_total` | Gauge | — | Number of site registers hosted. |
| `gnrd_register_pages` | Gauge | `sitename` | Active pages tracked per site. |
| `gnrd_register_connections` | Gauge | `sitename` | Active connections tracked per site. |
| `gnrd_register_users` | Gauge | `sitename` | Active users tracked per site. |
| `gnrd_cleanup_evictions_total` | Counter | `sitename`, `register` | Items evicted by the periodic cleanup loop. `register` is `page` or `connection`. |

### Example

```sh
GNR_DAEMON_METRICS_PORT=9090 gnrdaemon start
```

Prometheus scrape config:

```yaml
scrape_configs:
  - job_name: genro-daemon
    static_configs:
      - targets: ["localhost:9090"]
```

---

## HMAC authentication

| Variable | Default | Description |
|---|---|---|
| `GNR_DAEMON_HMAC_KEY` | _(none)_ | Shared secret used to authenticate messages. Must match the key configured in all clients. When unset, authentication is disabled (safe only on loopback / fully trusted networks). |

See [`security.md`](security.md) for full details and key-management guidance.

---

## CLI flag equivalents

| `environment.xml` attribute | CLI flag | Environment variable | Default | Description |
|---|---|---|---|---|
| `host` | `-H` / `--host` | `GNR_DAEMON_HOST` | `localhost` | Host clients connect to. |
| `port` | `-P` / `--port` | `GNR_DAEMON_PORT` | `40404` | TCP port (daemon and clients). |
| `bind` | _(n/a)_ | `GNR_DAEMON_BIND` | _(same as host)_ | Address the daemon's server socket binds to. |
| `hmac_key` | `-K` / `--hmac_key` | `GNR_DAEMON_HMAC_KEY` | _(none)_ | HMAC shared secret. |

---

## Site register cleanup

These values are set per-site via the site XML configuration (`<cleanup>` node
inside the site config, passed to `GnrSiteRegister.setConfiguration()`).
There are no environment variable overrides for these; they are documented here
for operational reference.

| Parameter | Default | Description |
|---|---|---|
| `interval` | `120` s | How often the cleanup loop runs (seconds). |
| `page_max_age` | `120` s | A page (browser tab) inactive for longer than this is evicted from the register. |
| `connection_max_age` | `600` s | An authenticated WebSocket connection inactive for longer than this is evicted. |
| `guest_connection_max_age` | `40` s | Same as above but for unauthenticated (guest) connections. |

---

## Precedence

When the same parameter is specified in multiple places, the resolution order
is (highest priority first):

1. CLI flags (`--host`, `--port`, …)
2. `GNR_DAEMON_*` environment variables
3. `environment.xml` / site XML configuration
4. Built-in defaults

---

## Example: Redis deployment with Docker

```sh
docker run \
  -e GNR_DAEMON_STORE=redis://:secret@redis:6379/0 \
  -e GNR_DAEMON_BIND=0.0.0.0 \
  -p 40404:40404 \
  myorg/genro-daemon
```
