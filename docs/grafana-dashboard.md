# Grafana Dashboard — Genro Daemon

`docs/grafana-dashboard.json` is a ready-to-import Grafana dashboard that
visualises every metric exported by the daemon's Prometheus endpoint.

---

## Prerequisites

1. **Prometheus** must be scraping the daemon's metrics endpoint.  See
   [environment-variables.md](environment-variables.md) for how to enable it:

   ```sh
   pip install "genro-daemon[prometheus]"
   export GNR_DAEMON_METRICS_PORT=9090
   gnrdaemon start
   ```

2. **Grafana ≥ 10.0** with a Prometheus data source already configured.

---

## Importing the dashboard

1. In Grafana, go to **Dashboards → Import**.
2. Click **Upload dashboard JSON file** and select `docs/grafana-dashboard.json`.
3. In the **Prometheus** field, choose the data source that scrapes the daemon.
4. Click **Import**.

The dashboard opens immediately.  Use the **Site** drop-down at the top to
filter panels to one or more specific sites.

---

## Layout

The dashboard is divided into five collapsible rows.

### Overview

Four stat panels showing the current state at a glance.

| Panel | Metric | Notes |
|---|---|---|
| **Request Rate** | `gnrd_requests_total` | Requests per second across all sites and methods. Shown with a mini sparkline. |
| **Error Rate** | `gnrd_requests_total{status="error"}` | Percentage of requests that ended in an error. Background turns yellow above 1 % and red above 5 %. |
| **Active TCP Connections** | `gnrd_active_tcp_connections` | Open TCP sockets to the daemon right now. |
| **Registered Sites** | `gnrd_sites_total` | Number of site registers hosted. |

### Request Throughput

| Panel | Queries | Description |
|---|---|---|
| **Requests per Second by Method** | `rate(gnrd_requests_total[…])` grouped by `method` | One line per RPC method (e.g. `new_page`, `refresh`, `handle_ping`). Useful for spotting which call dominates load. |
| **Error Rate by Site** | `rate(gnrd_requests_total{status="error"}[…])` grouped by `sitename` | Errors/s broken down per site. A flat zero line is the healthy baseline. |

### Latency

| Panel | Queries | Description |
|---|---|---|
| **Request Latency Percentiles** | `histogram_quantile(0.50/0.95/0.99, …)` | p50, p95, and p99 aggregated over all methods and selected sites. Shows the shape of the latency distribution over time. |
| **p95 Latency by Method** | `histogram_quantile(0.95, … by (le, method))` | Per-method p95. Helps isolate slow calls — for example, `handle_ping` sends data changes back to the browser and can be heavier than a plain `refresh`. |

Latency values are in **seconds**.  Typical healthy values are sub-millisecond
for `ping`/`echo` and 1–20 ms for register operations under normal load (see
the [benchmark reference](benchmarking.md) for baseline numbers).

### Site Register

Three time-series panels, one per register type, each broken down by
`sitename`.

| Panel | Metric | Description |
|---|---|---|
| **Active Pages** | `gnrd_register_pages` | Each open browser tab that has called `new_page`. Drops after `page_max_age` seconds of inactivity (default 120 s). |
| **Active Connections** | `gnrd_register_connections` | Authenticated WebSocket sessions. Drops after `connection_max_age` (default 600 s) without a refresh. |
| **Active Users** | `gnrd_register_users` | Distinct users with at least one active connection. A user is removed when their last connection is dropped. |

Watching these gauges over a day reveals usage patterns: peaks during business
hours, drops overnight.  A sudden drop in all three simultaneously usually
indicates a daemon restart.

### Cleanup

| Panel | Metric | Description |
|---|---|---|
| **Cleanup Evictions Rate** | `gnrd_cleanup_evictions_total` | Items evicted per second by the periodic cleanup loop, labelled by `sitename` and `register` (`page` or `connection`). A sustained non-zero rate is expected during normal operation; a spike may mean clients lost connectivity silently. |

---

## Template variable

| Variable | Source | Description |
|---|---|---|
| **Site** (`$sitename`) | `label_values(gnrd_requests_total, sitename)` | Multi-select filter. Select one site to focus all panels on that site, or leave on **All** to aggregate across every site. The variable refreshes every time the dashboard time range changes. |

---

## Recommended alert rules

The following PromQL expressions are good starting points for Grafana alerts.
Adjust thresholds to match your deployment.

### High error rate

```promql
100 * sum(rate(gnrd_requests_total{status="error"}[5m]))
    / sum(rate(gnrd_requests_total[5m]))
> 5
```

Fires when more than 5 % of requests fail over a 5-minute window.

### p99 latency above threshold

```promql
histogram_quantile(
  0.99,
  sum by (le) (rate(gnrd_request_duration_seconds_bucket[5m]))
) > 0.5
```

Fires when p99 latency exceeds 500 ms.  Based on the [benchmark
baseline](benchmarking.md#reference-benchmark), healthy p99 is well under
100 ms up to 64 concurrent connections.

### No requests received (daemon down or not scraped)

```promql
absent(gnrd_requests_total)
```

Fires when the metric disappears from Prometheus entirely — either the daemon
stopped or the scrape job is broken.

### Cleanup evictions spike

```promql
sum(rate(gnrd_cleanup_evictions_total[5m])) > 10
```

Fires when the cleanup loop evicts more than 10 items/s, which may indicate
mass client disconnections.

---

## Scrape configuration reference

Minimal Prometheus configuration to scrape the daemon:

```yaml
scrape_configs:
  - job_name: genro-daemon
    scrape_interval: 10s
    static_configs:
      - targets:
          - "localhost:9090"   # GNR_DAEMON_METRICS_PORT
```

For multi-host deployments, use `file_sd_configs` or a service-discovery
mechanism and add a `daemon_host` label to distinguish instances.
