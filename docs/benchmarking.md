# Benchmarking the Genro Daemon

`utils/gnrdaemonstress.py` is a multi-level stress-test tool that hammers the
daemon with concurrent requests and produces latency/throughput reports.  It
supports both the new msgpack-based daemon and the old Pyro4-based daemon, so
the two implementations can be compared side-by-side.

---

## How it works

Each benchmark *scenario* is defined by three parameters:

| Parameter | Meaning |
|---|---|
| **total requests** | Total number of request round-trips to execute |
| **processes** | Number of OS processes spawned in parallel |
| **threads per process** | Number of concurrent threads inside each process |

Effective concurrency = `processes × threads_per_process`.

Every individual request performs:

- **`new` backend** — `ping` (assert reply is non-empty) + `echo(payload)`
  (assert the reply matches the sent payload)
- **`pyro4` backend** — `ping` only (the old daemon does not expose `echo`)

Payloads are randomly generated on each call and can be:

- A random ASCII string (1–2000 characters)
- A random integer (`int32` range)
- A random float
- A nested dict with id, label, score, tags, active flag, and a nested sub-object

Request latency is measured with `time.perf_counter()` for each round-trip.
After all scenarios complete a summary table is printed and (optionally) saved
to disk.

---

## Running the tool

### Prerequisites

- The daemon must be running and accepting connections.
- Run from the repo root with the package on `PYTHONPATH`:

```sh
PYTHONPATH=src python3 utils/gnrdaemonstress.py
```

Or if the package is installed in the active virtual environment:

```sh
python3 utils/gnrdaemonstress.py
```

### Default matrix

Running without arguments executes the built-in scenario matrix against the
new daemon:

```sh
python3 utils/gnrdaemonstress.py
```

This runs eight pre-defined scenarios that sweep across request volume and
concurrency levels (see the [Default scenario matrix](#default-scenario-matrix)
section below).

### Custom single scenario

```sh
# 5 000 requests, 4 processes, 8 threads each
python3 utils/gnrdaemonstress.py -r 5000 -p 4 -t 8

# Same but skip the built-in matrix
python3 utils/gnrdaemonstress.py -r 5000 -p 4 -t 8 --no-matrix
```

### Targeting a remote daemon

```sh
python3 utils/gnrdaemonstress.py --host 10.0.0.5 --port 40404
```

### Comparing new vs old (Pyro4) daemon

Use `--backend pyro4` to drive an old Pyro4-based daemon running on the same
or a different port.  The HMAC key can be supplied explicitly or will be
auto-read from `environment.xml` (`gnrdaemon.hmac_key`):

```sh
# New daemon (default)
python3 utils/gnrdaemonstress.py --backend new -r 1000 -p 1 -t 8 --no-matrix

# Old Pyro4 daemon on port 40405 with explicit HMAC key
python3 utils/gnrdaemonstress.py --backend pyro4 --port 40405 \
    --hmac-key mysecret -r 1000 -p 1 -t 8 --no-matrix
```

### Register data scenario

The register scenario creates connections, pages, and users in the site
register, then queries and verifies consistency.  It is only supported with
the `new` backend.

```sh
python3 utils/gnrdaemonstress.py --register-scenario \
    --reg-sitename mysite --reg-connections 50 --reg-pages-per-connection 5
```

Run the register scenario without the ping/echo benchmark:

```sh
python3 utils/gnrdaemonstress.py --register-scenario --no-benchmark
```

### Saving reports

By default reports are saved to `benchmark_reports/` in JSON, CSV, and TXT
format.  The filename includes the backend name and a timestamp:

```
benchmark_reports/
  benchmark_new_20260326_165030.json
  benchmark_new_20260326_165030.csv
  benchmark_new_20260326_165030.txt
```

To control output:

```sh
# Only JSON and CSV
python3 utils/gnrdaemonstress.py --formats json,csv

# Custom directory
python3 utils/gnrdaemonstress.py --output-dir /var/log/bench

# Skip saving entirely
python3 utils/gnrdaemonstress.py --no-save
```

---

## CLI reference

```
usage: gnrdaemonstress.py [-h]
       [--host HOST] [--port PORT] [--timeout TIMEOUT]
       [--backend {new,pyro4}] [--pyro4-object NAME] [--hmac-key KEY]
       [-r REQUESTS] [-p PROCESSES] [-t THREADS]
       [--register-scenario] [--reg-sitename NAME]
       [--reg-connections N] [--reg-pages-per-connection N]
       [--reg-users N] [--reg-no-teardown] [--no-benchmark]
       [--no-matrix] [--output-dir OUTPUT_DIR]
       [--formats FORMATS] [--no-save]

connection:
  --host HOST               Daemon host (default: 127.0.0.1)
  --port PORT               Daemon port (default: 40404)
  --timeout TIMEOUT         Per-request timeout in seconds (default: 10.0)
  --backend {new,pyro4}     Client backend: 'new' (msgpack/genro-daemon) or
                            'pyro4' (old Pyro4 daemon) (default: new)
  --pyro4-object NAME       Pyro4 registered object name (default: GnrDaemon)
  --hmac-key KEY            HMAC key for the Pyro4 backend. Auto-read from
                            environment.xml if omitted.

single scenario:
  -r, --requests            Total requests for a custom single scenario
  -p, --processes           Processes for single scenario (default: cpu_count)
  -t, --threads             Threads per process for single scenario (default: 4)

register scenario:
  --register-scenario       Run the register data scenario (load/query/verify)
  --reg-sitename NAME       Site name for the register scenario (default: stress_site)
  --reg-connections N       Connections to create (default: 20)
  --reg-pages-per-connection N  Pages per connection (default: 5)
  --reg-users N             Distinct users (default: 10)
  --reg-no-teardown         Skip teardown after the scenario
  --no-benchmark            Skip the ping/echo benchmark (register scenario only)

output:
  --no-matrix               Skip the built-in scenario matrix
  --output-dir              Directory for saved report files (default: benchmark_reports)
  --formats                 Comma-separated report formats: json,csv,txt (default: all)
  --no-save                 Do not save any report files to disk
```

---

## Default scenario matrix

| # | Requests | Processes | Threads/proc | Effective concurrency |
|---|---|---|---|---|
| S01 | 100 | 1 | 1 | 1 |
| S02 | 500 | 1 | 4 | 4 |
| S03 | 1 000 | 1 | 8 | 8 |
| S04 | 1 000 | 2 | 4 | 8 |
| S05 | 5 000 | 2 | 8 | 16 |
| S06 | 5 000 | 4 | 8 | 32 |
| S07 | 10 000 | 4 | 16 | 64 |
| S08 | 10 000 | 8 | 16 | 128 |

---

## Output format

The report table columns are:

| Column | Description |
|---|---|
| **Scenario** | Scenario label |
| **Reqs** | Actual requests executed |
| **Proc** | Processes used |
| **Thr** | Threads per process |
| **Time(s)** | Total wall-clock time |
| **RPS** | Requests per second (throughput) |
| **Err%** | Error rate percentage |
| **Min ms** | Minimum request latency (ms) |
| **Median** | Median (p50) latency (ms) |
| **P95** | 95th-percentile latency (ms) |
| **P99** | 99th-percentile latency (ms) |
| **Max ms** | Maximum observed latency (ms) |

The JSON report also includes mean latency and standard deviation per scenario,
as well as the backend name.

The report header identifies the backend used:

```
BENCHMARK REPORT — Genro Daemon  [backend: new]
```

---

## Reference benchmark

The results below were captured on the development machine running a live
daemon instance (no other load). They serve as a baseline for regression
detection — a significant drop in RPS or spike in p99 latency on equivalent
hardware is a signal worth investigating.

### Hardware

| Component | Details |
|---|---|
| **CPU** | AMD Ryzen 7 PRO 7840U — 8 cores / 16 threads, up to 4.7 GHz boost |
| **RAM** | 30 GiB DDR5, ~18 GiB available during the run |
| **Swap** | 24 GiB |
| **OS** | Debian GNU/Linux 13 (trixie), kernel 6.12.74 x86_64 |
| **Storage** | LVM volume on NVMe, 413 GiB total |
| **Daemon** | Local loopback (127.0.0.1:40404), single process, memory backend |

### Results

```
============================================================================================================================
BENCHMARK REPORT — Genro Daemon  [backend: new]
Generated : 2026-03-26 16:50:30
============================================================================================================================

Scenario                               Reqs  Proc  Thr  Time(s)      RPS   Err%   Min ms   Median      P95      P99   Max ms
----------------------------------------------------------------------------------------------------------------------------
S01_r100_p1_t1                          100     1    1     0.02   4119.1    0.0      0.2      0.2      0.3      1.5      1.5
S02_r500_p1_t4                          500     1    4     0.10   5186.6    0.0      0.5      0.7      1.1      1.7      3.4
S03_r1000_p1_t8                        1000     1    8     0.17   5991.9    0.0      0.7      1.2      1.7      2.2      6.4
S04_r1000_p2_t4                        1000     2    4     0.16   6330.7    0.0      0.8      1.2      1.5      1.8      6.1
S05_r5000_p2_t8                        5000     2    8     0.84   5957.8    0.0      1.9      2.5      3.3      4.4     25.1
S06_r5000_p4_t8                        5000     4    8     0.90   5539.5    0.0      1.2      5.2      6.4      7.0     73.1
S07_r10000_p4_t16                     10000     4   16     1.82   5494.8    0.0      1.4     11.1     13.5     16.3     83.6
S08_r10000_p8_t16                     10000     8   16     2.71   3685.3    0.0      1.4     18.5     22.9     91.0   2064.0
----------------------------------------------------------------------------------------------------------------------------
```

### Observations

- **Peak throughput** is ~6 300 req/s (S04), reached at 8 concurrent connections
  split across 2 processes × 4 threads each.  Beyond this point, contention on
  the single-process daemon's event loop becomes the bottleneck.
- **Median latency** scales roughly linearly with concurrency: 0.2 ms at 1
  connection, 18.5 ms at 128 (S08).  This is expected — the daemon serialises
  requests on one thread.
- **p99 latency** stays under 8 ms up through S06 (32 concurrent), then jumps
  to ~16 ms at 64 concurrent (S07) and ~91 ms at 128 (S08), reflecting
  queueing at the socket backlog.
- **Error rate** is 0% across all scenarios, confirming the daemon handles
  sustained load without dropping connections.
- The high max latency in S08 (2 064 ms) is an outlier caused by OS-level TCP
  accept-queue pressure when 128 connections arrive simultaneously; p99 remains
  well below the 10 s timeout.

> **Note:** S08 uses 8 processes. In environments that restrict semaphore
> creation (containers, some CI runners) the tool automatically falls back to a
> single thread pool with equivalent total concurrency, so results for the
> multi-process scenarios may differ.
