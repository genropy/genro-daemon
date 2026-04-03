# Architecture

## Overview

genro-daemon is a single-port, multi-tenant async RPC daemon for the Genropy
web framework.  One daemon process handles all sites on a host; sites are
isolated by namespace, not by port or process.

---

## Component map

```
+-------------------------------------------------------------+
|  Genropy site process (gnr.web / gnr.app)                   |
|                                                             |
|  GnrDaemonClient -------- connection pool --------------+   |
|  (client.py)              (socket per thread)           |   |
+---------------------------------------------------------+---+
                                                          | TCP / msgpack
                                                          v
+-------------------------------------------------------------+
|  gnr-daemon process                                         |
|                                                             |
|  Ars (ars.py)  <--- asyncio TCP server (uvloop)             |
|  |  recv -> msgpack unpack -> _serve_request loop           |
|  |  send <- msgpack pack   <- result / error frame          |
|  |                                                          |
|  +-> GnrDaemon (handler.py)  -- extends Ars                 |
|       |  _req_parse():                                      |
|       |   1. extract _sitename from kwargs                  |
|       |   2. if sitename + method found in register -> (A)  |
|       |   3. else dispatch to daemon itself       -> (B)    |
|       |                                                     |
|       +-(A)-> GnrSiteRegister  (siteregister.py)            |
|       |        |  Coordinator for one Genropy site          |
|       |        +-- GlobalRegister      (app-wide state)     |
|       |        +-- UserRegister        (per-user state)     |
|       |        +-- ConnectionRegister  (per-TCP-conn state) |
|       |        +-- PageRegister        (per-browser-tab)    |
|       |                                                     |
|       +-(B)-> GnrDaemon methods  (ping, addSiteRegister...) |
|                                                             |
|  StorageBackend (storage/)                                  |
|   +-- InMemoryBackend  -- single process, no persistence    |
|   +-- RedisBackend     -- distributed, survives restarts    |
|                                                             |
|  Background workers (processes.py, per-site)                |
|   +-- GnrCronHandler      -- periodic tasks                 |
|   +-- GnrWorkerPool       -- queue consumers                |
|   +-- GnrDaemonServiceManager -- service loader             |
|                                                             |
|  Optional: Prometheus HTTP server (metrics.py)              |
+-------------------------------------------------------------+
```

---

## Request lifecycle

```
Client thread
  |
  +- GnrDaemonClient.call(method, *args, **kwargs)
  |     adds _sitename to kwargs
  |     borrows socket from _ConnectionPool
  |     packs [REQ, call_id, method, args, kwargs] with msgpack
  |     sends over TCP
  |
  v
Ars._serve_request (asyncio coroutine, one per TCP connection)
  |  reads msgpack frames from socket
  |  calls _req_parse -> resolves method object
  |  awaits method(*args, **kwargs)
  |  packs [RES, call_id, None, result] or [ERR, call_id, (exc, msg), None]
  |  sends back over TCP
  |
  v
Client thread
  +- unpacks response
  +- returns result to caller
  +- returns socket to pool
```

---

## Multi-register model

Each site gets one `GnrSiteRegister`, which owns four sub-registers indexed
by different keys:

| Register             | Indexed by     | Typical contents                     |
|----------------------|----------------|--------------------------------------|
| `GlobalRegister`     | sitename       | app-wide shared state, subscriptions |
| `UserRegister`       | user\_id       | user session, preferences            |
| `ConnectionRegister` | connection\_id | browser connection state             |
| `PageRegister`       | page\_id       | per-tab datachanges, subscriptions   |

Sub-registers share one `StorageBackend` instance per site.  Isolation between
registers is enforced by a key prefix: `gnrd:{sitename}:{ClassName}:`.

---

## Storage backends

| Backend           | When to use                        | Persistence            |
|-------------------|------------------------------------|------------------------|
| `InMemoryBackend` | single-process, development, tests | none (lost on restart) |
| `RedisBackend`    | multi-process or HA deployments    | survives restarts      |

Configured via `GNR_DAEMON_STORE` (env var) or `storage_backend` in site config.
See [environment-variables.md](environment-variables.md) for the full URL syntax.

The `StorageBackend` ABC defines the contract:

- **key/value**: `get`, `set`, `delete`, `keys(prefix)`
- **hash**: `hget`, `hset`, `hdel`, `hgetall`, `hkeys`
- **distributed lock**: `acquire_lock`, `release_lock`

---

## Protocol (ARS -- Async RPC over msgpack)

Frames are length-prefixed msgpack arrays over a persistent TCP connection.

```
Request:   [0,  call_id, method_name, args_list, kwargs_dict]
Response:  [1,  call_id, null,        result]
Error:     [-1, call_id, [exc_name, msg], null]
```

Custom types (Bag, datetime, date, set, arbitrary objects) are encoded by
`codec.py` using type-tagged dicts inside the msgpack payload.  Objects that
are not natively supported fall back to pickle (protocol 4).

Optional HMAC-SHA256 signing wraps the raw msgpack bytes.  See
[security.md](security.md) for deployment guidance.

---

## Key modules

| Module                      | Role                                                    |
|-----------------------------|---------------------------------------------------------|
| `ars.py`                    | asyncio TCP server, connection loop, msgpack framing    |
| `handler.py`                | `GnrDaemon` -- multi-site routing, daemon-level methods |
| `siteregister.py`           | `GnrSiteRegister` -- site coordinator, public API       |
| `siteregister_base.py`      | `BaseRegister` -- locking, cleanup, shared logic        |
| `siteregister_registers.py` | Concrete register classes                               |
| `client.py`                 | Thread-safe client with connection pooling              |
| `codec.py`                  | msgpack type extensions (Bag, datetime, set, pickle)    |
| `storage/`                  | Pluggable backend (memory, Redis)                       |
| `processes.py`              | Background worker / cron / service processes            |
| `metrics.py`                | Optional Prometheus instrumentation                     |
| `exceptions.py`             | Exception hierarchy                                     |
