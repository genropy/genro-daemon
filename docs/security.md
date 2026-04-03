# Security Guide

This document covers the security model of genro-daemon, recommended
deployment hardening, and known limitations.

---

## Authentication — HMAC message signing

By default the daemon runs without authentication, which is safe only
when the network is fully trusted (loopback or a private VLAN).

For any other environment, enable HMAC signing with the `-K` flag or
the `GNR_DAEMON_HMAC_KEY` environment variable:

```bash
# CLI
gnr web daemon -K "change-me-to-a-long-random-secret"

# Environment variable
GNR_DAEMON_HMAC_KEY="change-me-to-a-long-random-secret" gnr web daemon
```

The same key must be supplied to every client that connects to the
daemon.  Requests that fail the HMAC check are rejected before any
handler code runs.

**Key requirements:**
- Use a cryptographically random value of at least 32 bytes.
- Generate one with: `python -c "import secrets; print(secrets.token_hex(32))"`
- Rotate the key by restarting the daemon and all connected clients
  with the new value.
- Never commit the key to version control; pass it via a secret manager
  or environment injection.

---

## Network exposure

### Default binding

The daemon binds to `localhost:40404` by default, which is only
reachable from the same host.

### Production binding

If you need the daemon reachable on a private network interface, bind
explicitly and restrict access at the firewall level:

```bash
gnr web daemon -H 192.168.1.10 -P 40404
```

**Never bind to `0.0.0.0` on an internet-facing interface** without a
firewall rule that allows only trusted source IPs.

### No TLS

The daemon does not implement TLS.  If the connection crosses an
untrusted network (e.g., between data-centre racks, or over a VPN),
wrap it with a TLS tunnel:

```bash
# Example using stunnel (server side)
[gnrd]
accept  = 0.0.0.0:40405
connect = 127.0.0.1:40404
cert    = /etc/stunnel/server.pem
```

Alternatively, route traffic through WireGuard or another overlay
network that provides confidentiality and mutual authentication.

### Firewall example (iptables)

Allow only trusted application servers to reach the daemon port:

```bash
# Allow trusted source
iptables -A INPUT -p tcp --dport 40404 -s 10.0.0.0/24 -j ACCEPT
# Drop everything else
iptables -A INPUT -p tcp --dport 40404 -j DROP
```

---

## Docker / container deployments

Do not publish the daemon port to the host network unless strictly
required.  Use a user-defined bridge network so only co-located
containers can reach it:

```yaml
# docker-compose.yml
services:
  daemon:
    image: your-org/genro-daemon
    environment:
      GNR_DAEMON_HMAC_KEY: "${GNR_DAEMON_HMAC_KEY}"   # injected from .env or secret manager
      GNR_DAEMON_STORE: "redis://redis:6379/0"
    # Do NOT add `ports:` here unless the daemon must be reachable
    # from outside the compose network.
    networks:
      - internal

  app:
    image: your-org/genropy-app
    environment:
      GNR_DAEMON_HMAC_KEY: "${GNR_DAEMON_HMAC_KEY}"
    networks:
      - internal

networks:
  internal:
    driver: bridge
```

---

## Serialisation and trusted clients

The daemon uses **pickle** internally to serialise Python objects
transmitted over the protocol.  Pickle can execute arbitrary Python
code when deserialising; a malicious or compromised client can exploit
this to achieve remote code execution on the daemon process.

**Mitigations in place:**
- Private methods (names starting with `_`) cannot be invoked remotely.
- HMAC signing (when enabled) rejects unauthenticated messages before
  deserialisation.

**Required operational controls:**
- Enable HMAC signing in all non-loopback deployments.
- Restrict network access to the daemon port to trusted hosts only
  (firewall rules or overlay network).
- Never expose the daemon port to the public internet.
- Treat any host that can reach the daemon as a fully trusted peer.

---

## Process isolation

Each site runs its worker processes under the same OS user as the
daemon.  There is no additional sandboxing.

Recommendations:
- Run the daemon as a dedicated low-privilege user (not root).
- Use `systemd` with `ProtectSystem=strict`, `PrivateTmp=yes`, and
  `NoNewPrivileges=yes` to limit the blast radius of a compromise.

```ini
# /etc/systemd/system/genro-daemon.service (excerpt)
[Service]
User=genro
Group=genro
NoNewPrivileges=yes
ProtectSystem=strict
PrivateTmp=yes
ReadWritePaths=/var/lib/genro
```

---

## Redis backend security

When using `GNR_DAEMON_STORE=redis://...`:

- Prefer a Redis instance on `127.0.0.1` or a private network; never
  expose Redis to the public internet.
- Enable Redis authentication (`requirepass`) and include the password
  in the store URL: `redis://:password@host:6379/0`.
- Enable TLS on the Redis connection if traffic crosses an untrusted
  link: `rediss://host:6380/0`.
- Restrict Redis to the minimum required keyspace with ACLs.

---

## Secrets checklist

| Secret                               | Where to set         | How to rotate                               |
|--------------------------------------|----------------------|---------------------------------------------|
| HMAC key (`GNR_DAEMON_HMAC_KEY`)           | env / secret manager | Restart daemon + all clients simultaneously |
| Redis password                       | store URL env var    | Update URL, restart daemon                  |
| TLS certificates (stunnel/WireGuard) | sidecar config       | Roll cert, reload sidecar                   |

---

## Reporting vulnerabilities

Please report security issues privately by e-mailing the maintainers
rather than opening a public issue.  Include a description of the
vulnerability, steps to reproduce, and the version of genro-daemon
affected.
