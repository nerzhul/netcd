# netcd

A minimalist, single-node **etcd v3 gRPC-compatible** in-memory key-value
store written in Rust, optimised for hosting Kubernetes `Event` objects with
bounded capacity, FIFO eviction, server-side TTL caps, and multi-tenant mTLS
RBAC.

Wire-compatible with `kube-apiserver` (`--etcd-servers=https://netcd:2379`).

## Features

- **100% etcd v3 gRPC wire-compatible** — `KV`, `Watch`, `Lease`, `Maintenance`
  (Compact), `Health` services. `Compact` is a no-op that returns success so
  `kube-apiserver`'s opportunistic calls don't break the apiserver.
- **In-memory, no Raft, no disk persistence** for KV state.
- **Server-side TTL cap** that silently clamps any lease TTL above
  `server_max_ttl_seconds`.
- **Bounded cache by bytes** (`key.len() + value.len()`); FIFO eviction by
  insertion order, not LRU.
- **Multi-tenant mTLS RBAC**: client cert SHA-256 → tenant → allowed key
  prefixes. A request whose key/range falls outside the tenant's prefixes is
  rejected with `PermissionDenied`.
- **Hot-reloadable config** on `SIGHUP`; tenant config persisted in a single
  JSON file with atomic writes (`.tmp` → `fsync` → `rename`).
- **Per-process admin HTTP API** on a separate port for editing the config
  without going through the etcd gRPC surface. See [docs/api.md](docs/api.md).
- **Hyper-lightweight container image** based on `gcr.io/distroless/cc-debian12`
  (no OpenSSL — pure-Rust TLS via `rustls` + `aws-lc-rs`).

## Non-goals

- High availability / clustering / Raft.
- Full MVCC history indexing (only a small ring buffer for late Watch
  joiners).
- Per-key TTL via `Put` (etcd v3 Puts have no per-key TTL — TTL is via
  `Lease` only).
- etcd `AuthService` gRPC (user/password). mTLS only.
- HTTP v2 API (Kubernetes uses gRPC v3 exclusively).

## Quick start

### Run from source

```bash
cargo run --release -- \
    --listen      0.0.0.0:2379 \
    --admin-listen 0.0.0.0:2378 \
    --config      config.json
```

A default `config.json` is auto-created on first run. To enable mTLS:

```bash
netcd \
    --mtls \
    --server-cert /etc/netcd/server.pem \
    --server-key  /etc/netcd/server-key.pem \
    --client-ca   /etc/netcd/client-ca.pem
```

### Override a knob via environment

For containerised deployments, every flag also has a matching
`NETCD_*` environment variable. The 12-factor precedence is
**CLI flag > env var > `config.json` > in-code default**. For example,
capping lease TTL at one hour without editing the JSON file:

```bash
NETCD_SERVER_MAX_TTL_SECONDS=3600 netcd --config /etc/netcd/config.json
```

At boot the process logs one `effective config: <name>=<value> (origin)`
line per overridable setting, where `origin` is `Cli`, `Env`, `File`, or
`Default`. This is the easiest way to confirm a k8s `ConfigMap` /
`Secret` env var is actually wired up. **Note:** env vars are evaluated
at boot, **not** on `SIGHUP` — see [SIGHUP behaviour](#sighup-behaviour).

### Run in Docker

#### Pre-built images

Tagged releases are published automatically by the
[`release-docker`](.github/workflows/docker.yml) workflow to:

- `ghcr.io/<owner>/netcd:<tag>` (e.g. `ghcr.io/nerzhul/netcd:v1.2.3`)

Each semver tag `vX.Y.Z` produces the rolling tags `X.Y.Z`, `X.Y`, and `X`,
plus a short git SHA. `latest` is reserved for the default branch.

#### Local build

```bash
docker build -t netcd:dev .
docker run --rm -p 2379:2379 -p 2378:2378 \
    -v $PWD/config.json:/etc/netcd/config.json \
    netcd:dev
```

### Point a kube-apiserver at it

```bash
kube-apiserver \
    --etcd-servers=https://netcd.internal:2379 \
    --etcd-cafile=/etc/kubernetes/pki/etcd-ca.crt \
    --etcd-certfile=/etc/kubernetes/pki/apiserver-etcd-client.crt \
    --etcd-keyfile=/etc/kubernetes/pki/apiserver-etcd-client.key
```

The apiserver's client cert SHA-256 fingerprint must appear in
`tenants[].client_cert_fingerprint_sha256` in `config.json` for the
operations to be allowed. See [docs/api.md](docs/api.md) for how to add a
tenant at runtime.

## Configuration

The config file (`config.json` by default, override with `--config` or
`NETCD_CONFIG`) has the following shape:

```jsonc
{
  "server_max_ttl_seconds": 86400,        // clamp on Lease.Grant
  "max_cache_bytes":        268435456,    // 256 MiB ceiling (keys+values)
  "history_capacity":       1000,          // ring buffer for Watch start_revision
  "worker_threads":         3,             // async-runtime worker pool size (boot-time only)
  "tenants": [
    {
      "name": "team-a-apiserver",
      "allowed_prefixes": ["/registry/events/team-a/"],
      "client_cert_fingerprint_sha256": "ab12..."
    }
  ]
}
```

`client_cert_fingerprint_sha256` is the SHA-256 of the DER-encoded client
certificate (uppercase or lowercase hex, with or without `:` separators).
This is the value `openssl x509 -fingerprint -sha256 -noout` prints.

### Environment variables

Every runtime knob has a matching `NETCD_*` environment variable, set on
the container or via your process supervisor. The CLI flag (when given)
wins over the env var; the env var wins over `config.json`; `config.json`
wins over the in-code default. The boot log prints one
`effective config: <name>=<value> (origin)` line per setting to make
which layer won visible.

#### Config knobs (override `config.json`)

| Config field             | CLI flag                       | Env var                            |
|--------------------------|--------------------------------|------------------------------------|
| `server_max_ttl_seconds` | `--server-max-ttl-seconds`     | `NETCD_SERVER_MAX_TTL_SECONDS`     |
| `max_cache_bytes`        | `--max-cache-bytes`            | `NETCD_MAX_CACHE_BYTES`            |
| `history_capacity`       | `--history-capacity`           | `NETCD_HISTORY_CAPACITY`           |
| `worker_threads`         | `--worker-threads`             | `NETCD_WORKER_THREADS`             |

#### Transport / runtime flags

| CLI flag           | Env var              | Notes                                            |
|--------------------|----------------------|-------------------------------------------------|
| `--listen`         | `NETCD_LISTEN`       | gRPC bind                                       |
| `--admin-listen`   | `NETCD_ADMIN_LISTEN` | Admin API bind (`disabled` to turn it off)      |
| `--config`         | `NETCD_CONFIG`       | Path to the JSON config                         |
| `--mtls`           | `NETCD_MTLS`         | Presence toggle (`1`/`true`/`yes`/`on`)         |
| `--server-cert`    | `NETCD_SERVER_CERT`  | PEM, full chain                                 |
| `--server-key`     | `NETCD_SERVER_KEY`   | PEM                                             |
| `--client-ca`      | `NETCD_CLIENT_CA`    | PEM, client CA bundle                           |
| `--log`            | `NETCD_LOG`          | `tracing-subscriber` env-filter syntax          |

The `--listen`, `--admin-listen`, `--config`, and `--log` flags keep
their existing defaults; the env var simply provides another way to set
the same value.

### SIGHUP behaviour

A `SIGHUP` re-reads `config.json` from disk and swaps the live config in
(the same operation as `POST /v1/config/reload`). **Env vars are not
re-applied on `SIGHUP`** — they are start-time only. So an operator who
sets `NETCD_SERVER_MAX_TTL_SECONDS=1234` and then edits `config.json`
to a different value will see the env value at boot and the disk value
after `SIGHUP`. This is the standard 12-factor trade-off; if you need
the env value to persist across reloads, set it in the JSON file (or
restart the process).

Note that `worker_threads` is also strictly boot-time: the async
runtime is built once at startup with the resolved value, and the pool
size is immutable for the life of the process. Changing the value at
runtime (via `PUT /v1/config`, `SIGHUP`, or a config edit) is
persisted and will take effect on the **next** process start.

### RBAC is intentionally outside the override layer

The `tenants[]` array (and every `Tenant` field) has **no** CLI flag and
**no** environment variable. RBAC state can only be mutated through the
admin HTTP API (`POST`/`PUT`/`DELETE /v1/tenants`, `PUT /v1/config`).
This is enforced at the type level: the `Overrides` struct that carries
CLI/env values into `Config` has no `tenants` field, and the unit test
`overrides_do_not_touch_tenants` regressively guards the invariant.

## Admin API

The HTTP admin API on port 2378 lets operators view and edit the live
`Config` without restarting the process. It accepts `GET`, `PUT`, `POST`,
`DELETE` and is fully documented in [docs/api.md](docs/api.md).

Quick examples:

```bash
# Current stats
curl http://localhost:2378/v1/stats

# Add a tenant
curl -X POST http://localhost:2378/v1/tenants \
     -H 'Content-Type: application/json' \
     -d '{"tenant":{"name":"team-b","allowed_prefixes":["/registry/events/team-b/"],"client_cert_fingerprint_sha256":"DE:AD:..."}}'

# Reload from disk (same as SIGHUP)
curl -X POST http://localhost:2378/v1/config/reload
```

## Architecture

| Component         | Concurrency primitive             | Notes                                   |
|-------------------|-----------------------------------|-----------------------------------------|
| `kv`              | `parking_lot::RwLock<BTreeMap>`   | Cheap reads                             |
| `eviction`        | `Mutex<VecDeque>`                 | FIFO of `(key, size, rev)`              |
| `history`         | `Mutex<VecDeque<Mutation>>`       | Bounded ring for late Watch joiners     |
| `leases`          | `Mutex<HashMap<i64, LeaseEntry>>` | + background expiry task                 |
| `watches`         | `Mutex<HashMap<i64, WatchHandle>>`| Per-stream mpsc sender                  |
| `rev`             | `AtomicU64`                       | Monotonic revision counter              |
| `bytes_used`      | `AtomicU64`                       | Sum of `key.len() + value.len()`        |
| Config            | `arc_swap::ArcSwap<Config>`       | Lock-free reads, hot-reloadable         |

Separate locks on the four Mutex/RwLock avoid contention. All hot paths
return `Result<T, tonic::Status>` without allocation on success.

## Testing

```bash
cargo test              # 16 unit tests + 5 e2e integration tests
cargo test --release    # slower to compile, faster to run
```

The integration tests build a real CA + server cert + client cert in
process via `rcgen`, spin up a real `netcd` instance, and exercise
Put/Get/Watch/Txn over the etcd v3 gRPC surface using the generated
`KvClient`/`WatchClient` from `tonic-build`.

## License

Apache-2.0
