# Admin HTTP API

The netcd admin API is a small, JSON-over-HTTP control surface exposed on a
separate port (default `2378`, disable with `--admin-listen disabled`).
It exists so operators can inspect and edit the live runtime configuration
**without going through the etcd v3 gRPC surface** — the gRPC port stays
wire-compatible with etcd and only carries the four stock services (KV,
Watch, Lease, Maintenance) plus the standard `grpc.health.v1.Health`.

Every admin write goes through `ConfigRef::replace()` / `update()`:

1. The new config is **validated** (no empty prefix lists, no duplicate
   cert fingerprints, positive numeric fields, etc.).
2. The new config is **persisted atomically** to disk: write to
   `<config>.tmp`, `fsync`, then `rename` to the final path. The parent
   directory is also `fsync`-ed so the rename itself is durable.
3. If both steps succeed, the live `Arc<Config>` is swapped via
   `arc_swap` so all gRPC service threads pick it up on their next read
   without taking a lock.

If validation or persistence fails, the live config is **unchanged** and
the API returns `400 Bad Request` with the error message in the body.

## Conventions

- All endpoints are JSON. `Content-Type: application/json` on writes.
- All paths are prefixed with `/v1/`.
- All bodies are the raw `Config` or `Tenant` object — no envelope.
- `204 No Content` for successful deletes; `201 Created` for successful
  creates; `404 Not Found` for missing resources; `400 Bad Request` for
  validation failures; `500 Internal Server Error` for I/O failures.
- Idempotency: `PUT /v1/config` and `PUT /v1/tenants/{name}` are full
  replacements; `POST /v1/tenants` is rejected with `409 Conflict` if the
  tenant name already exists.

## Endpoints

### `GET /healthz`

Liveness probe. Returns `200 OK` with body `ok` as long as the process is
running.

```bash
curl -i http://localhost:2378/healthz
# HTTP/1.1 200 OK
# ok
```

### `GET /v1/config`

Return the current live config.

```bash
curl http://localhost:2378/v1/config | jq
```

```json
{
  "server_max_ttl_seconds": 86400,
  "max_cache_bytes": 268435456,
  "history_capacity": 1000,
  "worker_threads": 3,
  "tenants": []
}
```

### `PUT /v1/config`

Replace the entire config. Validates, persists atomically, swaps the live
config. Returns `200 OK` with the new config on success, `400` on
validation error.

```bash
curl -X PUT http://localhost:2378/v1/config \
     -H 'Content-Type: application/json' \
     -d @new-config.json
```

Example body:

```json
{
  "server_max_ttl_seconds": 43200,
  "max_cache_bytes": 536870912,
  "history_capacity": 2000,
  "worker_threads": 3,
  "tenants": [
    {
      "name": "team-a-apiserver",
      "allowed_prefixes": ["/registry/events/team-a/"],
      "client_cert_fingerprint_sha256": "ab12cd34..."
    }
  ]
}
```

### `POST /v1/config/reload`

Re-read the config from disk and swap it in. Equivalent to sending the
process a `SIGHUP`. Useful when the operator edits `config.json` directly
with their favourite editor.

```bash
curl -X POST http://localhost:2378/v1/config/reload
```

Returns `200 OK` with the new config. Returns `500` if the file is
unreadable or invalid JSON; the live config is left untouched in that
case.

### `GET /v1/tenants`

List the configured tenants (just the `tenants` array of the config).

```bash
curl http://localhost:2378/v1/tenants
```

```json
[
  {
    "name": "team-a-apiserver",
    "allowed_prefixes": ["/registry/events/team-a/"],
    "client_cert_fingerprint_sha256": "ab12cd34..."
  }
]
```

### `POST /v1/tenants`

Add a new tenant. Body is a single-field envelope:

```json
{
  "tenant": {
    "name": "team-b-apiserver",
    "allowed_prefixes": ["/registry/events/team-b/"],
    "client_cert_fingerprint_sha256": "deadbeef..."
  }
}
```

Returns `201 Created` with the new tenant object on success.
Returns `409 Conflict` if a tenant with the same name already exists.
Returns `400 Bad Request` on validation failure (empty name, empty
prefixes, empty fingerprint, duplicate fingerprint across tenants).

```bash
curl -X POST http://localhost:2378/v1/tenants \
     -H 'Content-Type: application/json' \
     -d '{"tenant":{"name":"team-b","allowed_prefixes":["/registry/events/team-b/"],"client_cert_fingerprint_sha256":"deadbeef"}}'
```

### `PUT /v1/tenants/{name}`

Update an existing tenant. Body shape (note: no `name` field — it's in
the URL):

```json
{
  "allowed_prefixes": ["/registry/events/team-b/", "/registry/events/shared/"],
  "client_cert_fingerprint_sha256": "newhash..."
}
```

Returns `200 OK` with the updated tenant on success, `404 Not Found` if
no tenant with that name exists, `400 Bad Request` on validation failure.

This is the endpoint to use when **rotating a client certificate**: mint
a new cert, compute its SHA-256, `PUT` the tenant with the new fingerprint
and (optionally) adjusted `allowed_prefixes`. The live config picks it up
immediately; the apiserver doesn't need to be restarted.

```bash
# Rotate team-b's client cert
NEW_FP=$(openssl x509 -in new-client.pem -noout -fingerprint -sha256 \
         | sed 's/^.*=//; s/://g' | tr 'A-F' 'a-f')

curl -X PUT http://localhost:2378/v1/tenants/team-b \
     -H 'Content-Type: application/json' \
     -d "{\"allowed_prefixes\":[\"/registry/events/team-b/\"],\"client_cert_fingerprint_sha256\":\"$NEW_FP\"}"
```

### `DELETE /v1/tenants/{name}`

Remove a tenant. Returns `204 No Content` on success, `404 Not Found` if
no tenant with that name exists.

```bash
curl -X DELETE http://localhost:2378/v1/tenants/team-b
```

### `GET /v1/stats`

Server runtime stats. Useful for dashboards and `kubectl`-style
introspection.

```bash
curl http://localhost:2378/v1/stats | jq
```

```json
{
  "revision": 42,
  "bytes_used": 4096,
  "max_cache_bytes": 268435456,
  "key_count": 17,
  "lease_count": 3,
  "watch_count": 1,
  "compacted_rev": 0,
  "oldest_rev": 38
}
```

Fields:

| Field             | Type    | Meaning                                                |
|-------------------|---------|--------------------------------------------------------|
| `revision`        | u64     | Current monotonic revision (last issued).              |
| `bytes_used`      | u64     | Sum of `key.len() + value.len()` across live entries.  |
| `max_cache_bytes` | u64     | Configured capacity ceiling.                           |
| `key_count`       | usize   | Number of live keys (after FIFO eviction).             |
| `lease_count`     | usize   | Number of active (non-expired) leases.                 |
| `watch_count`     | usize   | Number of currently-attached Watch streams.            |
| `compacted_rev`   | u64     | Highest revision known to be gone (explicit `Compact`  |
|                   |         | call or FIFO eviction of the history ring buffer).     |
|                   |         | A `Watch` with `start_revision <= compacted_rev` is    |
|                   |         | answered with a one-shot `compact_revision` notification. |
| `oldest_rev`      | u64     | Oldest revision still held in the history ring buffer  |
|                   |         | (0 if empty). Watch requests with `start_revision`     |
|                   |         | older than this are collapsed into the `compacted_rev` |
|                   |         | path. Exposed for ops/debug only; not part of the      |
|                   |         | etcd wire protocol.                                    |

## Examples

### Bootstrap a tenant for a new kube-apiserver

```bash
# 1. Get the apiserver's client cert fingerprint.
FP=$(openssl x509 -in apiserver-etcd-client.crt -noout -fingerprint -sha256 \
     | sed 's/^.*=//; s/://g' | tr 'A-F' 'a-f')

# 2. Add the tenant.
curl -X POST http://netcd:2378/v1/tenants \
     -H 'Content-Type: application/json' \
     -d "{\"tenant\":{\"name\":\"prod-apiserver\",\"allowed_prefixes\":[\"/registry/\",\"/registry/events/\"],\"client_cert_fingerprint_sha256\":\"$FP\"}}"

# 3. Verify.
curl http://netcd:2378/v1/tenants | jq
```

### Adjust the TTL cap at runtime

```bash
# Cap all leases at 1 hour.
curl -X PUT http://localhost:2378/v1/config \
     -H 'Content-Type: application/json' \
     -d '{
           "server_max_ttl_seconds": 3600,
           "max_cache_bytes": 268435456,
           "history_capacity": 1000,
           "worker_threads": 3,
           "tenants": []
         }'
```

### Pre-flight: verify the live config matches disk

```bash
# This is what SIGHUP would do; useful in CI to make sure a config
# reload picked up the right file.
diff <(curl -s http://netcd:2378/v1/config) <(jq . config.json) || {
    echo "live config differs from disk"
    exit 1
}
```

## What this API is *not*

- **Not authenticated / authorised.** The admin port is meant to live on
  a private network. If you need to expose it, put it behind a reverse
  proxy with mTLS / Basic Auth, or run it on `127.0.0.1` and tunnel.
- **Not a database.** It is a process-local control plane. There is no
  WAL, no replication, no schema. The on-disk `config.json` is the
  source of truth; the in-memory `Arc<Config>` is the live value.
- **Not the only way to read/write the RBAC surface — but it is the
  only way that *can*.** The `tenants[]` array (and every `Tenant` field)
  is intentionally unreachable from the CLI flag set and from any
  `NETCD_*` environment variable. The override layer only sees the
  four numeric knobs (`server_max_ttl_seconds`, `max_cache_bytes`,
  `history_capacity`, `worker_threads`). Adding a tenant, rotating a
  fingerprint, or changing an `allowed_prefixes` list must go through
  `POST`/`PUT`/`DELETE /v1/tenants` (or `PUT /v1/config` for a full
  replace). This rule is enforced at the type level — the `Overrides`
  struct that carries CLI/env values into `Config` has no `tenants`
  field — and is regressively guarded by the
  `overrides_do_not_touch_tenants` unit test.
- **Not part of the etcd wire protocol.** A standard `etcdctl` or
  `kube-apiserver` will not see or interact with these endpoints. They
  only see the four stock gRPC services on port 2379.

## Wire-compat note

Adding `AuthService` to the etcd gRPC surface would be a wire-compat
break. The admin HTTP API is intentionally out-of-band so that an etcd
v3 client (including `kube-apiserver`) can talk to netcd without
discovering any non-standard services.
