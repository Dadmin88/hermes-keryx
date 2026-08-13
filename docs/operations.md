# Hermes Keryx operations

Operator runbook for `keryxd`, `keryx-relay`, `keryx-node`, the `keryx` CLI, health checks, graceful shutdown, dual-run, and common failure modes.

See also: [current-product.md](current-product.md), [observability.md](observability.md), and [worker-loop.md](worker-loop.md).

## Versioned binary releases

A repository tag using a release name such as `v1.0.0` runs
`.github/workflows/release.yml` and
publishes one Linux x86-64 archive containing `keryx`, `keryxd`, `keryx-relay`,
and `keryx-node`, together with a SHA-256 checksum. The archive contains no
relay configuration, node token, identity key, TLS material, or durable state.

Ordinary installations should select an exact tag and verify the adjacent
checksum before replacing binaries. A moving branch archive or developer
checkout is not an installation artifact. Pull-request CI validates changes
but never publishes a release.

## Quick paths

| Goal | Command |
|---|---|
| local status without daemon endpoint | `cargo run -p keryx-cli --bin keryx -- status` |
| local doctor without daemon endpoint | `cargo run -p keryx-cli --bin keryx -- doctor` |
| daemon-backed status | `HERMES_KERYX_DAEMON_ENDPOINT=http://127.0.0.1:50051 keryx status` |
| daemon-backed doctor | `HERMES_KERYX_DAEMON_ENDPOINT=http://127.0.0.1:50051 keryx doctor` |
| dual-run start/status/stop | `./scripts/keryx-dual-run.sh --start|--status|--stop` |
| relay status | `HERMES_KERYX_RELAY_HEALTH_ENDPOINT=http://127.0.0.1:51052 keryx relay status` |
| registry listing | `HERMES_KERYX_RELAY_REGISTRY_ENDPOINT=http://127.0.0.1:51053 keryx relay registry list` |

## Daemon startup sequence

### 1. Configuration

Minimum for a listening daemon:

```bash
export HERMES_KERYX_DATA_DIR="${PWD}/.keryx-data"   # optional; default .keryx
export HERMES_KERYX_DAEMON_ADDR=127.0.0.1:50051      # loopback only
```

`HERMES_KERYX_DAEMON_ADDR` must parse to a **loopback** address (`127.0.0.1` or `[::1]`). Wildcard or public IPs are rejected by the current binary.

Clients:

```bash
export HERMES_KERYX_DAEMON_ENDPOINT=http://127.0.0.1:50051
```

### 2. Process bootstrap (`keryxd`)

`KeryxDaemonRuntime::startup`:

1. Create `HERMES_KERYX_DATA_DIR` if missing.
2. Open SQLite at `{data_dir}/keryx.db`.
3. Run migrations.
4. Read and check schema version (current supported: `7`).
5. Run startup `recover_stale_leases`.
6. Fail closed if recovery reports unrepaired corruption.
7. Create the blob directory on artifact writes (`{data_dir}/blobs`).
8. Attach relay discovery when `HERMES_KERYX_RELAY_REGISTRY_ENDPOINT` is configured.
9. Build `StartupReport`.

When `HERMES_KERYX_DAEMON_ADDR` is set, the binary starts gRPC and background loops:

| Loop | Default interval | Purpose |
|---|---:|---|
| Lease recovery | `30_000 ms` | `recover_stale_leases` for expired leases |
| Deadline enforcement | `30_000 ms` | `fail_expired_deadlines` for tasks with expired `deadline_ms` |
| Health | `60_000 ms` | Refresh cached `Readiness` snapshot |

If `HERMES_KERYX_DAEMON_ADDR` is unset or empty, `keryxd` performs startup recovery, logs ready, then exits. Use this for one-shot migration/recovery validation, or prefer `keryx status` / `keryx doctor` for operator checks.

### 3. Expected successful logs

```text
INFO ... component="keryxd" ... Hermes Keryx daemon runtime ready
INFO ... component="keryxd" lease_recovery_interval_ms=30000 deadline_enforcement_interval_ms=30000 health_check_interval_ms=60000 Hermes Keryx background loops started
INFO ... component="keryxd" listen_addr=127.0.0.1:50051 Hermes Keryx daemon RPC service listening
```

## Daemon operator verification

Local embedded runtime:

```bash
cargo run -p keryx-cli --bin keryx -- status
cargo run -p keryx-cli --bin keryx -- doctor
```

Against a running daemon:

```bash
HERMES_KERYX_DAEMON_ENDPOINT=http://127.0.0.1:50051 \
  cargo run -p keryx-cli --bin keryx -- status
HERMES_KERYX_DAEMON_ENDPOINT=http://127.0.0.1:50051 \
  cargo run -p keryx-cli --bin keryx -- doctor
```

Expect `keryx status: ready` and `keryx doctor: pass` when the store, schema, startup recovery, limits, and cancellation surfaces are healthy.

## CLI task and artifact operations

Task lifecycle:

```bash
export HERMES_KERYX_DAEMON_ENDPOINT=http://127.0.0.1:50051

keryx task submit demo-task
keryx task claim demo-task --worker worker-a --lease-duration-ms 120000
keryx task heartbeat demo-task --lease '<lease_id>' --worker worker-a --lease-duration-ms 120000
keryx task complete demo-task --lease '<lease_id>' --worker worker-a --duration-ms 5000
keryx task fail demo-task --lease '<lease_id>' --worker worker-a --reason transient --duration-ms 5000
```

Current CLI note: the daemon and Python SDK expose cancellation, but the Rust CLI does not yet expose a `task cancel` subcommand.

Artifacts:

```bash
keryx artifact put demo-task ./result.json --media-type application/json
keryx artifact ls demo-task
keryx artifact get '<artifact_id>' --metadata-only
keryx artifact get '<artifact_id>' --output ./downloaded-result.json
keryx artifact rm '<artifact_id>'
```

Artifact limits:

| Limit | Value |
|---|---:|
| inline bytes threshold | `64 KiB` |
| maximum artifact/blob size | `256 MiB` |
| daemon gRPC message cap for artifact RPC | max blob + 1 MiB overhead |

## Relay operations

Relay offline mailboxes, frame ownership, and the recent acknowledgement/task-receipt history are process-local in-memory state. They survive reconnect to the same relay process, but not relay restart; acknowledgement and retained task-receipt history are bounded to 8,192 entries. Blank frame identities are rejected, every mailbox entry consumes bounded frame ownership, and reconnect backpressure never panics the relay. A task relay-acceptance receipt proves only authenticated relay acceptance, not execution or durable destination acknowledgement. `PublishResult` is stronger: it returns success only after the authenticated destination has either durably applied the exact result or safely accounted for it against a structurally verified deadline/cancellation terminal state, then acknowledged the relay-issued frame. An executor therefore does not settle its durable result outbox on relay admission alone.

### Typed Nodescale identity-binding control

`nodescale.identity.bind.v1` is control traffic, not task traffic. A control-capable edge installs the public typed `NodescaleIdentityBindHandler` and advertises the `nodescale_identity_bind_v1` protocol feature. The edge can run this handler with a relay endpoint and no daemon endpoint. Only daemon-backed nodes advertise `daemon_task_consumer_v1`; the relay rejects task and result publications to destinations without that capability so unsupported frames cannot block their direct-control mailbox.

Operational invariants:

- configure node-token authentication for both publisher and destination;
- use TLS for non-loopback relay control endpoints;
- never place a sender identity in the operation body—the relay derives it from authenticated metadata;
- use the read-only `AuthenticatedDirectContext` provenance getters in handlers; this context has no public constructor or deserializer, and typed route/completion helpers are relay-internal;
- never log or audit the binding nonce;
- treat the relay frame ID as correlation only, not application authority;
- complete a control frame only with `CompleteNodescaleIdentityBind`; generic `AckFrame` cannot settle it;
- expect timeout, disconnect, missing feature, missing handler, or handler failure to return no semantic success;
- control delivery leaves generic task-routing metrics and daemon/task state unchanged.

The relay retains only bounded, process-local pending control ownership while awaiting the destination's typed result. Timeout or publisher cancellation removes the waiter, frame ownership, and mailbox copy. Relay restart similarly loses pending transport state but does not define or revoke any durable Nodescale identity binding.

`nodescale.identity.challenge.v1` follows the same direct-control lifecycle through `NodescaleIdentityChallengeHandler`, `PublishNodescaleIdentityChallenge`, and `CompleteNodescaleIdentityChallenge`. Its exact advertised feature is `nodescale.identity.challenge.v1`. Challenge requests have no peer/source/sender/nonce field. Keryx provides authenticated at-least-once delivery only; the installed Nodescale handler is authoritative for issuance and MUST durably serialize `(authenticated_sender_peer_id, operation_id)` so duplicate, concurrent, and restart retries cannot issue a second secret for the same key. Keryx must not cache or persist challenge secrets and does not provide durable issuance idempotency across restart. Only an issued result can include delivery-only challenge material; rejected results, including duplicate rejections, must contain no secret. Challenge material is never logged, persisted, delivered through task/dead-letter paths, or surfaced through Python task handlers. Bind and challenge are mutually exclusive relay-frame payloads, and generic `AckFrame` cannot settle either control kind.

`fleet.observation.publish.v1` uses the same non-execution transport boundary through `FleetObservationPublishHandler`, but remains Fleet-state agnostic. Acquire carries only `(source, network_id, device_id)`. Publish carries that selector, the exact authority epoch `(binding_id, authenticated_peer_id, binding_generation, projection_generation)`, and a bounded observation sample. The sender exists only in `AuthenticatedDirectContext`, the destination must advertise the exact feature, absolute deadlines are future-only and bounded to 30 seconds, and generic `AckFrame` cannot settle the kind-specific waiter. Timeout, cancellation, disconnect, and relay restart clean only process-local transport ownership; no task, mailbox consumer, Fleet authority, or durable Fleet state is created by Keryx.

`keryx-node` supervises its `ConnectNode` stream. Clean EOF, transport loss, and relay replacement reconnect automatically with authentication metadata reapplied and node-specific jitter around an exponential base delay from 250 ms to a 5-second cap. Shutdown interrupts both an active stream and reconnect sleep. A separately supervised result-delivery worker keeps inbound frame consumption independent from outbound destination acknowledgement. Transient publication failures remain unacknowledged and retry durably for at most 10 total publication attempts with delivery-specific jittered exponential backoff capped at 60 seconds. The tenth failed attempt transitions the exact outbox row to retained dead-letter state with its terminal result, artifact data, and last failure reason intact; typed permanent failures still dead-letter immediately without masquerading as destination ACK. Verified artifact-free terminal late results do not reopen the task or prevent later frames from being consumed. Artifact-bearing late results fail closed so their source outbox retains the result and artifacts for durable dead-letter inspection rather than silently discarding bytes.

### JSON config (direct `RelayConfig`)

```json
{
  "listen_addresses": ["/ip4/127.0.0.1/tcp/4101", "/ip4/127.0.0.1/udp/4101/quic-v1"],
  "bootstrap_peers": [],
  "enable_mdns": false,
  "keypair_path": null,
  "max_circuits": 256,
  "max_reservations": 128,
  "max_reservations_per_peer": 4,
  "connection_timeout_ms": 30000,
  "use_ipv6": false,
  "health_grpc_bind": "127.0.0.1:51052",
  "health_http_bind": "127.0.0.1:18081",
  "registry_grpc_bind": "127.0.0.1:51053"
}
```

Authenticated relay control and registry gRPC permit plaintext only on
loopback. A non-loopback `health_grpc_bind` or `registry_grpc_bind` must also
configure both `registry_tls_cert_path` and `registry_tls_key_path`; the same
TLS identity protects both services. PEM paths are resolved relative to the
config file. Rust control and registry clients must use `https://` remotely
and may trust a private CA through `HERMES_KERYX_REGISTRY_CA_CERT`.

Start and inspect:

```bash
export HERMES_KERYX_RELAY_CONFIG=/path/to/relay.json
cargo run -p keryx-relay --bin keryx-relay

HERMES_KERYX_RELAY_HEALTH_ENDPOINT=http://127.0.0.1:51052 \
  cargo run -p keryx-cli --bin keryx -- relay status
```

### TOML config (security-enabled)

TOML config supports `[relay]`, `[security]`, and `[registry]` sections. It enables allowlists and node-token auth primitives. Relative security paths resolve relative to the relay config file.

Relay identity keys, node-token files, and TLS private keys are secrets. Keep
them outside version control, restrict them to the service account, and
provision them through an operator-controlled secret channel. Public examples
must contain placeholders only.

```toml
[relay]
listen_addresses = ["/ip4/127.0.0.1/tcp/4101", "/ip4/127.0.0.1/udp/4101/quic-v1"]
bootstrap_peers = []
enable_mdns = false
max_connections = 256
max_reservations = 128
max_reservations_per_peer = 4
connection_timeout_ms = 30000
use_ipv6 = false

[security]
allowlist_path = "allowlist.toml"
empty_allowlist_policy = "deny" # "allow" disables enforcement when the file is empty
node_tokens_path = "node-tokens.toml"
revoked_nodes = []

[[security.node_tokens]]
node_id = "node:example"
token = "replace-with-at-least-16-bytes"

[registry]
ttl_seconds = 300
max_skills_per_peer = 64
```

Allowlist file:

```toml
[[allowed]]
peer_id = "<libp2p-peer-id>"

# or derive peer id from an Ed25519 public key
[[allowed]]
ed25519_public_key_b64 = "<base64-32-byte-public-key>"
```

On Unix, send SIGHUP to a TOML-configured relay to reload the allowlist file and,
when `security.node_tokens_path` is configured, the complete owner-managed node
token authentication file. The two reloads are independent. A token file is
fully parsed and validated before one atomic in-process replacement; a missing
or invalid replacement leaves the previous authentication snapshot active.
Existing relay streams are not disconnected by this reload. Reload logs report
only success or failure and never include node-token values.

### Relay code defaults vs dual-run defaults

| Surface | Code default | Dual-run default |
|---|---|---|
| libp2p TCP/QUIC | `0.0.0.0:4001` | `127.0.0.1:4101` |
| gRPC health | `127.0.0.1:50052` | `127.0.0.1:51052` |
| HTTP health | `127.0.0.1:8081` | `127.0.0.1:18081` |
| registry gRPC | `127.0.0.1:50053` | `127.0.0.1:51053` |

Use dual-run defaults when migrating alongside AgentAnycast.

## Skill registry operations

Daemon-side discovery registration:

```bash
export HERMES_KERYX_RELAY_REGISTRY_ENDPOINT=http://127.0.0.1:51053
export HERMES_KERYX_DAEMON_SKILLS=echo,analysis
export HERMES_KERYX_DAEMON_NAME=demo-daemon
export HERMES_KERYX_DAEMON_DESCRIPTION="demo daemon"
export HERMES_KERYX_DAEMON_REGISTRATION_TTL_SECONDS=300
```

Edge node registration:

```bash
export HERMES_KERYX_DAEMON_ENDPOINT=http://127.0.0.1:50051
export HERMES_KERYX_RELAY_REGISTRY_ENDPOINT=http://127.0.0.1:51053
export HERMES_KERYX_NODE_SKILLS=echo,analysis
export HERMES_KERYX_NODE_NAME=demo-node
export HERMES_KERYX_NODE_DESCRIPTION="demo node"
export HERMES_KERYX_NODE_TTL_SECONDS=300
cargo run -p keryx-relay --bin keryx-node
```

Edge-node skill registration is one-shot at process startup. A deployment that
needs continuous discovery must arrange a refresh before the configured TTL
expires; do not treat an expired registry entry as transport disconnection or
task completion.

CLI discovery:

```bash
HERMES_KERYX_RELAY_REGISTRY_ENDPOINT=http://127.0.0.1:51053 \
  keryx relay registry list
HERMES_KERYX_RELAY_REGISTRY_ENDPOINT=http://127.0.0.1:51053 \
  keryx node discover echo --limit 10
```

## Dual-run script

`./scripts/keryx-dual-run.sh` builds missing debug binaries, writes a JSON relay config unless an explicit config is provided, starts `keryxd`, waits for daemon health, starts `keryx-relay`, waits for relay health, and reports status.

Useful overrides:

| Variable | Default |
|---|---|
| `KERYX_DUAL_RUN_STATE_DIR` | `~/.hermes/.keryx` |
| `KERYX_DUAL_RUN_LOG_DIR` | `$STATE_DIR/logs` |
| `KERYX_DUAL_RUN_RUN_DIR` | `$STATE_DIR/run` |
| `HERMES_KERYX_DATA_DIR` | `$STATE_DIR/data` |
| `HERMES_KERYX_DAEMON_ADDR` | `127.0.0.1:50051` |
| `HERMES_KERYX_DAEMON_ENDPOINT` | `http://$HERMES_KERYX_DAEMON_ADDR` |
| `HERMES_KERYX_RELAY_HEALTH_GRPC_ADDR` | `127.0.0.1:51052` |
| `HERMES_KERYX_RELAY_HEALTH_HTTP_ADDR` | `127.0.0.1:18081` |
| `HERMES_KERYX_REGISTRY_ENDPOINT` | `127.0.0.1:51053` |
| `HERMES_KERYX_RELAY_LISTEN_TCP` | `/ip4/127.0.0.1/tcp/4101` |
| `HERMES_KERYX_RELAY_LISTEN_QUIC` | `/ip4/127.0.0.1/udp/4101/quic-v1` |

Health check order is `grpcurl` when available, then the Keryx CLI, then a Python gRPC fallback.

## Graceful shutdown sequence

Triggered by SIGINT (`Ctrl+C`) in `keryxd` when the RPC listener is active.

1. Log `shutdown signal received`.
2. Stop lease recovery loop.
3. Stop deadline enforcement loop.
4. Stop health loop.
5. `KeryxDaemonRuntime::shutdown`:
   - mark shutting down and signal gRPC stop
   - stop discovery registration loop
   - drain in-flight RPCs up to 30s
   - close `SqliteStore`
   - log completion
6. Await gRPC server task completion.

`RpcInFlightGuard` rejects new RPCs with gRPC `UNAVAILABLE` once shutdown starts. Calls already in progress are allowed to finish until the drain timeout.

## Health check procedures

### Bootstrap / deploy gate

1. Start `keryxd` with data dir and listen addr.
2. `keryx doctor` via endpoint → must be `pass`.
3. gRPC `Readiness` → `ready: true` and empty `not_ready_reasons`.
4. Optional: `Liveness` → `alive: true`.
5. For relay, `KeryxRelay/Health` → `healthy: true`, `transport_status: listening`.

### Steady state

- **Liveness:** cheap process/RPC stack check; does not validate SQLite.
- **Readiness:** use before sending task traffic; refreshes from store health loop.
- **Status:** rich report including startup recovery, limits, metrics, cancellation/deadline counters.
- **Doctor:** named checks for actionable debugging.

## Common issues and troubleshooting

### Daemon refuses to bind address

**Cause:** non-loopback bind is intentionally rejected.

**Action:** use `127.0.0.1:PORT` or `[::1]:PORT`; front with SSH or a local proxy if remote operators need access.

### Startup fails with corruption

**Cause:** event stream does not replay to snapshot or another unrepaired store integrity issue.

**Action:**

1. Do not delete `keryx.db` without a backup.
2. Run `keryx doctor` locally against a copy of the data dir.
3. Identify corrupted tasks from recovery report/readiness reasons.
4. Restore from backup or apply a documented repair. Keryx is fail-closed until repair policy is approved.

### Tasks stuck in `running`

**Cause:** active lease has not yet expired/recovered, or deadline has not been reached.

**Action:** confirm the lease recovery loop is running, wait for the next tick (30s default), and check worker heartbeat cadence. If a task has `deadline_ms`, confirm the deadline enforcement loop is logging ticks.

### SubmitTask rejected by limits

**Cause:** pending task count is at/above `max_pending_tasks`, envelope bytes exceed `max_envelope_bytes`, or retained durable envelope bytes would exceed the aggregate retained-envelope limit.

**Action:** claim/drain pending work, prune/recreate local test stores when retained completed envelopes are intentionally disposable, increase limits in embedded config, or use `LimitsConfig::unlimited()` in tests. The current binary uses default limits until external config wiring is added.

### Artifact upload rejected

**Cause:** artifact is larger than 256 MiB or the daemon cannot write the blob directory.

**Action:** shrink/split the artifact, verify data dir permissions, or store an external reference in task metadata.

### Relay status is unhealthy

**Cause:** libp2p transport has not emitted `NewListenAddr`, health endpoint mismatch, or config parse/bind failure.

**Action:** check `keryx-relay` logs, verify `HERMES_KERYX_RELAY_CONFIG`, and query the configured health endpoint.

### Registry is empty

**Cause:** no daemon/node has registered skills, TTL expired, registry endpoint mismatch, or skill IDs differ.

**Action:** set `HERMES_KERYX_RELAY_REGISTRY_ENDPOINT` for Rust CLI/node/daemon discovery, or `HERMES_KERYX_REGISTRY_ENDPOINT` for Python SDK/dual-run alias, then register skills again.

### CLI cannot reach daemon

**Action:** verify `keryxd` is listening, endpoint includes a valid scheme for Rust CLI (`http://host:port`), firewall/loopback binding, and process logs.

## Environment variable reference

| Variable | Used by | Purpose |
|---|---|---|
| `HERMES_KERYX_DATA_DIR` | `keryxd`, CLI, dual-run | SQLite directory. DB file: `{dir}/keryx.db`; blobs under `{dir}/blobs`. |
| `HERMES_KERYX_DAEMON_ADDR` | `keryxd`, dual-run | Optional loopback `host:port` to bind gRPC. Unset = no listener. |
| `HERMES_KERYX_DAEMON_ENDPOINT` | CLI, SDK, node, scripts | `http://host:port` for Rust CLI; Python SDK also accepts bare `host:port`. |
| `HERMES_KERYX_RELAY_CONFIG` | relay, node, dual-run | JSON/TOML relay config path. |
| `HERMES_KERYX_RELAY_BIN` | CLI relay start | Override sibling `keryx-relay` binary lookup. |
| `HERMES_KERYX_NODE_BIN` | CLI node start | Override sibling `keryx-node` binary lookup. |
| `HERMES_KERYX_RELAY_HEALTH_ENDPOINT` | CLI relay status | Relay health gRPC endpoint, default `http://127.0.0.1:50052`. |
| `HERMES_KERYX_RELAY_REGISTRY_ENDPOINT` | Rust CLI/node/daemon discovery | Relay registry gRPC endpoint, default for CLI `http://127.0.0.1:50053`. |
| `HERMES_KERYX_REGISTRY_ENDPOINT` | Python SDK, dual-run | SDK/dual-run registry endpoint alias. |
| `HERMES_KERYX_DAEMON_SKILLS` | daemon discovery | Comma-separated skills registered by daemon. |
| `HERMES_KERYX_NODE_SKILLS` | edge node | Comma-separated skills registered by `keryx-node`. |
| `KERYX_TEST_RPC_DELAY_MS` | integration tests | Artificial RPC delay for graceful shutdown tests. |

Daemon-internal defaults are listed in [current-product.md](current-product.md).

## Validation before release

```bash
cd /path/to/Hermes_Keryx
cargo fmt --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

Focused runtime checks:

```bash
cargo test -p keryx-daemon --test health_probes
cargo test -p keryx-daemon --test tracing_instrumentation
cargo test -p keryx-daemon --test graceful_shutdown
cargo test -p keryx-daemon --test artifact_rpc
cargo test -p keryx-daemon --test task_routing
cargo test -p keryx-relay --test health
cargo test -p keryx-relay --test registry_grpc
cargo test -p keryx-relay --test security
cargo test -p keryx-observe
```

### Local daemon RPC authorization

When `keryxd` listens on `HERMES_KERYX_DAEMON_ADDR`, `HERMES_KERYX_DAEMON_TOKEN` is required. Sensitive reads, task/result dequeue, artifact access, task lifecycle mutation, remote ingress, and `SendTask` use the same `Authorization: Bearer ...` credential. Status, doctor, liveness, readiness, peer listing, and skill discovery remain read-only public-local diagnostics. Running-task cancellation also requires the exact active lease id and worker id; the daemon token alone does not grant lease ownership.
