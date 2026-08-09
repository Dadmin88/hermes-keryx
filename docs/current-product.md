# Current Hermes Keryx product surface

This page is the repository-wide map of what is implemented today. Older RFCs and ADRs in this repository are design history; when they differ from this page, this page and the Rust/Python source are the current product contract.

## Components

| Component | Implemented surface |
|---|---|
| `keryxd` | Local daemon runtime; SQLite store; gRPC `KeryxDaemon` service; lifecycle, durable task envelopes, artifacts, cancellation, deadline enforcement, routing, discovery hooks, health/readiness/status/doctor. |
| `keryx-relay` | libp2p relay process; TCP + QUIC listen addresses; gRPC health; HTTP `/health`; fail-closed authenticated task/result publication; recipient-owned frame acknowledgement; relay-issued acceptance receipts; capability-gated deadline and byte-result delivery; typed `nodescale.identity.bind.v1` and `nodescale.identity.challenge.v1` non-execution control delivery with runtime-authenticated sender provenance; in-memory offline mailboxes and skill registry with TTL cleanup/gossipsub discovery sync (gossip cannot assert protocol capabilities); peer allowlist and node token authentication. |
| `keryx-node` | Edge node binary from `keryx-relay`; optionally verifies daemon readiness, dials bootstrap peers, registers skills, consumes relay frames, submits task envelopes into its local daemon, and dispatches closed typed Nodescale identity-binding and challenge control seams without a daemon or task fallback. |
| `keryx` | Operator CLI for `status`, `doctor`, `task`, `artifact`, `relay`, and `node` subcommands. |
| Python SDK | Package/import name `keryx`; async `KeryxNode`; daemon lifecycle methods; relay registry and protocol-feature helpers; durable remote worker/result loop; public task reattachment by ID for refresh/wait with fail-closed cancellation; explicit `TaskResultUnavailableError` for pre-v7 terminal rows without durable result data; verified artifact retrieval and explicit-path atomic download; AgentAnycast-compatible transition helpers. |
| Ops scripts | `scripts/keryx-dual-run.sh` for one local daemon+relay pair; `scripts/migrate-to-keryx.sh` for Hermes config migration/revert. |

## Canonical lifecycle

The persisted lifecycle remains four-state:

```text
pending -> running -> completed | failed
```

Operational outcomes are metadata/events rather than extra task status values:

- retry requeue: `running -> pending`, increments `retry_count`, appends `RecoveryAction`
- dead-letter: `running -> failed`, sets `dead_lettered` and `dead_letter_reason`
- cancel: `pending` or `running` -> canonical durable terminal state with a persisted `Canceled` outcome; reattachment maps that outcome back to `canceled` and never reopens it as generic failure
- cross-node cancellation requests remain explicitly unavailable: an origin record targeted at another executor returns `FAILED_PRECONDITION` without claiming remote cancellation or mutating local terminal state; independently, when the destination locally cancels a remote-origin task, it atomically emits the normal durable canceled-result outbox delivery back to that authenticated origin, and duplicate local cancellation returns the original durable outcome without creating a second delivery
- deadline expiry: `TaskEnvelope.deadline_ms` carries an absolute Unix epoch deadline across local, relay, and offline-mailbox routes only when the destination advertises `absolute_deadlines_v1`; unknown or older destinations fail before relay acceptance
- byte-bearing remote result artifacts require the origin destination to advertise `result_artifact_bytes_v1`; the executor verifies that authenticated registry capability before accepting terminal completion, so unsupported or unknown origins fail while the task remains running and no terminal result/outbox entry is created; descriptor-only results remain compatible with older peers
- relay acceptance receipts bind the submitted task ID, authenticated source and destination identities, relay-issued frame ID, route, and acceptance timestamp, and report `relay_accepted`; they prove relay acceptance, not execution
- incoming relay acceptance atomically persists the immutable envelope with authenticated sender, local destination/executor, relay frame identity, and original receive timestamp; exact frame replays are idempotent while changed context conflicts
- routing approval hold: `SendTask` can return `awaiting_approval` as a routing outcome; it is not a canonical persisted `TaskStatus`

## Daemon gRPC API

`proto/hermes/keryx/v1/daemon.proto` implements:

- health/operator: `Status`, `Doctor`, `Liveness`, `Readiness`
- worker lifecycle: `SubmitTask`, `SubmitRemoteTask`, `ClaimTask`, `ClaimNextTask`, `Heartbeat`, `CompleteTask`, `FailTask`, `CancelTask`
- remote results: `GetTaskResult`, `ClaimNextResultDelivery`, `AckResultDelivery`, `FailResultDelivery`, `IngestRemoteResult`
- result-delivery claims return `lease_expires_at_ms` as a claim-generation fence; ACK/failure callers must echo that exact active value, and stale or expired claims fail closed even when a later claimant reuses the same worker ID
- the executor settles its durable result-delivery outbox only after `PublishResult` observes the authenticated destination's `AckFrame`, which the destination sends after `IngestRemoteResult` succeeds; relay restart, timeout, or response loss before that acknowledgement leaves/requeues the durable outbox delivery for an idempotent fresh-frame retry
- artifacts: `PutArtifact`, `GetArtifact`, `ListArtifacts`, `DeleteArtifact`
- routing/discovery: `SendTask`, `ListPeers`, `DiscoverSkills`

Important defaults:

| Setting | Default |
|---|---:|
| schema version | `7` |
| lease TTL when omitted | `300_000 ms` |
| lease recovery interval | `30_000 ms` |
| deadline enforcement interval | `30_000 ms` |
| health probe interval | `60_000 ms` |
| shutdown drain timeout | `30_000 ms` |
| pending task limit | `10_000` (`0` means unlimited) |
| submit envelope limit | `4 MiB` (`0` means unlimited) |
| inline artifact threshold | `64 KiB` |
| max artifact/blob size | `256 MiB` |
| cross-node result artifact content | `4 MiB` aggregate per terminal result |
| result transport frame ceiling | `5 MiB` |
| default local peer id | `node-local` |
| default `SendTask` timeout | `30_000 ms` |

## Storage

`keryx-store` provides `InMemoryStore` for tests and `SqliteStore` for runtime. The SQLite store owns:

- task snapshots and per-task event log
- complete encoded `TaskEnvelope` records keyed by task ID
- idempotency keys
- active/inactive leases
- retry/dead-letter metadata
- artifact metadata plus inline bytes/blob references
- deadline/cancellation fields

Schema v6 added `task_envelopes`. `SubmitTask` persists the complete encoded protobuf envelope atomically with the pending lifecycle row, idempotency key, and accepted event. Nested messages, raw bytes, metadata maps, correlation IDs, and requested capability hints therefore survive daemon restart. Schema v7 adds authenticated transport context, durable terminal results, and retryable result-delivery outbox records.

The store intentionally treats the encoded envelope as opaque bytes and does not depend on `keryx-proto`; protobuf encoding and decoding remain daemon/SDK concerns. Idempotent retries must match both the lifecycle record and the stored envelope. Conflicting envelope bytes fail closed.

Default local CLI/runtime data directory is `.keryx` when `HERMES_KERYX_DATA_DIR` is unset. Operator dual-run uses `~/.hermes/.keryx/data`.

## Relay and registry

`keryx-relay` supports both JSON and TOML process config:

- JSON `RelayConfig` exposes direct fields such as `listen_addresses`, `health_grpc_bind`, `health_http_bind`, and `registry_grpc_bind`.
- TOML config supports `[relay]`, `[security]`, and `[registry]` sections. TOML enables allowlist files, empty-allowlist policy, inline/external node tokens, and registry TTL/max-skills settings.

Relay defaults in code are `0.0.0.0:4001` TCP/QUIC, `127.0.0.1:50052` gRPC health, `127.0.0.1:8081` HTTP health, and `127.0.0.1:50053` registry. The dual-run script intentionally overrides these to loopback non-conflicting ports.

Current registry limits:

- Registry registration and deregistration require configured node-token authentication. The relay derives the mutation owner from authenticated node metadata, rejects request-body identity mismatches, and fails closed when node authentication is absent. Plaintext authenticated relay control and registry gRPC are accepted only on loopback. Non-loopback control or registry binds use the configured TLS certificate/key, and remote Rust/Python clients require `https://` with optional private-CA trust. Skill discovery remains unauthenticated and read-only.
- Task publication cannot create, refresh, or alter the destination peer's registry entry; registry state is mutated only through owner-authenticated registration APIs.
- `max_skills_per_peer` is parsed from relay configuration but is not currently enforced.
- Registry state is in-memory and TTL-based.

`ConnectNode` is a receive-only delivery stream. Task and result mutations use the authenticated unary `PublishTask` and `PublishResult` RPCs so the relay applies the same identity and compatibility admission boundary to every accepted mutation.

Terminal-result publication requires configured node-token authentication and fails closed when the relay has no `NodeTokenAuth`. This prevents descriptor-only and byte-bearing results from using a claimed `source_node_id` as an authenticated executor identity.

`PublishNodescaleIdentityBind` is a separate typed control operation for `nodescale.identity.bind.v1`. Its protobuf body contains no sender or peer identity field. The relay derives the source exclusively from authenticated node metadata, projects that source and the destination into an opaque relay-owned `AuthenticatedDirectContext`, and admits the frame only when the destination advertises `nodescale_identity_bind_v1`. Handler implementations can read provenance through its getters but cannot construct, deserialize, or alter it; typed routing and completion methods are relay-internal. The destination invokes a closed Rust handler and returns a bounded typed semantic result through `CompleteNodescaleIdentityBind`; only that authenticated destination may complete the exact frame. Generic `AckFrame`, task storage, daemon submission, result outboxes, Python handlers, and generic task-routing counters are not part of this path. Relay timeout, cancellation, restart, missing handlers, or handler failure cannot fabricate semantic success.

`PublishNodescaleIdentityChallenge` is the analogous non-execution control operation for `nodescale.identity.challenge.v1`. Its request contains no peer, source, sender, or nonce fields. The relay accepts it only when the destination advertises the exact `nodescale.identity.challenge.v1` feature and returns the destination-authenticated typed result through `CompleteNodescaleIdentityChallenge`. Keryx is authenticated at-least-once transport, not the durable issuance authority: the installed `NodescaleIdentityChallengeHandler` MUST durably serialize and deduplicate `(authenticated_sender_peer_id, operation_id)` across duplicate, concurrent, and restart retries so a key never issues a second secret. Keryx does not cache or persist challenge secrets and provides no durable issuance idempotency across relay restart. An issued result may carry a delivery-only challenge secret; rejected (including duplicate) results carry no challenge material. The secret is not logged, persisted, routed as a task, written to result/dead-letter paths, or exposed through Python task handlers.

## Cross-node delivery boundary

Keryx proves the authenticated round trip:

```text
sender keryxd SendTask
  -> relay PublishTask
  -> destination keryx-node stream
  -> destination keryxd SubmitRemoteTask
  -> destination lifecycle row + durable full envelope
  -> Python worker ClaimNextTask + handler
  -> destination durable terminal result/outbox
  -> authenticated relay result frame
  -> origin keryxd IngestRemoteResult
  -> Python TaskHandle.wait()
```

Phase 17 was completed in [PR #29](https://github.com/DeployFaith/hermes-keryx/pull/29). The permanent proof starts a relay/registry, two daemons, two edge nodes, and a real Python worker, then verifies discovery, authenticated sender/executor identity, remote handler execution, durable result return, canonical origin-assigned artifact descriptors, exact binary artifact retrieval, explicit-path download, and clean shutdown. Cross-node result content is bounded to 4 MiB aggregate, integrity-checked before origin persistence, and never uses remote logical names as local paths.

The relay's offline mailbox is currently in-memory. It delivers frames when a node reconnects to the same running relay process, retains each pending frame until the authenticated destination acknowledges that exact relay frame, and preserves unsent reconnect overflow for later delivery. Relay task-envelope conflict checks and stable acceptance receipts are retained in a bounded in-memory history; after an acknowledged entry ages out, a later publication receives a fresh relay frame identity, so stale acknowledgements cannot remove the new delivery. None of this state is relay-restart durable.

## Operator CLI

Actual `keryx` CLI subcommands:

```text
keryx status
keryx doctor
keryx task submit|claim|heartbeat|complete|fail
keryx artifact put|get|ls|rm
keryx relay start|status|registry list
keryx node start|status|discover
```

Notes:

- `keryx task` currently has no `cancel` subcommand even though the daemon exposes `CancelTask`.
- `status` and `doctor` run an embedded local runtime when `HERMES_KERYX_DAEMON_ENDPOINT` is unset, or query the daemon endpoint when set.
- `artifact`, `task`, and `node status` require a daemon endpoint.
- `relay status` defaults to `http://127.0.0.1:50052` unless `HERMES_KERYX_RELAY_HEALTH_ENDPOINT` is set.
- `relay registry list` / `node discover` default to `http://127.0.0.1:50053` unless `HERMES_KERYX_RELAY_REGISTRY_ENDPOINT` is set.

## Python SDK

The Python package is `keryx` and exports:

- `KeryxNode`, `KeryxConfig`, `load_config`
- `TaskState`, `TaskResult`, `TaskArtifact`
- `AgentCard`, `Skill`
- `Task`, `IncomingTask`, `TaskHandle`, `TaskStatus`
- `peer_id_to_did_key`, `register_agent`, `deregister_agent`

Native daemon lifecycle methods include `connect`, `status`, `doctor`, `peers`, `skills`, `submit`, `claim`, `claim_next`, `heartbeat`, `complete`, `fail`, and `cancel`. Compatibility helpers include `start`, `stop`, `discover`, `send_task`, `register_skills`, `deregister_skills`, and `serve_forever`.

Current compatibility behavior:

- `serve_forever()` claims durable daemon tasks, dispatches them into registered handlers, and heartbeats until the `IncomingTask` completes, fails, or the worker stops.
- `send_task(..., deadline_ms=...)` propagates a zero-or-positive absolute Unix epoch deadline through the configured daemon/relay route and returns a `TaskHandle` that polls durable origin-side results. The handle retains an immutable submission receipt with the daemon's exact `task_id`, `status`, `routed_to`, and `delivery_route`; this execution deadline remains separate from the daemon client's delivery `timeout_ms`.
- `IncomingTask.complete()` / `.fail()` persist terminal state and feed the authenticated relay result route.
- High-level Python `Skill.tags` propagate through registry publication and discovery.
- Python `register_skills()` remains a one-shot primitive. The opt-in `start_registration()` lifecycle registers immediately, then makes best-effort refresh attempts before TTL expiry and retries after rejection or registry errors. `registration_status()` exposes health and pending cleanup; a prolonged outage can still let the registry lease expire. Registry mutation RPCs use finite deadlines, and one stop budget covers both refresh cancellation acknowledgement and deregistration. Work exceeding that budget remains tracked, blocks restart, and preserves refresh-before-deregister ordering. Shutdown transfers its registry client to pending cleanup so deregistration can finish before client close. The edge binary's registration remains one-shot.

The SDK default daemon endpoint is the current user's private `~/.hermes/keryx/run/keryx-daemon.sock`; repository integration examples may override it with `127.0.0.1:50051` / `http://127.0.0.1:50051`.

## Dual-run defaults

`scripts/keryx-dual-run.sh` starts one local daemon and one relay without colliding with common AgentAnycast ports:

| Component | Default |
|---|---|
| daemon gRPC | `127.0.0.1:50051` |
| relay gRPC health | `127.0.0.1:51052` |
| relay HTTP health | `127.0.0.1:18081` |
| relay registry gRPC | `127.0.0.1:51053` |
| relay libp2p TCP | `/ip4/127.0.0.1/tcp/4101` |
| relay libp2p QUIC | `/ip4/127.0.0.1/udp/4101/quic-v1` |
| state root | `~/.hermes/.keryx` |

Dual-run validates one local infrastructure pair. Use `scripts/e2e_two_node.py` for the complete authenticated remote round trip.

## Environment variables

Common variables:

| Variable | Used by | Purpose |
|---|---|---|
| `HERMES_KERYX_DATA_DIR` | daemon, CLI, dual-run | SQLite data directory |
| `HERMES_KERYX_DAEMON_ADDR` | daemon, dual-run | daemon bind address (loopback-only in `keryxd`) |
| `HERMES_KERYX_DAEMON_ENDPOINT` | CLI, SDK, node, scripts | daemon client endpoint |
| `HERMES_KERYX_RELAY_CONFIG` | relay, node, scripts | relay JSON/TOML config path |
| `HERMES_KERYX_RELAY_ENDPOINT` | daemon routing publisher, node stream | relay gRPC endpoint with scheme |
| `HERMES_KERYX_RELAY_HEALTH_ENDPOINT` | CLI, daemon fallback alias, node fallback alias | relay health/control gRPC endpoint with scheme |
| `HERMES_KERYX_RELAY_REGISTRY_ENDPOINT` | relay CLI, node CLI, daemon discovery, node binary | relay registry gRPC endpoint with scheme for clients |
| `HERMES_KERYX_REGISTRY_ENDPOINT` | Python SDK, dual-run script | SDK/dual-run registry endpoint alias |
| `HERMES_KERYX_DAEMON_SKILLS` | daemon discovery | comma-separated daemon skills to register |
| `HERMES_KERYX_NODE_SKILLS` | `keryx-node` | comma-separated edge-node skills to register |
| `HERMES_KERYX_WORKER_ID` | Python SDK | default worker id for claim/heartbeat/complete/fail |

## Validation commands

```bash
cargo fmt --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace

cd sdk/python
python -m pip install -e ".[dev]"
pytest

bash -n scripts/migrate-to-keryx.sh
bash -n scripts/keryx-dual-run.sh
./scripts/migrate-to-keryx.sh --dry-run
./scripts/keryx-dual-run.sh --status
```
