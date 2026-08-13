# Current Hermes Keryx Product Surface

This page is the canonical repository-level map of the implemented Keryx runtime. RFCs and ADRs preserve design history; when historical documents differ from this page or the source, the implemented current contract wins.

## Components

| Component | Implemented surface |
| --- | --- |
| `keryxd` | Local daemon, SQLite-backed lifecycle, durable envelopes, artifacts, cancellation, deadlines, routing, worker claims/leases, remote result ingestion, health, readiness, status, and doctor. |
| `keryx-relay` | libp2p relay, authenticated task/result publication, relay acceptance receipts, recipient-owned frame acknowledgement, peer allowlisting, node-token authentication, skill registry, bounded offline mailboxes, TLS-capable control/registry endpoints, and typed authenticated control delivery. |
| `keryx-node` | Edge process that connects a local daemon to the relay, advertises skills/capabilities, consumes relay frames, submits remote tasks locally, and handles closed typed control operations. |
| `keryx` | Operator CLI for status, doctor, task lifecycle, artifacts, relay, and node operations. |
| Python SDK | Async `KeryxNode`, daemon lifecycle methods, discovery, worker loop, durable remote task/result observation, task reattachment, fail-closed cancellation, artifact retrieval/download, and compatibility helpers. |
| Ops scripts | Local daemon/relay dual-run, migration tooling, and authenticated two-node integration verification. |

## Canonical lifecycle

Keryx persists four task states:

```text
pending -> running -> completed | failed
```

Operational outcomes remain metadata/events around that lifecycle:

- retry can return interrupted work from `running` to `pending` while incrementing retry metadata;
- dead-letter marks exhausted work failed with explicit dead-letter metadata;
- cancellation persists a durable canceled outcome without inventing a fifth canonical task state;
- deadline expiry uses the normal terminal outcome/event model;
- routing approval can be reported as a routing outcome without becoming a persisted task status;
- result delivery uses a separate durable outbox and claim lifecycle.

## Daemon API

The `KeryxDaemon` gRPC service includes:

### Health and operator surfaces

- `Status`
- `Doctor`
- `Liveness`
- `Readiness`

### Task lifecycle

- `SubmitTask`
- `SubmitRemoteTask`
- `ClaimTask`
- `ClaimNextTask`
- `Heartbeat`
- `CompleteTask`
- `FailTask`
- `CancelTask`

### Remote results

- `GetTaskResult`
- `ClaimNextResultDelivery`
- `AckResultDelivery`
- `FailResultDelivery`
- `IngestRemoteResult`

### Artifacts

- `PutArtifact`
- `GetArtifact`
- `ListArtifacts`
- `DeleteArtifact`

### Routing and discovery

- `SendTask`
- `ListPeers`
- `DiscoverSkills`

Worker and result-delivery claims include lease/fencing data. Completion, failure, acknowledgement, and retry operations must match the active claim generation; stale or expired claims fail closed.

## Storage

`keryx-store` provides in-memory test storage and SQLite runtime storage.

Current SQLite state includes:

- task lifecycle snapshots;
- per-task event history;
- complete encoded task envelopes keyed by task ID;
- idempotency keys;
- active and inactive leases;
- retry and dead-letter metadata;
- artifact metadata and inline/blob content references;
- deadline and cancellation fields;
- authenticated remote transport context;
- durable terminal results;
- retryable result-delivery outbox records.

The current schema version is `7`.

The complete protobuf envelope is persisted atomically with task acceptance, which means nested messages, raw bytes, metadata maps, correlation identifiers, and capability hints survive daemon restart. Idempotent retries must agree with the stored task and envelope; conflicting bytes fail closed.

Task admission enforces both a per-envelope encoded-size limit and an aggregate retained-envelope byte limit. The retained-envelope limit defaults to `256 MiB` and can be set to `0` only to opt into unlimited retained-envelope storage.

The store treats the encoded envelope as opaque transport data. Protobuf encoding and decoding remain daemon/SDK responsibilities rather than leaking protocol types into the persistence boundary.

## Cross-node task delivery

A remote task follows this path:

```text
origin keryxd SendTask
  -> authenticated relay PublishTask
  -> destination keryx-node
  -> destination keryxd SubmitRemoteTask
  -> durable remote task
  -> worker claim / handler
  -> durable terminal result outbox
  -> authenticated relay PublishResult
  -> origin keryxd IngestRemoteResult
```

Incoming relay context is persisted with the remote task, including authenticated sender, local destination/executor, relay frame identity, and receive timestamp. Exact replay is idempotent; incompatible context conflicts.

See [Cross-node delivery](cross-node-delivery.md) for the full contract.

## Relay acceptance and acknowledgement

Relay acceptance receipts bind:

- task ID;
- authenticated source identity;
- destination identity;
- relay-issued frame ID;
- delivery route;
- acceptance timestamp.

A receipt proves relay acceptance, not remote execution.

The destination acknowledges a relay frame only after local durable ingestion succeeds. Terminal-result publication is settled only after the authenticated origin acknowledges its result frame after durable ingestion. Timeout or response loss before acknowledgement leaves delivery retryable.

## Relay and registry security

Task and result mutations require authenticated node credentials when relay authentication is configured. Missing, invalid, revoked, or identity-mismatched credentials fail closed.

On Unix, a TOML-configured relay can atomically reload the complete owner-managed
`security.node_tokens_path` authentication snapshot on SIGHUP without restarting
the relay. Streams belonging to removed or revoked nodes are disconnected as the
new snapshot is applied, while authorized streams remain connected. Invalid or
missing replacement files leave the previous working snapshot active; allowlist
and token reload outcomes are independent.

Registry registration/deregistration ownership is also derived from authenticated node metadata. Request-body identity cannot authorize mutation of another peer's registry entry.

Read-only skill discovery remains separate from mutation authority.

Plaintext authenticated control/registry RPCs are suitable only for loopback. Non-loopback deployments use TLS, and clients may be configured with a private CA where required.

Task publication cannot create or refresh registry ownership for the destination. Registry mutation occurs only through the dedicated authenticated registration surfaces.

## Registry and discovery

The relay registry tracks peer cards, skills, TTL, and protocol capabilities in memory.

Current behavior includes:

- authenticated registration and deregistration;
- read-only skill discovery;
- TTL expiry;
- capability advertisement;
- discovery synchronization/gossip for non-authoritative metadata.

Security-sensitive protocol capabilities are not inferred solely from gossip.

Registry state is process-memory state and does not survive relay restart.

## Protocol capability negotiation

Features that materially change transport semantics are negotiated rather than assumed.

Current examples include:

- `absolute_deadlines_v1` for cross-node absolute deadlines;
- `result_artifact_bytes_v1` for bounded byte-bearing terminal result artifacts;
- `daemon_task_consumer_v1` for nodes backed by a daemon that can consume task and result frames;
- `nodescale_identity_bind_v1` and `nodescale_identity_bind_v2` for the versioned typed Nodescale identity-binding control paths;
- `nodescale.identity.challenge.v1` and `nodescale.identity.challenge.v2` for the versioned authenticated Nodescale challenge control paths.

Unknown or unsupported destinations fail explicitly when a requested feature cannot safely be downgraded.

## Authenticated Nodescale identity binding

Keryx exposes dedicated typed non-execution operations for `nodescale.identity.bind.v1` and `nodescale.identity.bind.v2`. V1 retains its historical `join_session_id` field at protobuf tag 4. V2 instead carries `provider_binding_id` at tag 4; callers and handlers must select the matching typed message and must never reinterpret a V1 join-session value as a provider-binding value.

Properties of this path:

- the request body contains no authoritative sender/peer identity field;
- the relay derives the source only from authenticated node credentials;
- the relay binds source and destination into relay-owned context;
- delivery requires the destination to advertise `nodescale_identity_bind_v1`;
- the destination invokes a closed Rust handler;
- only the authenticated destination can complete the exact control frame;
- semantic success is returned through the typed completion path;
- generic task storage, daemon submission, Python workers, Hermes runs, and generic task/result counters are not used as fallback mechanisms.

Timeout, cancellation, restart, missing handler, or handler failure cannot fabricate semantic success.

This control path exists so higher-level identity systems can use Keryx runtime provenance without pretending a generic task body is authenticated identity.

## Authenticated Nodescale identity challenge

Keryx also exposes versioned typed non-execution challenge operations. V1 carries `join_session_id`; V2 carries `provider_binding_id` without changing the remaining field numbers or result semantics.

The challenge request carries no authoritative sender, peer, or nonce identity. The relay derives the source from authenticated node credentials, requires the destination to advertise the matching V1 or V2 challenge feature, and returns a bounded typed result only through the authenticated destination completion path.

Keryx provides authenticated at-least-once transport for this operation, not durable challenge issuance authority. The installed destination handler is responsible for durable serialization and deduplication of `(authenticated_sender_peer_id, operation_id)` across duplicate, concurrent, and restart retries. Keryx does not persist challenge secrets, route them through generic task/result storage, or expose this control path to Python task handlers.

## Remote result artifacts

Descriptor-only artifact results are the compatibility baseline.

Byte-bearing result artifacts require the authenticated origin to advertise `result_artifact_bytes_v1`. The executor verifies that capability before accepting the terminal completion that contains bytes.

At the origin:

- artifact bytes are integrity-checked before persistence;
- canonical local descriptors are assigned by the origin;
- remote logical names remain display metadata;
- download helpers require an explicit caller-selected destination path.

## Deadlines

Cross-node execution deadlines use an absolute signed 64-bit Unix epoch timestamp in the task envelope.

The destination must advertise `absolute_deadlines_v1`. Unknown or incompatible peers are rejected before relay acceptance rather than receiving work whose deadline semantics were silently weakened.

## Cancellation

The local daemon supports durable cancellation.

Cross-node cancellation remains intentionally conservative. An origin record cannot prove that a remote worker observed cancellation, so the origin-side remote cancellation surface fails closed instead of claiming the executor stopped.

If the destination itself durably cancels remote-origin work, its normal terminal canceled result can be returned through the authenticated result path. Duplicate local cancellation reuses the durable outcome rather than creating duplicate result delivery.

## Offline mailbox

The relay supports bounded in-memory offline mailboxes.

Frames can survive a peer disconnect and be delivered after the peer reconnects to the same relay process. They remain pending until the authenticated destination acknowledges the exact frame.

Mailbox state and relay delivery-history state do not survive relay restart.

## Python SDK

The Python package is `keryx`.

Core exports include:

- `KeryxNode`, `KeryxConfig`, `load_config`;
- `Task`, `IncomingTask`, `TaskHandle`, `TaskStatus`;
- `TaskState`, `TaskResult`, `TaskArtifact`;
- `AgentCard`, `Skill`;
- peer identity and registration helpers.

Native daemon methods include connection, status/doctor, discovery, submit, claim, heartbeat, complete, fail, and cancel.

The worker loop can claim durable tasks, dispatch registered handlers, heartbeat active leases, and persist completion/failure.

`TaskHandle.wait()` observes durable origin-side results. A handle can be reconstructed by task ID after controller restart for status/result observation. Historical terminal rows that predate durable result storage return an explicit unavailable error rather than fabricating data.

Registry registration supports a one-shot primitive and an opt-in refresh lifecycle with bounded cleanup. Prolonged registry outage may still expire a registration lease, which remains visible through registration status.

See [sdk/python/README.md](../sdk/python/README.md).

## Operator CLI

Implemented command groups:

```text
keryx status
keryx doctor
keryx task submit|claim|heartbeat|complete|fail
keryx artifact put|get|ls|rm
keryx relay start|status|registry list
keryx node start|status|discover
```

`CancelTask` exists in the daemon/SDK even though the Rust CLI does not currently expose a `task cancel` subcommand.

## Important defaults

| Setting | Default |
| --- | ---: |
| schema version | `7` |
| lease TTL when omitted | `300000 ms` |
| lease recovery interval | `30000 ms` |
| deadline enforcement interval | `30000 ms` |
| health probe interval | `60000 ms` |
| shutdown drain timeout | `30000 ms` |
| pending task limit | `10000` (`0` = unlimited) |
| submit envelope limit | `4 MiB` (`0` = unlimited) |
| inline artifact threshold | `64 KiB` |
| max artifact/blob size | `256 MiB` |
| cross-node result artifact content | `4 MiB` aggregate per terminal result |
| result transport frame ceiling | `5 MiB` |
| default local peer ID | `node-local` |
| default `SendTask` timeout | `30000 ms` |

## Common environment variables

| Variable | Purpose |
| --- | --- |
| `HERMES_KERYX_DATA_DIR` | daemon/CLI SQLite data directory |
| `HERMES_KERYX_DAEMON_ADDR` | daemon bind address |
| `HERMES_KERYX_DAEMON_ENDPOINT` | daemon client endpoint |
| `HERMES_KERYX_RELAY_CONFIG` | relay/edge configuration path |
| `HERMES_KERYX_RELAY_ENDPOINT` | relay task/control endpoint |
| `HERMES_KERYX_RELAY_HEALTH_ENDPOINT` | relay health endpoint |
| `HERMES_KERYX_RELAY_REGISTRY_ENDPOINT` | relay registry endpoint |
| `HERMES_KERYX_REGISTRY_ENDPOINT` | Python SDK registry endpoint alias |
| `HERMES_KERYX_DAEMON_SKILLS` | daemon-advertised skills |
| `HERMES_KERYX_NODE_SKILLS` | edge-advertised skills |
| `HERMES_KERYX_WORKER_ID` | default Python SDK worker ID |
| `HERMES_KERYX_NODE_TOKEN` | authenticated node credential metadata |
| `HERMES_KERYX_REGISTRY_CA_CERT` | private CA for HTTPS registry/control endpoints |

## Verification

Run the normal repository gates on the exact revision under evaluation:

```bash
cargo fmt --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
cargo build --workspace --bins

python -m pip install -e "sdk/python[dev]"
python -m pytest sdk/python/tests -q
python scripts/e2e_two_node.py --bin-dir target/debug
```

A historical checkpoint or completed implementation phase is not proof for a changed tree.

## Known limitations

- relay mailbox and registry state are not relay-restart durable;
- cross-node cancellation does not claim success without remote-observation evidence;
- Python result observation is polling-based rather than a streaming subscription;
- some AgentAnycast-era compatibility surfaces remain for migration of older consumers.

### Local daemon RPC authorization

When `keryxd` listens on `HERMES_KERYX_DAEMON_ADDR`, `HERMES_KERYX_DAEMON_TOKEN` is required. Sensitive reads, task/result dequeue, artifact access, task lifecycle mutation, remote ingress, and `SendTask` use the same `Authorization: Bearer ...` credential. Status, doctor, liveness, readiness, peer listing, and skill discovery remain read-only public-local diagnostics. Running-task cancellation also requires the exact active lease id and worker id; the daemon token alone does not grant lease ownership.
