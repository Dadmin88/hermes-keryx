# Hermes Keryx

Hermes Keryx is a Rust-native transport and durable task runtime for distributed Hermes systems. It provides authenticated peer routing, durable local task state, relay delivery, worker leases, terminal result return, artifacts, deadlines, discovery, and a Python SDK without coupling application-level authorization to the transport layer.

Keryx is infrastructure. Products such as Hermes Fleet and Nodescale build higher-level coordination and identity workflows on top of its authenticated transport contracts.

## What Keryx provides

- **`keryxd`** — local daemon with SQLite-backed task lifecycle, leases, recovery, deadlines, artifacts, cancellation, routing, health, and diagnostics.
- **`keryx-relay`** — authenticated libp2p relay with routing, peer admission, registry/discovery, delivery acknowledgement, bounded offline mailboxes, and result return.
- **`keryx-node`** — edge process that connects a local daemon to the relay and advertises supported skills/capabilities.
- **`keryx` CLI** — operator status, doctor, task, artifact, relay, and node commands.
- **Python SDK** — async `KeryxNode`, task handles, worker dispatch, registry helpers, durable result reattachment, and artifact retrieval.
- **Authenticated control operations** — closed non-execution control paths for integrations that require runtime-authenticated sender provenance, including Nodescale identity binding.

## Architectural boundary

Keryx authenticates and transports work. It does not decide what application-level operations a peer should be allowed to perform.

For example:

- Keryx proves which peer submitted or completed a transport operation.
- Hermes Fleet decides whether that peer may invoke `fleet.message` or `fleet.hermes.run`.
- Nodescale decides how managed device membership and identity are represented.
- Hermes owns local agent execution.

Authenticated identity is therefore not the same thing as application authorization or trusted content.

## Core lifecycle

The durable task lifecycle uses four canonical states:

```text
pending -> running -> completed | failed
```

Retry, dead-letter, cancellation, deadline expiry, routing approval, and delivery state are represented as durable metadata/events rather than proliferating task status values.

Keryx uses leases and generation/fencing semantics to reject stale worker actions and supports restart-safe recovery from interrupted work.

## Cross-node delivery

A normal remote round trip is:

```text
origin client / SDK
  -> origin keryxd
  -> authenticated relay publication
  -> destination keryx-node
  -> destination keryxd
  -> worker claim / handler
  -> durable terminal result outbox
  -> authenticated relay result publication
  -> origin keryxd
  -> TaskHandle.wait() / reattachment
```

The relay authenticates source and destination transport identities. Terminal result delivery is acknowledged by the authenticated recipient after durable ingestion. Ambiguous delivery remains retryable rather than being silently treated as success.

See [Cross-node delivery](docs/cross-node-delivery.md).

## Authenticated control operations

Some integrations need authenticated sender provenance without creating a normal task or running an agent. Keryx supports closed typed control paths for that purpose.

`nodescale.identity.bind.v1` is delivered through a dedicated relay operation whose request body contains no authoritative sender identity. The relay derives the sender from authenticated runtime credentials and passes that provenance to the destination's typed handler. The path does not fall back to generic task storage, Python worker dispatch, or Hermes execution.

## Workspace

```text
crates/keryx-core/       domain model, identifiers, lifecycle, limits, artifacts
crates/keryx-proto/      protobuf and gRPC-facing types
crates/keryx-store/      persistence traits and SQLite store
crates/keryx-daemon/     daemon runtime and keryxd
crates/keryx-relay/      relay, registry, security, keryx-relay and keryx-node
crates/keryx-cli/        operator CLI
crates/keryx-policy/     policy, keys, tokens, approvals
crates/keryx-observe/    metrics and observability helpers
crates/keryx-testkit/    shared test fixtures
sdk/python/              Python SDK package `keryx`
proto/                   protobuf definitions
scripts/                 migration, local dual-run, and integration tooling
docs/                    product, architecture, operations, contracts, ADRs, RFCs
```

## Build and test

```bash
cargo fmt --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
cargo build --workspace --bins
```

Python SDK:

```bash
cd sdk/python
python -m pip install -e ".[dev]"
pytest
```

Authenticated two-node integration:

```bash
python scripts/e2e_two_node.py --bin-dir target/debug
```

Run verification against the exact revision being evaluated. Historical merge results are not evidence for changed code.

## Local quickstart

Start one daemon and relay pair on loopback-only development ports:

```bash
./scripts/keryx-dual-run.sh --start
./scripts/keryx-dual-run.sh --status
./scripts/keryx-dual-run.sh --stop
```

Manual daemon:

```bash
export HERMES_KERYX_DATA_DIR="${PWD}/.keryx-data"
export HERMES_KERYX_DAEMON_ADDR=127.0.0.1:50051
cargo run -p keryx-daemon --bin keryxd
```

CLI examples:

```bash
export HERMES_KERYX_DAEMON_ENDPOINT=http://127.0.0.1:50051

cargo run -p keryx-cli --bin keryx -- status
cargo run -p keryx-cli --bin keryx -- doctor
cargo run -p keryx-cli --bin keryx -- task submit my-task-id
```

Implemented CLI groups:

```text
keryx status
keryx doctor
keryx task submit|claim|heartbeat|complete|fail
keryx artifact put|get|ls|rm
keryx relay start|status|registry list
keryx node start|status|discover
```

The daemon supports task cancellation even though the Rust CLI does not currently expose a `keryx task cancel` command.

## Python SDK

Package and import name: `keryx`.

```python
from keryx import AgentCard, KeryxNode, Skill

card = AgentCard(
    name="demo-agent",
    description="Example Keryx node",
    skills=[Skill(id="echo", description="echo messages")],
)

node = KeryxNode(
    card=card,
    daemon_endpoint="127.0.0.1:50051",
    registry_endpoint="127.0.0.1:51053",
)
```

See [sdk/python/README.md](sdk/python/README.md) for the SDK API, worker loop, configuration, and compatibility helpers.

## Security model

Production deployments should:

- keep `keryxd` on loopback or another explicitly trusted local transport;
- require authenticated node credentials for relay mutations and registry ownership;
- use TLS for non-loopback relay control and registry endpoints;
- treat request-body peer IDs as claims, never as authenticated identity;
- keep node tokens, private keys, and TLS private material out of Git and logs;
- negotiate protocol capabilities before using features such as absolute deadlines or byte-bearing result artifacts;
- fail closed when sender provenance, destination capability, or delivery acknowledgement is uncertain.

Read-only skill discovery is intentionally separate from authenticated mutation authority.

## Documentation

Start with the [documentation index](docs/README.md).

- [Current product surface](docs/current-product.md)
- [Cross-node delivery](docs/cross-node-delivery.md)
- [Lifecycle, store, and daemon semantics](docs/lifecycle-store-daemon-semantics.md)
- [Worker loop](docs/worker-loop.md)
- [Operations](docs/operations.md)
- [Observability](docs/observability.md)
- [System contract](docs/contracts/system-contract.md)
- [Architecture notes](docs/architecture/)
- [Architecture decision records](docs/adr/)
- [RFC history](docs/rfc/)
- [Legacy AgentAnycast migration](docs/migration-from-agentanycast.md)

RFCs, ADRs, and migration documents may describe historical decisions. The current product surface and implemented source take precedence when historical documents differ from the runtime.

## Known limitations

- Relay offline mailboxes and registry state are process-memory state and do not survive relay restart.
- Cross-node cancellation remains intentionally fail-closed where the origin cannot prove the remote executor observed cancellation.
- Python task result observation uses bounded polling rather than a streaming subscription.
- Some compatibility helpers remain for older AgentAnycast-era integrations; they are transition surfaces, not the preferred architecture for new consumers.

## License

Hermes Keryx is licensed under the Apache License 2.0. See [LICENSE](LICENSE).
