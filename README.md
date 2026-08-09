# Hermes Keryx

Hermes Keryx is the secure transport layer for distributed Hermes systems.

Its job is to move application messages, tasks, results, and control operations between authenticated peers while keeping enough durable state to recover from retries, disconnects, and restarts.

A simple mental model is:

```text
Hermes / Agency / Fleet
        ↓
Keryx
  authenticate peers
  route work
  persist task/result state
  retry delivery safely
        ↓
remote node
```

Keryx is not a scheduler and it does not decide which machine should run a workload. Higher-level systems such as Hermes Fleet make those decisions.

## How it fits into the Hermes stack

```text
Nodescale
  Device identity and trust
        ↓
Keryx
  Authenticated application transport
        ↓
Hermes Fleet / Hermes Agency
  Coordination and application policy
        ↓
Hermes Agent
  Actual execution
```

Nodescale may use Keryx's authenticated runtime provenance to bind a trusted device to an application peer. Fleet and Agency use Keryx to communicate without having to build their own relay and durable delivery systems.

## Main components

| Component | Purpose |
| --- | --- |
| `keryxd` | Local daemon that owns durable task/result state and worker lifecycle. |
| `keryx-relay` | Cross-node relay, authenticated publication boundary, health service, and skill registry. |
| `keryx-node` | Edge process that connects a local daemon to the relay and receives relay frames. |
| `keryx` | Operator CLI. |
| Python SDK | Async client and worker API used by Hermes integrations and standalone applications. |

The repository is Rust-native. The Python SDK lives in `sdk/python/`.

## What Keryx provides

Keryx currently provides:

- authenticated peer identity;
- durable local task lifecycle;
- cross-node relay routing;
- task claims and leases;
- heartbeats and recovery;
- deadlines;
- retries and dead-letter handling;
- durable terminal results;
- result-delivery retries;
- artifact descriptors and bounded artifact bytes where supported;
- cancellation records;
- peer and skill discovery;
- bounded offline mailbox delivery while the relay process remains running;
- typed non-execution control traffic used by Nodescale identity binding;
- a Python SDK for higher-level Hermes applications.

## Durable task lifecycle

The canonical persisted task lifecycle stays intentionally small:

```text
pending
  ↓
running
  ├─ completed
  └─ failed
```

Other facts, such as cancellation, retry, dead-lettering, or deadline expiry, are recorded as outcomes and metadata rather than inventing a large set of task status values.

This keeps recovery easier to reason about.

## Why durable state matters

Distributed systems fail in awkward places.

For example:

```text
sender publishes task
→ relay accepts it
→ connection drops
```

or:

```text
worker finishes
→ result is stored
→ relay restarts before sender acknowledges it
```

Keryx keeps the important local task/result state durable so callers can inspect what is known instead of guessing.

The relay itself still has some in-memory state, including offline mailboxes, so relay restart durability is a separate limitation.

## Authenticated transport

Keryx treats claimed identity and authenticated identity as different things.

The relay derives authoritative sender identity from its authentication context. A request body cannot become trusted simply by including a peer ID string.

Important transport rules include:

- task/result publication uses authenticated mutation RPCs;
- destination-owned acknowledgements settle delivery;
- registry mutation is authenticated separately from read-only discovery;
- non-loopback control/registry endpoints require TLS when configured for remote use;
- typed Nodescale identity-binding control traffic is separate from generic task execution.

See [Current product surface](docs/current-product.md) for the detailed contract.

## What Keryx does not do

Keryx does not:

- choose a machine based on CPU, RAM, or GPU capacity;
- manage private-network membership;
- decide Fleet permissions;
- install Hermes profiles;
- replace Hermes Agent;
- provide a generic remote shell;
- make peer-produced content trusted just because the peer authenticated.

Those responsibilities belong to other layers.

## Workspace

```text
crates/keryx-core      Domain types, identifiers, lifecycle, limits
crates/keryx-proto     Protocol types and gRPC bindings
crates/keryx-store     Persistence interfaces and SQLite store
crates/keryx-daemon    Local runtime and keryxd
crates/keryx-relay     Relay, edge node, registry, and transport security
crates/keryx-cli       Operator CLI
crates/keryx-policy    Policy and authentication helpers
crates/keryx-observe   Metrics
crates/keryx-testkit   Test helpers
sdk/python/            Python SDK
proto/                 Protobuf definitions
docs/                  Product and operator documentation
scripts/               Local/e2e/migration helpers
```

## Build and test

```bash
cargo fmt --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
cargo build --workspace --bins
```

After installing the Python SDK development dependencies, the permanent two-node proof can be run with:

```bash
python scripts/e2e_two_node.py --bin-dir target/debug
```

That proof starts isolated relay, daemon, edge-node, and Python worker processes and checks a real authenticated round trip.

## Quick local start

For a local daemon/relay pair on loopback addresses:

```bash
./scripts/keryx-dual-run.sh --start
./scripts/keryx-dual-run.sh --status
./scripts/keryx-dual-run.sh --stop
```

This is a local infrastructure check, not the full two-node proof.

## CLI

Main command groups:

```text
keryx status
keryx doctor
keryx task submit|claim|heartbeat|complete|fail
keryx artifact put|get|ls|rm
keryx relay start|status|registry list
keryx node start|status|discover
```

The daemon supports cancellation, but the Rust CLI does not currently expose `keryx task cancel`.

## Python SDK

Install for development:

```bash
cd sdk/python
python -m pip install -e ".[dev]"
pytest
```

Minimal example:

```python
from keryx import AgentCard, KeryxNode, Skill

card = AgentCard(
    name="demo-agent",
    description="Example Keryx node",
    skills=[Skill(id="echo", description="echo messages")],
)

node = KeryxNode(card=card)
await node.connect()
try:
    state = await node.submit(message="hello", metadata={"skill": "echo"})
    print(state.task_id, state.status)
finally:
    await node.close()
```

See [sdk/python/README.md](sdk/python/README.md) for the full SDK surface.

## Hermes Agency and Fleet integration

Hermes Agency uses Keryx as its primary transport.

Hermes Fleet uses Keryx for authenticated node-to-node communication and durable task/result delivery while keeping Fleet-specific policy and execution binding in Fleet.

Nodescale uses a narrow authenticated Keryx control boundary to prove which application peer belongs to a trusted device.

These integrations share Keryx transport, but they do not share databases.

## Security notes

- Prefer loopback daemon binds for single-host installations.
- Do not put private peer IDs, node tokens, TLS private keys, credentials, or private infrastructure paths into public docs or examples.
- Authentication proves who sent something. It does not make the sender's payload trusted.
- Unsupported or ambiguous transport capabilities fail closed where the contract requires them.
- Result and task delivery are bounded. Keryx is not intended to become an unlimited blob-transfer system.

## Detailed documentation

Start with:

- [Current product surface](docs/current-product.md)
- [Lifecycle and store semantics](docs/lifecycle-store-daemon-semantics.md)
- [Worker loop](docs/worker-loop.md)
- [Operations](docs/operations.md)
- [Observability](docs/observability.md)
- [Python SDK](sdk/python/README.md)

Older RFCs and ADRs are design history when they conflict with the current product documentation and source.

## License

Current Hermes Keryx releases use the Apache License 2.0. See [LICENSE](LICENSE).
