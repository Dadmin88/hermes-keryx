# Migration from AgentAnycast to Hermes Keryx

Hermes Agency Phase 12–13 replaces the legacy AgentAnycast transport (`agentanycastd`, libp2p gRPC registry on port 50052) with **Hermes Keryx**: a Rust `keryxd` daemon, `keryx-relay` for libp2p + skill registry, SQLite-backed task lifecycle, and the **`keryx` Python SDK** (`KeryxNode`).

This guide is for operators migrating an existing Hermes home from AgentAnycast defaults to Keryx.

## Before you start

1. Build or install Keryx binaries from the [Hermes Keryx](https://github.com/DeployFaith/hermes-keryx) repository (`keryxd`, `keryx-relay`, optional `keryx` CLI).
2. Install the Python SDK:

   ```bash
   cd /path/to/Hermes_Keryx/sdk/python
   pip install -e ".[dev]"
   ```

3. Ensure a loopback daemon listener is available when running nodes with gRPC (typical: `127.0.0.1:50051`).
4. Back up `~/.hermes/config.yaml` (the migration script creates an additional timestamped backup).

## Config migration (automated)

From the Hermes Agency repository:

```bash
./scripts/migrate-to-keryx.sh --dry-run    # inspect planned changes
./scripts/migrate-to-keryx.sh              # apply migration
```

Optional flags:

- `--keryx-daemon /path/to/keryx-daemon` — pin the daemon binary written to `agency.daemon_bin`
- `--revert` — restore from `agency.keryx.migration_backup` or the latest pre-Keryx backup
- `--revert /path/to/config.yaml.pre-keryx.*.bak` — restore a specific backup

The migrator sets:

| Field | Value |
| --- | --- |
| `agency.transport_backend` | `keryx` |
| `agency.daemon_bin` | resolved `keryx-daemon` (or `~/.hermes/.keryx/bin/keryx-daemon`) |
| `agency.keryx.daemon_endpoint` | `127.0.0.1:50051` unless already set |
| `agency.keryx.allowlist_path` | `~/.hermes/.keryx/allowlist.toml` |
| `agency.keryx.relay_config_path` | `~/.hermes/.keryx/relay.toml` |
| `agency.keryx.migration_backup` | path to the YAML backup for `--revert` |

It also regenerates Keryx relay TOML from `agency.relay.allowlist` / `agency.relay.allow_all`.

## Environment variables

| Variable | Purpose |
| --- | --- |
| `HERMES_AGENCY_TRANSPORT_BACKEND` | `keryx` or `agentanycast` (overrides YAML) |
| `HERMES_AGENCY_POOL_TRANSPORT` | Pool wake/send backend (`keryx` routes pool traffic through Keryx) |
| `HERMES_KERYX_DAEMON_ENDPOINT` | gRPC URL for `keryxd` (e.g. `http://127.0.0.1:50051`) |
| `HERMES_KERYX_REGISTRY_ENDPOINT` | Skill registry on the Keryx relay (e.g. `<host>:50053`) |
| `HERMES_KERYX_RELAY_CONFIG` | Path to relay TOML (`agency.keryx.relay_config_path`) |
| `HERMES_KERYX_SDK_PATH` | Optional path to `Hermes_Keryx/sdk/python` when not installed via pip |

Registry discovery prefers `HERMES_KERYX_REGISTRY_ENDPOINT`. The legacy `AGENTANYCAST_REGISTRY_ADDRS` variable is only consulted when the Keryx endpoint is unset.

## Runtime layout

```text
~/.hermes/
├── config.yaml              # agency.transport_backend: keryx
├── .keryx/
│   ├── allowlist.toml       # relay peer allowlist (from agency.relay.allowlist)
│   ├── relay.toml           # generated relay config
│   └── bin/keryx-daemon     # expected daemon location after install
└── profiles/<name>/.agency/ # per-profile node state (unchanged)
```

Keryx daemon data (SQLite) defaults under `HERMES_KERYX_DATA_DIR` or `~/.hermes/keryx/` per the Keryx operator docs.

## Relay and ports (typical)

| Service | Port | Notes |
| --- | --- | --- |
| `keryxd` gRPC | `50051` | Loopback only by default (`HERMES_KERYX_DAEMON_ADDR`) |
| Keryx skill registry | `50053` | On `keryx-relay`; set `HERMES_KERYX_REGISTRY_ENDPOINT` |
| libp2p relay | `4001` | TCP/QUIC per `relay.toml` `listen_addresses` |

Replace any systemd unit or compose service that ran `agentanycast-relay` / `agentanycastd` with `keryx-relay` and per-profile `keryxd` / `KeryxNode` as documented in `docs/operations.md`.

## Verification

1. `hermes agency doctor` — daemon binary resolves to Keryx; SDK importable.
2. `hermes agency start` / `hermes agency info --compact` — transport label `keryx`.
3. `hermes agency discover <skill>` — registry reachable at `HERMES_KERYX_REGISTRY_ENDPOINT`.
4. `hermes agency send` or `agency_pool_send` — task submitted with `transport: keryx` in status metadata.

Automated regression (no live relay required):

```bash
cd Hermes_Agency
make test-agency
python -m pytest hermes-agency/tests/test_transport_backend.py hermes-agency/tests/test_pool_manager.py -q
cd ../Hermes_Keryx && cargo test -p keryx-daemon --test task_routing
```

## Rollback

```bash
./scripts/migrate-to-keryx.sh --revert
```

Or restore the backup file recorded in `agency.keryx.migration_backup`, set `agency.transport_backend` to `agentanycast`, and point `agency.daemon_bin` back to `agentanycastd` if you must run on the legacy stack temporarily.

## Python code changes

Application code that imported `agentanycast.Node` should use `keryx.KeryxNode` (or `keryx.compat.agentanycast` shims during transition). Hermes Agency selects the backend via `hermes-agency/transport.py` based on config — no profile code changes are required after YAML migration when using Agency tools only.

## Related docs

- [Hermes Keryx README](../README.md) — workspace crates and operator quickstart
- [sdk/python/README.md](../sdk/python/README.md) — `keryx-py` API
- [docs/operations.md](../docs/operations.md) — daemon startup and health
- [docs/worker-loop.md](../docs/worker-loop.md) — task RPC flow
- Hermes Agency `keryx-phase-12d-integration-validation.md` — CI sign-off and live smoke checklist