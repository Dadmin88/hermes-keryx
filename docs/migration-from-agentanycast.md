# Migration from AgentAnycast to Hermes Keryx

Hermes Agency replaces the legacy AgentAnycast transport (`agentanycastd`, libp2p/registry around ports `4001` / `50052`) with **Hermes Keryx**:

- Rust daemon: `keryxd`
- Rust relay: `keryx-relay` (libp2p + skill registry + health)
- SQLite-backed task lifecycle (schema v5)
- Python package/import: `keryx` (`KeryxNode`)

This guide is for operators migrating an existing Hermes home from AgentAnycast defaults to Keryx.

## Before you start

1. Build or install Keryx binaries from this repository (`keryxd`, `keryx-relay`, optional `keryx` CLI):

   ```bash
   cargo build --release --bin keryxd --bin keryx-relay --bin keryx
   ```

2. Ensure the Python SDK is available to Hermes Agency either via:
   - vendored package in Hermes Agency (`src/keryx/` when installing Agency), or
   - editable install from this repo:

   ```bash
   cd /path/to/Hermes_Keryx/sdk/python
   python -m pip install -e .
   ```

3. Plan ports:
   - Keryx dual-run defaults intentionally avoid AgentAnycast ports.
   - You can stop AgentAnycast after Keryx is healthy, or keep both briefly.

4. Back up `~/.hermes/config.yaml` (the migration script also creates a timestamped backup).

## Automated config migration

From this repository:

```bash
./scripts/migrate-to-keryx.sh --dry-run
./scripts/migrate-to-keryx.sh
```

Useful flags:

| Flag | Purpose |
|------|---------|
| `--dry-run` | Show planned changes only |
| `--revert` | Restore from recorded/latest pre-Keryx backup |
| `--revert /path/to/config.yaml.pre-keryx.*.bak` | Restore a specific backup |
| `--keryx-daemon PATH` | Pin `agency.daemon_bin` |

Environment overrides:

| Variable | Purpose |
|----------|---------|
| `HERMES_KERYX_MIGRATION_HOME` | Hermes root home (default `~/.hermes`) |
| `HERMES_CONFIG` | Config path override |
| `HERMES_KERYX_DAEMON_ENDPOINT` | Daemon endpoint written into config (default `127.0.0.1:50051`) |
| `HERMES_KERYX_DAEMON_BIN` | Default daemon binary path |

Typical fields written:

| Field | Value |
|-------|-------|
| `agency.transport_backend` | `keryx` |
| `agency.daemon_bin` | resolved `keryxd` path when available |
| `agency.keryx.daemon_endpoint` | `127.0.0.1:50051` unless already set |
| `agency.keryx.allowlist_path` | `~/.hermes/.keryx/allowlist.toml` |
| `agency.keryx.relay_config_path` | under `~/.hermes/.keryx/` |
| `agency.keryx.migration_backup` | backup path for `--revert` |

## Start Keryx alongside (or instead of) AgentAnycast

```bash
./scripts/keryx-dual-run.sh --start
./scripts/keryx-dual-run.sh --status
```

Dual-run defaults (loopback):

| Service | Address |
|---------|---------|
| `keryxd` | `127.0.0.1:50051` |
| relay health gRPC | `127.0.0.1:51052` |
| relay registry gRPC | `127.0.0.1:51053` |
| relay HTTP health | `127.0.0.1:18081` |
| libp2p | `127.0.0.1:4101` |

Stop cleanly:

```bash
./scripts/keryx-dual-run.sh --stop
```

Optional: stop legacy AgentAnycast after Keryx is healthy:

```bash
systemctl --user stop agentanycast-relay   # if installed as a user unit
```

## Environment variables

| Variable | Purpose |
|----------|---------|
| `HERMES_AGENCY_TRANSPORT_BACKEND` | `keryx` or `agentanycast` (overrides YAML when Agency supports it) |
| `HERMES_KERYX_DAEMON_ADDR` | daemon bind address |
| `HERMES_KERYX_DAEMON_ENDPOINT` | client endpoint for gRPC calls |
| `HERMES_KERYX_REGISTRY_ENDPOINT` | skill registry endpoint |
| `HERMES_KERYX_RELAY_CONFIG` | path to relay config |
| `HERMES_KERYX_DATA_DIR` | SQLite/data directory |

Legacy `AGENTANYCAST_*` variables apply only when running the AgentAnycast fallback path.

## Runtime layout

```text
~/.hermes/
├── config.yaml                 # agency.transport_backend: keryx
├── .keryx/
│   ├── allowlist.toml          # optional peer allowlist
│   ├── relay.toml / relay.json # relay config
│   ├── data/                   # daemon data when dual-run defaults used
│   ├── logs/                   # keryxd / keryx-relay logs
│   └── run/                    # pid files
└── profiles/<name>/.agency/    # per-profile node state
```

## systemd (optional)

User units can keep Keryx up after restarts:

```ini
# ~/.config/systemd/user/keryxd.service
[Service]
Environment=HERMES_KERYX_DAEMON_ADDR=127.0.0.1:50051
ExecStart=/path/to/keryxd
Restart=always
```

```ini
# ~/.config/systemd/user/keryx-relay.service
[Service]
WorkingDirectory=%h/.hermes/.keryx
Environment=HERMES_KERYX_RELAY_CONFIG=%h/.hermes/.keryx/relay.json
ExecStart=/path/to/keryx-relay
Restart=always
```

```bash
systemctl --user daemon-reload
systemctl --user enable --now keryxd keryx-relay
systemctl --user status keryxd keryx-relay
```

## Validation checklist

1. `./scripts/keryx-dual-run.sh --status` shows daemon + relay healthy
2. Hermes Agency config has `agency.transport_backend: keryx`
3. `from keryx import KeryxNode` works in the Hermes Python env
4. Agency doctor/status reports Keryx transport (or effective backend)
5. Pool wake/send still functions for at least one offline→online agent
6. Revert path tested once before production cutover (`--dry-run` then staged `--revert`)

## Rollback

```bash
./scripts/keryx-dual-run.sh --stop
./scripts/migrate-to-keryx.sh --revert
# optionally restart AgentAnycast
systemctl --user start agentanycast-relay
```

## Notes

- Dual-run is the safe cutover path: bring Keryx up first, validate, then stop AgentAnycast.
- Schema v5 includes deadline/cancellation fields; old stores migrate on daemon start.
- Keep private peer IDs, tokens, and hostnames out of shared docs and commits.
