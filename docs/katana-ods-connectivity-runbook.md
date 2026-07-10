# Keryx Katana/ODS Connectivity Runbook

Operator reference for the VPS ↔ Katana ↔ ODS Keryx/Agency network topology.

> **Privacy note:** Private peer IDs, hostnames, relay addresses, and tokens are replaced with `<REDACTED>` in this public-facing doc. Exact values live in `~/.hermes/.keryx/` on each host.

## Topology

```
┌─────────────────────────────────────────────────────┐
│  VPS (Tailscale: <VPS_TAILNET_IP>)                  │
│  ┌──────────────┐  ┌──────────────┐                 │
│  │ keryxd       │  │ keryx-relay  │                 │
│  │ 127.0.0.1:   │  │ libp2p:      │                 │
│  │   50051      │  │   4101       │                 │
│  │              │  │ health:      │                 │
│  │              │  │   51052      │                 │
│  │              │  │ registry:    │                 │
│  │              │  │   51053      │                 │
│  └──────────────┘  └──────┬───────┘                 │
│                           │                          │
│  relay.toml               │ allowlist.toml           │
│  keryx.db                 │                          │
└───────────────────────────┼──────────────────────────┘
                            │ Tailscale/libp2p
              ┌─────────────┴─────────────┐
              │                           │
    ┌─────────┴──────────┐    ┌───────────┴──────────┐
    │  Katana            │    │  ODS Container       │
    │  (<KATANA_IP>)     │    │  (on Katana)         │
    │  ┌──────────────┐  │    │  ┌──────────────┐    │
    │  │ keryxd       │  │    │  │ ods-hermes   │    │
    │  │ 127.0.0.1:   │  │    │  │ Docker       │    │
    │  │   50051      │  │    │  │              │    │
    │  └──────────────┘  │    │  └──────────────┘    │
    │  ┌──────────────┐  │    │  ┌──────────────┐    │
    │  │ keryx-node   │  │    │  │ keryx-node   │    │
    │  │ (Katana-ODS) │  │    │  │ (Hermes ODS) │    │
    │  │ refresh/110s │  │    │  │ refresh/110s │    │
    │  └──────────────┘  │    │  └──────────────┘    │
    │  ┌──────────────┐  │    │                      │
    │  │ task-bridge  │  │    │  Open WebUI: 3000    │
    │  │ claim/complete│ │    │  llama-server: 11434  │
    │  └──────────────┘  │    └──────────────────────┘
    └────────────────────┘
```

### Endpoints

| Endpoint | Address | Scope |
|---|---|---|
| VPS daemon gRPC | `127.0.0.1:50051` | loopback |
| VPS relay libp2p | `<VPS_TAILNET_IP>:4101` | Tailscale |
| VPS relay health gRPC | `<VPS_TAILNET_IP>:51052` | Tailscale |
| VPS relay registry gRPC | `<VPS_TAILNET_IP>:51053` | Tailscale |
| VPS relay health HTTP | `127.0.0.1:18081` | loopback |
| Katana daemon gRPC | `127.0.0.1:50051` | loopback |

### Registered Nodes

| Name | Peer ID | Skills |
|---|---|---|
| Katana-ODS | `<KATANA_PEER_ID>` | katana, local-dev, gpu-worker, ods, ods-hermes, local-ai, open-webui, llama-server |
| Hermes ODS | `<ODS_PEER_ID>` | ods, ods-hermes, local-ai, open-webui, llama-server, hermes-container |

## Config Paths

### VPS

| File | Path |
|---|---|
| Relay config | `~/.hermes/.keryx/relay.toml` |
| Allowlist | `~/.hermes/.keryx/allowlist.toml` |
| Daemon data | `~/.hermes/.keryx/data/` |
| Logs | `~/.hermes/.keryx/logs/` |

### Katana

| File | Path |
|---|---|
| Env config | `/home/kyle/.hermes/.keryx/katana.env` |
| ODS env config | `/home/kyle/.hermes/.keryx/ods.env` |
| Daemon data | `/home/kyle/.hermes/.keryx/data/` |
| ODS data | `/home/kyle/.hermes/.keryx/ods-data/` |
| Binaries | `/home/kyle/.hermes/.keryx/bin/` |
| Logs | `/home/kyle/.hermes/.keryx/logs/` |

## Service Units

### VPS (dadmin user-level systemd)

| Service | Unit Path |
|---|---|
| `keryxd.service` | `~/.config/systemd/user/keryxd.service` |
| `keryx-relay.service` | `~/.config/systemd/user/keryx-relay.service` |

### Katana (kyle user-level systemd)

| Service | Unit Path | Purpose |
|---|---|---|
| `keryxd.service` | `/home/kyle/.config/systemd/user/keryxd.service` | Local daemon |
| `keryx-node-refresh.service` | `/home/kyle/.config/systemd/user/keryx-node-refresh.service` | Katana-ODS registration (110s cycle) |
| `keryx-ods-node-refresh.service` | `/home/kyle/.config/systemd/user/keryx-ods-node-refresh.service` | ODS container registration (110s cycle) |
| `keryx-task-bridge.service` | `/home/kyle/.config/systemd/user/keryx-task-bridge.service` | Claim bridge (polls, claims, completes) |

All services: `Restart=always`, `RestartSec=5`.

## Start/Stop/Restart/Status

### VPS

```bash
# Status
systemctl --user status keryxd keryx-relay

# Restart
systemctl --user restart keryxd keryx-relay

# Stop
systemctl --user stop keryxd keryx-relay

# Start
systemctl --user start keryxd keryx-relay
```

### Katana

```bash
# Status (SSH as root, run as kyle)
ssh <KATANA_HOST> 'sudo -u kyle XDG_RUNTIME_DIR=/run/user/$(id -u kyle) \
  systemctl --user status keryxd keryx-node-refresh keryx-ods-node-refresh keryx-task-bridge'

# Restart all
ssh <KATANA_HOST> 'sudo -u kyle XDG_RUNTIME_DIR=/run/user/$(id -u kyle) \
  systemctl --user restart keryxd keryx-node-refresh keryx-ods-node-refresh keryx-task-bridge'

# Restart daemon only
ssh <KATANA_HOST> 'sudo -u kyle XDG_RUNTIME_DIR=/run/user/$(id -u kyle) \
  systemctl --user restart keryxd'
```

## Health Checks

### Relay health

```bash
curl -s http://127.0.0.1:18081/health
# Expect: {"healthy":true,"connected_peers":>=2,"registry_size":>=2,...}
```

### Daemon status

```bash
# VPS
HERMES_KERYX_DAEMON_ENDPOINT=http://127.0.0.1:50051 keryx status

# Katana
ssh <KATANA_HOST> '/home/kyle/.hermes/.keryx/bin/keryx status'
```

### Daemon doctor

```bash
# VPS
HERMES_KERYX_DAEMON_ENDPOINT=http://127.0.0.1:50051 keryx doctor

# Katana
ssh <KATANA_HOST> '/home/kyle/.hermes/.keryx/bin/keryx doctor'
```

### Registry / Discovery

```bash
# List all registered nodes
HERMES_KERYX_RELAY_REGISTRY_ENDPOINT=http://<VPS_TAILNET_IP>:51053 keryx relay registry list

# Discover by skill
keryx node discover ods
keryx node discover katana

# Agency discovery
hermes agency discover ods
hermes agency discover katana
```

### E2E smoke

```bash
# Task submission (one-way delivery — no response expected)
cd /home/dadmin/repos/Hermes_Keryx
HERMES_KERYX_DAEMON_ENDPOINT=http://127.0.0.1:50051 keryx task submit <TASK_ID>
# Then check Katana daemon for completed task
```

## Troubleshooting

### grpcio/protobuf mismatch

**Symptom:** `agency_discover` or `agency_send` fails with `grpcio 1.81.1 vs generated stubs require >=1.82.1`

**Fix:** Upgrade grpcio in the Hermes venv:
```bash
/home/dadmin/.hermes/hermes-agent/venv/bin/pip install 'grpcio>=1.82.1'
```

### Relay connected_peers=0

**Symptom:** Relay health shows `connected_peers=0` despite Katana processes running.

**Cause:** Katana keryx-node connects to relay then disconnects. Check:
1. Katana node process is running: `ps -u kyle | grep keryx-node`
2. Node refresh service is active: `systemctl --user status keryx-node-refresh`
3. Allowlist contains Katana peer ID
4. Check Katana node logs: `journalctl --user -u keryx-node-refresh -n 30`

**Fix:** Restart node refresh service on Katana.

### Registry TTL expiry

**Symptom:** `keryx node discover ods` returns 0 results despite processes running.

**Cause:** Node registration TTL (300s default) expired. The node refresh service should re-register every 110s.

**Check:** Verify refresh service is running and cycling:
```bash
ssh <KATANA_HOST> 'ps -u kyle | grep keryx-node'
```

**Fix:** Restart refresh service:
```bash
ssh <KATANA_HOST> 'sudo -u kyle XDG_RUNTIME_DIR=/run/user/$(id -u kyle) \
  systemctl --user restart keryx-node-refresh keryx-ods-node-refresh'
```

### Allowlist denial

**Symptom:** Node can't connect to relay; relay logs show peer rejected.

**Fix:** Add peer ID to `~/.hermes/.keryx/allowlist.toml` on VPS:
```toml
[[allowed]]
peer_id = "<NEW_PEER_ID>"
```
Then reload relay: `systemctl --user restart keryx-relay`

### Tailnet/DNS missing ODS

**Symptom:** ODS services not reachable; `ods` Tailscale node not found.

**Cause:** ODS is NOT a separate Tailscale node. It runs as Docker container `ods-hermes` on Katana.

**Check:**
```bash
ssh <KATANA_HOST> 'docker ps --filter name=ods-hermes'
```

## Security Notes (from KX-CONN-6)

- Relay allowlist: 79 peers, `empty_allowlist_policy = "deny"` — unknown peers rejected
- All management endpoints on loopback or Tailscale only
- Node identity keys: 600 permissions (`node.key`, `ods-node.key`)
- `allow_remote_tasks: false` — remote task execution disabled by default
- `incoming.tool_access: safe` — restricted tool access for remote incoming tasks
- No Keryx services bind to public internet

## Recovery After Reboot

### VPS

Services auto-start via `WantedBy=default.target`. After reboot:
```bash
systemctl --user status keryxd keryx-relay
curl -s http://127.0.0.1:18081/health
```

### Katana

Services auto-start via `WantedBy=default.target`. After reboot:
```bash
ssh <KATANA_HOST> 'sudo -u kyle XDG_RUNTIME_DIR=/run/user/$(id -u kyle) \
  systemctl --user status keryxd keryx-node-refresh keryx-ods-node-refresh keryx-task-bridge'
```

### Full recovery sequence

```bash
# 1. VPS
systemctl --user restart keryxd keryx-relay
sleep 5
curl -s http://127.0.0.1:18081/health

# 2. Katana
ssh <KATANA_HOST> 'sudo -u kyle XDG_RUNTIME_DIR=/run/user/$(id -u kyle) \
  systemctl --user restart keryxd keryx-node-refresh keryx-ods-node-refresh keryx-task-bridge'

# 3. Wait for reconnection (~60-120s)
sleep 120

# 4. Verify
curl -s http://127.0.0.1:18081/health
hermes agency discover ods
```

## Log Paths

| Host | Service | Log |
|---|---|---|
| VPS | keryxd | `~/.hermes/.keryx/logs/keryxd.log` |
| VPS | keryx-relay | `~/.hermes/.keryx/logs/keryx-relay.log` |
| Katana | keryxd | `/home/kyle/.hermes/.keryx/logs/keryxd.log` |
| Katana | keryx-node | `/home/kyle/.hermes/.keryx/logs/keryx-node.log` |
| Katana | systemd | `journalctl --user -u <service-name> -n 50` |
