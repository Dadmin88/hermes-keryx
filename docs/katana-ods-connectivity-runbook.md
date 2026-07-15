# Keryx Connectivity Runbook Template

This public document is a generic template for operating a private Keryx deployment. Do not add deployment-specific hostnames, peer IDs, account names, private multiaddrs, absolute maintainer paths, allowlist sizes, container names, or security posture details to this file. Keep those values in private operator notes on the relevant hosts.

## Topology Template

Use this template to document your private topology outside the repository:

```text
[operator host or relay]
  - keryxd daemon endpoint: <DAEMON_ENDPOINT>
  - relay libp2p endpoint: <RELAY_MULTIADDR>
  - relay health endpoint: <RELAY_HEALTH_ENDPOINT>
  - relay registry endpoint: <RELAY_REGISTRY_ENDPOINT>

[worker node]
  - keryxd daemon endpoint: <DAEMON_ENDPOINT>
  - keryx-node registration target: <RELAY_REGISTRY_ENDPOINT>
  - advertised skills: <PUBLIC_SKILL_LABELS_ONLY>
```

Keep the public skill labels broad enough for discovery without revealing private host roles or internal application inventory.

## Public-Safe Configuration Checklist

- Use placeholders such as `<RELAY_HOST>`, `<WORKER_HOST>`, `<PEER_ID>`, `<NODE_NAME>`, `<RUNTIME_ROOT>`, and `<SERVICE_NAME>`.
- Prefer documented Keryx defaults from `docs/current-product.md` and `docs/operations.md` instead of private deployment values.
- Store real peer IDs, private multiaddrs, node tokens, and host-local paths only in private runtime configuration.
- Avoid publishing local usernames, SSH command patterns, exact systemd unit names, Docker container names, private ports, allowlist counts, or key filenames.
- Review all examples before commit with a search for private usernames, absolute home paths, peer IDs, private hostnames, and deployment codenames.

## Generic Operations

### Status

```bash
HERMES_KERYX_DAEMON_ENDPOINT=http://<DAEMON_HOST>:<DAEMON_PORT> keryx status
```

### Doctor

```bash
HERMES_KERYX_DAEMON_ENDPOINT=http://<DAEMON_HOST>:<DAEMON_PORT> keryx doctor
```

### Relay registry

```bash
HERMES_KERYX_RELAY_REGISTRY_ENDPOINT=http://<RELAY_HOST>:<REGISTRY_PORT> \
  keryx relay registry list
```

### Node discovery

```bash
keryx node discover <SKILL_NAME>
```

### Task smoke test

```bash
HERMES_KERYX_DAEMON_ENDPOINT=http://<DAEMON_HOST>:<DAEMON_PORT> \
  keryx task submit <TASK_ID>
```

## Generic Troubleshooting

### Relay has no connected peers

Check that the relay is running, the node can reach the relay address, the node identity is authorized by your private relay policy, and the node registration process is active.

### Registry results expire

Node registrations are time-limited. Verify that your private node refresh mechanism is still running and that its registration interval is shorter than the registry TTL.

### Task does not complete

Confirm that a worker claimed the task, that the worker heartbeat is current, and that the task did not hit a deadline or cancellation path. Use private logs on the relevant hosts for deployment-specific details.

## Private Runbook Guidance

Private deployment runbooks may include hostnames, user accounts, absolute paths, service names, container names, allowlist details, and recovery commands, but those files must remain outside the public repository. If a private runbook needs to reference this template, link to the generic sections above and keep the sensitive values in the private document.
