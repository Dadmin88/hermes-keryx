# RFC 005: Security Model

## Local daemon access

`keryxd` listens on a Unix socket by default. Socket files must be created under `~/.hermes/keryx/run/` with owner-only permissions. Loopback TCP is only a development/Windows fallback and must not expose remote control by default.

## Relay authentication

Initial relay authentication uses node tokens. Tokens identify a node allowed to connect, publish capabilities, fetch mailbox items, and exchange relay frames. Future signed challenge auth can replace or augment bearer-style tokens without changing task semantics.

## Identity

Node identity is rooted in a node key. Agent identity is scoped under a node and must not be globally trusted without relay/node context. Capabilities are declared by agents and evaluated by policy before routing.

## Capability permissions and approvals

Policy decides whether a caller may dispatch to a capability and whether approval is required. Approval-required tasks enter `AwaitingApproval` and emit audit events before execution.

## Secret handling

Secrets must be redacted from logs, doctor output, events, and error metadata. Secret-looking values should never be persisted as task metadata unless explicitly marked as secure artifact references.

## Audit events

Security-sensitive decisions emit events: policy denial, approval requested/granted/denied, node auth failure, key rotation, revoked-node rejection, and recovery actions.
