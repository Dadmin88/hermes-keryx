# RFC 008: Identity and Authentication

## Node identity

Node identity is rooted in `~/.hermes/keryx/identity/node.key`. Node IDs are derived from key material or explicitly configured during recovery.

## Agent identity

Agent IDs are scoped to a node and use stable, human-readable identifiers where possible. Relay-visible identities include node context.

## Relay authentication

Initial relay auth uses node token auth. Future signed challenge auth should use the node key without changing task lifecycle semantics.

## Key rotation and revocation

Key rotation creates an auditable identity transition. Revoked nodes lose relay access and mailbox fetch permissions. Lost key recovery creates a new node identity unless an operator-approved recovery flow is defined.
