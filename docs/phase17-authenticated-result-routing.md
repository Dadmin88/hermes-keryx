# Authenticated Phase 17 result routing

The Phase 17 control plane now binds task and result source identities to configured Keryx node tokens.

When node-token authentication is configured, the relay rejects missing, invalid, revoked, or mismatched node credentials before routing task frames, result frames, or acknowledgements. Terminal-result publication fails closed when node-token authentication is not configured, because a claimed executor identity is not an authenticated identity. The two-node proof configures separate sender and receiver credentials and verifies the complete terminal-result path, including bounded artifact bytes, canonical origin descriptors, verified retrieval, and explicit-path download.
