# Authenticated Phase 17 result routing

The Phase 17 control plane now binds task and result source identities to configured Keryx node tokens.

When node-token authentication is configured, the relay rejects missing, invalid, revoked, or mismatched node credentials before routing task frames, result frames, or acknowledgements. Terminal-result publication fails closed when node-token authentication is not configured, because a claimed executor identity is not an authenticated identity. Descriptor-only results remain the compatibility baseline; bounded artifact bytes traverse only when the authenticated receiving/origin destination advertises `result_artifact_bytes_v1`. The two-node proof configures separate sender and receiver credentials and verifies that negotiated terminal-result path, canonical origin descriptors, verified retrieval, and explicit-path download.
