# Authenticated Phase 17 result routing

The Phase 17 control plane now binds task and result source identities to configured Keryx node tokens.

The relay rejects missing, invalid, revoked, or mismatched node credentials before routing task frames, result frames, or acknowledgements. The two-node proof configures separate sender and receiver credentials and verifies the complete terminal-result and artifact return path.
