# RFC 009: Artifact Storage

## Model

Small text and JSON artifacts may be inline task parts. Larger artifacts are represented by metadata rows plus content-addressed blob references.

## Local storage

`keryxd` owns a local blob store under `~/.hermes/keryx/blobs/`. The SQLite store tracks artifact metadata, digest, media type, byte length, task ownership, and retention policy.

## Relay storage

Relay artifact transfer is deferred. v1 relay tasks may carry artifact references and metadata; future relay blob transfer must preserve digest verification and retention rules.

## Limits

Artifact limits are explicit policy/config values. Oversized inline artifacts are rejected with typed errors instead of silently truncating.
