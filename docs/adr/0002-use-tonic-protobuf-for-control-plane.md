# ADR 0002: Use Tonic Protobuf For Control Plane

## Status

Accepted

## Context

Hermes Keryx needs durable, standalone runtime decisions before implementation expands across daemon, relay, SDK, and operations tracks.

## Decision

Adopt this plan default for the standalone Keryx v1 unless a later ADR supersedes it.

## Consequences

The decision is documented up front so implementation agents can proceed without re-litigating baseline technology choices.
