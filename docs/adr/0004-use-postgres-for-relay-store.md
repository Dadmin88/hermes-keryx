# ADR 0004: Use Postgres For Relay Store

## Status

Accepted

## Context

Hermes Keryx needs durable, standalone runtime decisions before implementation expands across daemon, relay, SDK, and operations tracks.

## Decision

Adopt this plan default for the standalone Keryx v1 unless a later ADR supersedes it.

## Consequences

The decision is documented up front so implementation agents can proceed without re-litigating baseline technology choices.
