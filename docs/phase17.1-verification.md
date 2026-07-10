# Phase 17.1 verification

Phase 17.1 is implemented by pull request #16 and tracked by issue #12.

The verified boundary is durable envelope retention only:

- schema version 6 stores the complete encoded `TaskEnvelope`
- lifecycle acceptance, idempotency, accepted event, and envelope bytes commit atomically
- nested messages, binary parts, maps, and correlation data survive daemon restart
- format, Clippy with warnings denied, and the complete Rust workspace pass

Worker dequeue, handler execution, result routing, sender observation, and the real two-node E2E remain subsequent Phase 17 slices.
