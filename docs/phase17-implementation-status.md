# Phase 17 implementation status

## Completed on this branch

- schema version 7 migration
- authenticated task transport context records
- durable canonical terminal result records
- durable result-delivery outbox records
- atomic task completion plus terminal-result persistence
- atomic terminal failure plus result persistence
- retryable result-delivery leasing, acknowledgement, and failure transitions
- idempotent duplicate result handling
- executor identity conflict checks
- SQLite restart coverage and in-memory parity

## Remaining dependency order

1. Wire canonical results into daemon completion and failure RPCs.
2. Add authenticated task and result relay-frame contracts.
3. Drain destination result outboxes through the edge node.
4. Persist and acknowledge remote results at the origin daemon.
5. Connect Python `TaskHandle.wait()` and cancellation to durable state.
6. Prove the complete two-daemon, two-edge topology.
7. Synchronize the proven SDK into Hermes Agency.
8. Connect Agency reconciliation, Kanban state, Fabric events, and the demo command.

Public product claims must continue to describe the remote result path as incomplete until the two-node topology proof passes.
