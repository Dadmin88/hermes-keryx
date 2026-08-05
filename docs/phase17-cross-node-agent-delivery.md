# Phase 17: durable cross-node agent delivery

Status: **implemented and verified**

- Tracking issue: [#10](https://github.com/DeployFaith/hermes-keryx/issues/10) — closed
- Implementation: [PR #29](https://github.com/DeployFaith/hermes-keryx/pull/29)
- Merge commit: `906823badac04fd9d159c4da927dda5c25d712dc`
- Permanent proof: `scripts/e2e_two_node.py` and `.github/workflows/phase17-e2e.yml`

## Implemented round trip

```text
sender Python SDK
  -> sender keryxd SendTask
  -> authenticated relay PublishTask
  -> destination keryx-node stream
  -> destination keryxd SubmitRemoteTask
  -> durable lifecycle row + full envelope
  -> Python worker ClaimNextTask
  -> registered on_task() handler
  -> durable terminal result + result-delivery outbox
  -> authenticated relay result frame
  -> origin keryxd IngestRemoteResult
  -> TaskHandle.wait()
```

The implementation includes:

- durable full-envelope retention;
- atomic compatible-task claims and worker leases;
- Python `serve_forever()` worker dispatch and heartbeats;
- authenticated sender and executor identities;
- durable terminal results and retryable result delivery;
- `TaskHandle.wait()` result observation and cancellation;
- returned artifact descriptors/text previews;
- idempotent result ingestion and acknowledgement;
- a real two-daemon/two-edge-node process harness.

## Verification contract

Run from the repository root:

```bash
cargo fmt --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
cargo build --workspace --bins

python -m pip install -e "sdk/python[dev]"
python -m pytest sdk/python/tests -q
python scripts/e2e_two_node.py --bin-dir target/debug
```

The E2E proof uses isolated SQLite stores, dynamic loopback ports, separate node identities/tokens, a real Python receiver worker, and preserved logs on failure. It verifies skill discovery, authenticated sender identity, remote handler execution, terminal result return, authenticated executor identity, and returned artifact descriptors/bounded text previews. It does not transport artifact bytes; daemon artifact CRUD remains node-local.

## Definition of done

- [x] Full envelopes survive durable submission.
- [x] A receiver worker atomically claims the next compatible task.
- [x] Python `serve_forever()` invokes the registered handler.
- [x] Completion/failure results propagate to the origin.
- [x] Sender `TaskHandle.wait()` receives terminal status and artifact descriptors/bounded text previews.
- [x] Sender and executor identities are transport-authenticated.
- [x] Two-daemon/two-edge-node harness is repeatable.
- [x] Rust workspace gates pass at the merged checkpoint.
- [x] Python SDK tests pass at the merged checkpoint.
- [x] Authenticated cross-process E2E passes at the merged checkpoint.

## Current limitations

- The relay offline mailbox is in-memory. It handles reconnects to the same relay process but does not survive relay restarts.
- The high-level Python `Skill`/`register_skills()` path does not yet propagate protocol-level skill tags.
- The high-level Python `send_task()` helper does not expose every lower-level envelope field or routing option.
- `TaskHandle` observes results by bounded polling rather than a streaming subscription.
