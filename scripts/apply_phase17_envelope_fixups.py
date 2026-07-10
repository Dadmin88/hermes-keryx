#!/usr/bin/env python3
"""Apply follow-up Phase 17.1 integration fixups.

Temporary build helper. Remove before proposing the implementation to main.
"""

from pathlib import Path


def replace_once(path: str, old: str, new: str) -> None:
    file = Path(path)
    text = file.read_text(encoding="utf-8")
    count = text.count(old)
    if count != 1:
        raise SystemExit(f"{path}: expected one match, found {count}: {old[:160]!r}")
    file.write_text(text.replace(old, new, 1), encoding="utf-8")


replace_once(
    "crates/keryx-daemon/src/lib.rs",
    '''        StoreError::TaskAlreadyExists(task_id) => {
            Status::already_exists(format!("task already exists: {task_id}"))
        }
        StoreError::ArtifactNotFound(artifact_id) => {''',
    '''        StoreError::TaskAlreadyExists(task_id) => {
            Status::already_exists(format!("task already exists: {task_id}"))
        }
        StoreError::TaskEnvelopeNotFound(task_id) => {
            Status::not_found(format!("task envelope not found: {task_id}"))
        }
        StoreError::TaskEnvelopeMismatch {
            task_id,
            envelope_task_id,
        } => Status::failed_precondition(format!(
            "task envelope id {} does not match task {}",
            envelope_task_id.as_str(),
            task_id.as_str()
        )),
        StoreError::TaskEnvelopeConflict(task_id) => Status::already_exists(format!(
            "task envelope conflicts with the stored envelope for task {}",
            task_id.as_str()
        )),
        StoreError::ArtifactNotFound(artifact_id) => {''',
)

replace_once(
    "crates/keryx-cli/tests/daemon_client.rs",
    "store: ready sqlite schema_version=5 supported_schema_version=5",
    "store: ready sqlite schema_version=6 supported_schema_version=6",
)
