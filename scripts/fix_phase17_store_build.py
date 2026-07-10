#!/usr/bin/env python3
"""Apply focused compile fixes after the Phase 17 store source is generated."""
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

results = ROOT / "crates/keryx-store/src/results.rs"
text = results.read_text()
text = text.replace("use std::collections::HashSet;\n\n", "")
text = text.replace(
    "mod tests {\n    use super::*;\n",
    "mod tests {\n    use std::collections::HashSet;\n\n    use super::*;\n",
)
results.write_text(text)

daemon = ROOT / "crates/keryx-daemon/src/lib.rs"
text = daemon.read_text()
anchor = '''        StoreError::TaskEnvelopeConflict(task_id) => Status::already_exists(format!(
            "task envelope conflicts with the stored envelope for task {}",
            task_id.as_str()
        )),
'''
addition = anchor + '''        StoreError::TransportContextNotFound(task_id) => Status::not_found(format!(
            "transport context not found for task {}",
            task_id.as_str()
        )),
        StoreError::TransportContextConflict(task_id) => Status::already_exists(format!(
            "transport context conflicts for task {}",
            task_id.as_str()
        )),
        StoreError::TransportContextTaskMismatch {
            task_id,
            context_task_id,
        } => Status::failed_precondition(format!(
            "transport context task {} does not match task {}",
            context_task_id.as_str(),
            task_id.as_str()
        )),
        StoreError::TerminalResultNotFound(task_id) => Status::not_found(format!(
            "terminal result not found for task {}",
            task_id.as_str()
        )),
        StoreError::TerminalResultConflict(task_id) => Status::already_exists(format!(
            "terminal result conflicts for task {}",
            task_id.as_str()
        )),
        StoreError::TerminalResultTaskMismatch {
            task_id,
            result_task_id,
        } => Status::failed_precondition(format!(
            "terminal result task {} does not match task {}",
            result_task_id.as_str(),
            task_id.as_str()
        )),
        StoreError::TerminalResultNotTerminal(task_id) => Status::failed_precondition(format!(
            "terminal result for task {} is not terminal",
            task_id.as_str()
        )),
        StoreError::ResultDeliveryLeaseMismatch(delivery_id) => Status::permission_denied(
            format!("result delivery lease mismatch: {delivery_id}"),
        ),
        StoreError::RemoteResultExecutorMismatch {
            task_id,
            expected,
            actual,
        } => Status::permission_denied(format!(
            "remote result executor mismatch for task {}: expected {}, got {}",
            task_id.as_str(),
            expected.as_str(),
            actual.as_str()
        )),
'''
if addition not in text:
    if anchor not in text:
        raise RuntimeError("daemon StoreError mapping anchor not found")
    text = text.replace(anchor, addition, 1)
daemon.write_text(text)

for relative in [
    "crates/keryx-cli/tests/daemon_client.rs",
    "crates/keryx-store/tests/artifact_store.rs",
    "crates/keryx-store/tests/envelope_store.rs",
]:
    path = ROOT / relative
    text = path.read_text()
    text = text.replace(
        "store: ready sqlite schema_version=6 supported_schema_version=6",
        "store: ready sqlite schema_version=7 supported_schema_version=7",
    )
    text = text.replace("schema_version().await.unwrap(), 6", "schema_version().await.unwrap(), 7")
    text = text.replace("CURRENT_SCHEMA_VERSION, 6", "CURRENT_SCHEMA_VERSION, 7")
    text = text.replace("schema_is_v6", "schema_is_v7")
    path.write_text(text)
