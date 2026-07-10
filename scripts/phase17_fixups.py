#!/usr/bin/env python3
from pathlib import Path


def replace_exact(path: str, old: str, new: str, expected: int) -> None:
    file = Path(path)
    text = file.read_text(encoding="utf-8")
    count = text.count(old)
    if count != expected:
        raise SystemExit(f"{path}: expected {expected} occurrences of {old!r}, found {count}")
    file.write_text(text.replace(old, new), encoding="utf-8")


replace_exact(
    "crates/keryx-cli/tests/daemon_client.rs",
    "store: ready sqlite schema_version=6 supported_schema_version=6",
    "store: ready sqlite schema_version=7 supported_schema_version=7",
    1,
)
replace_exact(
    "crates/keryx-store/tests/artifact_store.rs",
    "assert_eq!(store.schema_version().await.unwrap(), 6);",
    "assert_eq!(store.schema_version().await.unwrap(), 7);",
    1,
)
replace_exact(
    "crates/keryx-store/tests/envelope_store.rs",
    "assert_eq!(CURRENT_SCHEMA_VERSION, 6);",
    "assert_eq!(CURRENT_SCHEMA_VERSION, 7);",
    1,
)
replace_exact(
    "crates/keryx-store/tests/envelope_store.rs",
    "sqlite_envelope_survives_reopen_and_schema_is_v6",
    "sqlite_envelope_survives_reopen_and_schema_is_v7",
    1,
)
replace_exact(
    "crates/keryx-store/tests/sqlite_store.rs",
    "assert_eq!(store.schema_version().await.unwrap(), 6);",
    "assert_eq!(store.schema_version().await.unwrap(), 7);",
    2,
)
