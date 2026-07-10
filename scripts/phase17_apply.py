#!/usr/bin/env python3
from pathlib import Path

path = Path('crates/keryx-store/tests/sqlite_store.rs')
text = path.read_text(encoding='utf-8')
old = 'assert_eq!(store.schema_version().await.unwrap(), 5);'
count = text.count(old)
if count != 2:
    raise SystemExit(f'expected exactly 2 stale schema assertions, found {count}')
path.write_text(text.replace(old, 'assert_eq!(store.schema_version().await.unwrap(), 6);'), encoding='utf-8')
