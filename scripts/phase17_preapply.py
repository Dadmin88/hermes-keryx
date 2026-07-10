#!/usr/bin/env python3
from pathlib import Path

path = Path("scripts/phase17_apply.py")
text = path.read_text(encoding="utf-8")
old = '''def replace_once(path: str, old: str, new: str) -> None:
    file = Path(path)
    text = file.read_text(encoding="utf-8")
    count = text.count(old)
    if count != 1:
        raise SystemExit(f"{path}: expected one match, found {count}: {old[:140]!r}")
    file.write_text(text.replace(old, new, 1), encoding="utf-8")
'''
new = '''def replace_once(path: str, old: str, new: str) -> None:
    file = Path(path)
    text = file.read_text(encoding="utf-8")
    count = text.count(old)
    if count == 0 and old.endswith("\\n"):
        without_final_newline = old.rstrip("\\n")
        if text.count(without_final_newline) == 1:
            old = without_final_newline
            new = new.rstrip("\\n")
            count = 1
    if count != 1:
        raise SystemExit(f"{path}: expected one match, found {count}: {old[:140]!r}")
    file.write_text(text.replace(old, new, 1), encoding="utf-8")
'''
if old not in text:
    raise SystemExit("expected replace_once helper was not found")
path.write_text(text.replace(old, new, 1), encoding="utf-8")
