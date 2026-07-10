#!/usr/bin/env python3
from pathlib import Path

path = Path("scripts/phase17_apply.py")
text = path.read_text(encoding="utf-8")
old_replace = '''def replace_once(path: str, old: str, new: str) -> None:
    file = Path(path)
    text = file.read_text(encoding="utf-8")
    count = text.count(old)
    if count != 1:
        raise SystemExit(f"{path}: expected one match, found {count}: {old[:140]!r}")
    file.write_text(text.replace(old, new, 1), encoding="utf-8")
'''
new_replace = '''def _indent_block(value: str, prefix: str) -> str:
    return "".join(prefix + line if line.strip() else line for line in value.splitlines(keepends=True))


def replace_once(path: str, old: str, new: str) -> None:
    file = Path(path)
    text = file.read_text(encoding="utf-8")
    count = text.count(old)
    if count == 0:
        for depth in range(1, 6):
            prefix = "    " * depth
            nested_old = _indent_block(old, prefix)
            if text.count(nested_old) == 1:
                old = nested_old
                new = _indent_block(new, prefix)
                count = 1
                break
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
if old_replace not in text:
    raise SystemExit("expected replace_once helper was not found")
text = text.replace(old_replace, new_replace, 1)

old_insert = '''def insert_before(path: str, marker: str, addition: str) -> None:
    file = Path(path)
    text = file.read_text(encoding="utf-8")
    count = text.count(marker)
    if count != 1:
        raise SystemExit(f"{path}: expected one marker, found {count}: {marker[:140]!r}")
    file.write_text(text.replace(marker, addition.rstrip() + "\\n\\n" + marker, 1), encoding="utf-8")
'''
new_insert = '''def insert_before(path: str, marker: str, addition: str) -> None:
    file = Path(path)
    text = file.read_text(encoding="utf-8")
    count = text.count(marker)
    if count != 1:
        raise SystemExit(f"{path}: expected one marker, found {count}: {marker[:140]!r}")
    addition = addition.rstrip()
    marker_prefix = marker[: len(marker) - len(marker.lstrip(" "))]
    if marker_prefix and not addition.startswith(marker_prefix):
        addition = _indent_block(addition, marker_prefix)
    file.write_text(text.replace(marker, addition + "\\n\\n" + marker, 1), encoding="utf-8")
'''
if old_insert not in text:
    raise SystemExit("expected insert_before helper was not found")
text = text.replace(old_insert, new_insert, 1)

newline_join = '                text = "\\n".join(part.text for part in artifact.parts if part.text)\n'
escaped_newline_join = '                text = "\\\\n".join(part.text for part in artifact.parts if part.text)\n'
if text.count(newline_join) != 1:
    raise SystemExit("expected artifact newline join was not found exactly once")
text = text.replace(newline_join, escaped_newline_join, 1)

result_join = '                result_metadata["result_text"] = "\\n\\n".join(result_texts)[:65_536]\n'
escaped_result_join = '                result_metadata["result_text"] = "\\\\n\\\\n".join(result_texts)[:65_536]\n'
if text.count(result_join) != 1:
    raise SystemExit("expected result newline join was not found exactly once")
text = text.replace(result_join, escaped_result_join, 1)

path.write_text(text, encoding="utf-8")
