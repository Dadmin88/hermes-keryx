#!/usr/bin/env python3
from pathlib import Path

path = Path('scripts/phase17_apply.py')
text = path.read_text(encoding='utf-8')

block = '''# The replacement above consumes the existing decorator; repair the exact neighboring block.
replace_once(
    MODELS,
    "@dataclass(slots=True)\\n@dataclass(slots=True)\\nclass ClaimedTask:",
    "@dataclass(slots=True)\\nclass ClaimedTask:",
)

'''
if block not in text:
    raise SystemExit('expected obsolete decorator repair block was not found')
text = text.replace(block, '', 1)

docstring = '            """A task atomically dequeued from the daemon for worker execution."""\n'
if docstring not in text:
    raise SystemExit('expected nested ClaimedTask docstring was not found')
text = text.replace(
    docstring,
    '            # A task atomically dequeued from the daemon for worker execution.\n',
    1,
)

old_helpers = '''def replace_once(path: str, old: str, new: str) -> None:
    file = Path(path)
    text = file.read_text(encoding="utf-8")
    count = text.count(old)
    if count != 1:
        raise SystemExit(f"{path}: expected one match, found {count}: {old[:160]!r}")
    file.write_text(text.replace(old, new, 1), encoding="utf-8")


def insert_before(path: str, marker: str, addition: str) -> None:
    file = Path(path)
    text = file.read_text(encoding="utf-8")
    if addition.strip() in text:
        return
    count = text.count(marker)
    if count != 1:
        raise SystemExit(f"{path}: expected one marker, found {count}: {marker[:160]!r}")
    file.write_text(text.replace(marker, addition.rstrip() + "\\n\\n" + marker, 1), encoding="utf-8")
'''
new_helpers = '''def _indent_block(value: str, prefix: str = "    ") -> str:
    return "".join(prefix + line if line.strip() else line for line in value.splitlines(keepends=True))


def replace_once(path: str, old: str, new: str) -> None:
    file = Path(path)
    text = file.read_text(encoding="utf-8")
    count = text.count(old)
    if count == 0:
        indented_old = _indent_block(old)
        if text.count(indented_old) == 1:
            old = indented_old
            new = _indent_block(new)
            count = 1
    if count != 1:
        raise SystemExit(f"{path}: expected one match, found {count}: {old[:160]!r}")
    file.write_text(text.replace(old, new, 1), encoding="utf-8")


def insert_before(path: str, marker: str, addition: str) -> None:
    file = Path(path)
    text = file.read_text(encoding="utf-8")
    addition = addition.rstrip()
    if marker.startswith("    ") or marker.startswith("}\\n\\n/// Serve the minimal local daemon RPC surface"):
        if not addition.startswith("    "):
            addition = _indent_block(addition)
    if addition.strip() in text:
        return
    count = text.count(marker)
    if count != 1:
        raise SystemExit(f"{path}: expected one marker, found {count}: {marker[:160]!r}")
    file.write_text(text.replace(marker, addition + "\\n\\n" + marker, 1), encoding="utf-8")
'''
if old_helpers not in text:
    raise SystemExit('expected original generator helpers were not found')
text = text.replace(old_helpers, new_helpers, 1)

path.write_text(text, encoding='utf-8')
