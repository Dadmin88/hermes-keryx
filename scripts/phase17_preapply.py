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
path.write_text(text, encoding='utf-8')
