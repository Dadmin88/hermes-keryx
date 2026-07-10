#!/usr/bin/env python3
from pathlib import Path

path = Path('scripts/phase17_apply.py')
text = path.read_text(encoding='utf-8')
block = '''# The replacement above consumes the existing decorator; repair the exact neighboring block.\nreplace_once(\n    MODELS,\n    "@dataclass(slots=True)\\n@dataclass(slots=True)\\nclass ClaimedTask:",\n    "@dataclass(slots=True)\\nclass ClaimedTask:",\n)\n\n'''
if block not in text:
    raise SystemExit('expected obsolete decorator repair block was not found')
path.write_text(text.replace(block, '', 1), encoding='utf-8')
