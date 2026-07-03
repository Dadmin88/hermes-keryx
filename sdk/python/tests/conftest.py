"""Shared pytest fixtures for the Keryx Python SDK."""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

PROTO_ROOT = ROOT / "keryx" / "proto"
if str(PROTO_ROOT) not in sys.path:
    sys.path.insert(0, str(PROTO_ROOT))