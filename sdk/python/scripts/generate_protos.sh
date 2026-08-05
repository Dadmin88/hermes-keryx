#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../../.." && pwd)"
PY_SDK="$(cd "$(dirname "$0")/.." && pwd)"
PROTO_ROOT="$ROOT/proto"
OUT="$PY_SDK/keryx/proto"

mkdir -p "$OUT"
python -m grpc_tools.protoc \
  -I"$PROTO_ROOT" \
  --python_out="$OUT" \
  --grpc_python_out="$OUT" \
  "$PROTO_ROOT"/hermes/keryx/v1/*.proto

# Ensure package inits exist
touch "$OUT/hermes/__init__.py" \
      "$OUT/hermes/keryx/__init__.py" \
      "$OUT/hermes/keryx/v1/__init__.py"