#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../../.." && pwd)"
PY_SDK="$(cd "$(dirname "$0")/.." && pwd)"
PROTO_ROOT="$ROOT/proto"
OUT="$PY_SDK/keryx/proto"
EXPECTED_GRPC_TOOLS_VERSION="1.83.0"

ACTUAL_GRPC_TOOLS_VERSION="$(python -c 'from importlib.metadata import version; print(version("grpcio-tools"))')"
if [[ "$ACTUAL_GRPC_TOOLS_VERSION" != "$EXPECTED_GRPC_TOOLS_VERSION" ]]; then
  echo "grpcio-tools $EXPECTED_GRPC_TOOLS_VERSION is required (found $ACTUAL_GRPC_TOOLS_VERSION)" >&2
  exit 1
fi

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