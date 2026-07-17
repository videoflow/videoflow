#!/usr/bin/env bash
# Regenerate the checked-in Python protobuf bindings from spec/proto/.
#
# Output lands at videoflow/v1/{value,envelope,payloads}_pb2.py (+ .pyi stubs).
# The generated files are checked in so users of the package never need protoc.
# Run this whenever a .proto in spec/proto/ changes, then commit the result.
#
# Uses grpc_tools.protoc (pip: grpcio-tools) so no external protoc/buf binary is
# required. The proto import root is spec/proto and the Python output root is the
# repo root, so a module declared `package videoflow.v1;` at
# spec/proto/videoflow/v1/foo.proto generates to videoflow/v1/foo_pb2.py and its
# cross-file imports (`from videoflow.v1 import value_pb2`) resolve against the
# installed videoflow package.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

PROTO_ROOT="spec/proto"
PYTHON="${PYTHON:-python}"

PROTOS=(
  "videoflow/v1/value.proto"
  "videoflow/v1/envelope.proto"
  "videoflow/v1/payloads.proto"
)

mkdir -p videoflow/v1

"$PYTHON" -m grpc_tools.protoc \
  --proto_path="$PROTO_ROOT" \
  --python_out=. \
  --pyi_out=. \
  "${PROTOS[@]/#/$PROTO_ROOT/}"

# Ensure the generated package is importable.
if [ ! -f videoflow/v1/__init__.py ]; then
  echo "# Generated protobuf bindings for videoflow protocol v1 (see spec/proto/)." > videoflow/v1/__init__.py
fi

echo "Generated: videoflow/v1/{value,envelope,payloads}_pb2.py"
