#!/usr/bin/env bash
# Builds the videoflow base images (framework + broker client + built-in node deps).
# Your own nodes go in your own image built FROM one of these — see
# docker/user-image.example.Dockerfile.
#
# The whole project targets Python 3.12. Two bases are built:
#   - videoflow-base:py3.12       CPU     (python:3.12-slim)
#   - videoflow-base:py3.12-cuda  GPU     (CUDA 12.4 + cuDNN, Ubuntu 24.04 / py3.12)
# The CPU image is also tagged videoflow-base:${TAG} (default latest).
#
# videoflow-contrib components ship a CPU Dockerfile (FROM videoflow-base:py3.12) and,
# when they can use a GPU, a Dockerfile.gpu (FROM videoflow-base:py3.12-cuda).
#
# Run from the repo root:  ./docker/build-images.sh [REGISTRY] [TAG]
#
#   REGISTRY  optional image registry prefix, e.g. ghcr.io/acme (default: none/local)
#   TAG       tag applied to the CPU base (default: latest)
set -euo pipefail

REGISTRY="${1:-}"
TAG="${2:-latest}"
PREFIX=""
if [ -n "$REGISTRY" ]; then
  PREFIX="${REGISTRY%/}/"
fi

CPU_TAG="${PREFIX}videoflow-base:py3.12"
GPU_TAG="${PREFIX}videoflow-base:py3.12-cuda"
DEFAULT_TAG="${PREFIX}videoflow-base:${TAG}"

echo "Building CPU base -> ${CPU_TAG}"
docker build -f docker/base/Dockerfile --build-arg PYTHON_VERSION=3.12 -t "${CPU_TAG}" .
docker tag "${CPU_TAG}" "${DEFAULT_TAG}"

echo "Building GPU base -> ${GPU_TAG}"
docker build -f docker/base/Dockerfile.gpu -t "${GPU_TAG}" .

echo "Done. Built:"
for t in "${CPU_TAG}" "${DEFAULT_TAG}" "${GPU_TAG}"; do echo "  ${t}"; done
if [ -n "$REGISTRY" ]; then
  echo "Push with:"
  for t in "${CPU_TAG}" "${DEFAULT_TAG}" "${GPU_TAG}"; do echo "  docker push ${t}"; done
fi
