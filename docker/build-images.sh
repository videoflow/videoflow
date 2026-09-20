#!/usr/bin/env bash
# Builds the videoflow base images (framework + broker client + built-in node deps).
# Your own nodes go in your own image built FROM one of these — see
# docker/user-image.example.Dockerfile.
#
# The whole project targets Python 3.12. Two bases are built:
#   - videoflow-base:py3.12       CPU     (python:3.12-slim)
#   - videoflow-base:py3.12-cuda  GPU     (CUDA 12.4 + cuDNN, Ubuntu 24.04 / py3.12)
# The CPU image is also tagged videoflow-base:${TAG} and the GPU one videoflow-base:${TAG}-cuda
# (default latest) — with a REGISTRY and a version TAG that is exactly the published naming
# scheme videoflow/deploy/build.py pulls on a wheel install:
#   ./docker/build-images.sh ghcr.io/videoflow 1.2.0    # then the printed docker push lines
# (the release workflow does this for every version; this is the manual form.)
#
# videoflow-contrib components ship a CPU Dockerfile (FROM videoflow-base:py3.12) and,
# when they can use a GPU, a gpu.Dockerfile (FROM videoflow-base:py3.12-cuda).
#
# Run from the repo root:  ./docker/build-images.sh [REGISTRY] [TAG]
#
#   REGISTRY  optional image registry prefix, e.g. ghcr.io/acme (default: none/local)
#   TAG       version tag applied to both bases, CPU as :TAG and GPU as :TAG-cuda (default: latest)
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
DEFAULT_GPU_TAG="${PREFIX}videoflow-base:${TAG}-cuda"

echo "Building CPU base -> ${CPU_TAG}"
docker build -f docker/base/Dockerfile --build-arg PYTHON_VERSION=3.12 -t "${CPU_TAG}" .
docker tag "${CPU_TAG}" "${DEFAULT_TAG}"

echo "Building GPU base -> ${GPU_TAG}"
docker build -f docker/base/Dockerfile.gpu -t "${GPU_TAG}" .
docker tag "${GPU_TAG}" "${DEFAULT_GPU_TAG}"

echo "Done. Built:"
for t in "${CPU_TAG}" "${DEFAULT_TAG}" "${GPU_TAG}" "${DEFAULT_GPU_TAG}"; do echo "  ${t}"; done
if [ -n "$REGISTRY" ]; then
  echo "Push with:"
  for t in "${CPU_TAG}" "${DEFAULT_TAG}" "${GPU_TAG}" "${DEFAULT_GPU_TAG}"; do echo "  docker push ${t}"; done
fi
