#!/usr/bin/env bash
# Builds the videoflow base image (framework + broker client + built-in node deps).
# Your own nodes go in your own image built FROM this one — see
# docker/user-image.example.Dockerfile.
#
# Run from the repo root:  ./docker/build-images.sh [REGISTRY] [TAG]
#
#   REGISTRY  optional image registry prefix, e.g. ghcr.io/acme (default: none/local)
#   TAG       image tag (default: latest)
set -euo pipefail

REGISTRY="${1:-}"
TAG="${2:-latest}"
PREFIX=""
if [ -n "$REGISTRY" ]; then
  PREFIX="${REGISTRY%/}/"
fi

BASE_TAG="${PREFIX}videoflow-base:${TAG}"

echo "Building base -> ${BASE_TAG}"
docker build -f docker/base/Dockerfile -t "${BASE_TAG}" .

echo "Done. Built: ${BASE_TAG}"
if [ -n "$REGISTRY" ]; then
  echo "Push with:  docker push ${BASE_TAG}"
fi
