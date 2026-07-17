#!/usr/bin/env bash
# Builds the videoflow base image and every per-component family image.
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

for family in basic vision video-io; do
  IMG="${PREFIX}videoflow-${family}:${TAG}"
  echo "Building ${family} -> ${IMG}"
  docker build -f "docker/${family}/Dockerfile" \
    --build-arg "BASE_IMAGE=${BASE_TAG}" \
    -t "${IMG}" .
done

echo "Done. Built: base + basic, vision, video-io"
if [ -n "$REGISTRY" ]; then
  echo "Push with:"
  echo "  docker push ${BASE_TAG}"
  for family in basic vision video-io; do
    echo "  docker push ${PREFIX}videoflow-${family}:${TAG}"
  done
fi
