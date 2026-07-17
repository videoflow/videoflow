#!/usr/bin/env bash
# Builds the videoflow base image (framework + broker client + built-in node deps).
# Your own nodes go in your own image built FROM this one — see
# docker/user-image.example.Dockerfile.
#
# It builds the base at several Python versions (tagged videoflow-base:py<ver>) so that
# downstream node images — e.g. the videoflow-contrib components — can each pick the
# highest interpreter their ML framework supports:
#   - py3.13: framework-free / modern-dep nodes (opencv, numpy, torch, scipy)
#   - py3.11: TensorFlow-2 (Keras-2) nodes and detectron2 nodes
#   - py3.9 : MXNet/GluonCV nodes (detector_mxnet, person_reid, retinaface)
# The highest version is also tagged videoflow-base:${TAG} (default latest).
#
# Run from the repo root:  ./docker/build-images.sh [REGISTRY] [TAG]
#
#   REGISTRY  optional image registry prefix, e.g. ghcr.io/acme (default: none/local)
#   TAG       tag applied to the default (highest) interpreter build (default: latest)
set -euo pipefail

REGISTRY="${1:-}"
TAG="${2:-latest}"
PREFIX=""
if [ -n "$REGISTRY" ]; then
  PREFIX="${REGISTRY%/}/"
fi

# Python interpreters to build the base for. The first one is also tagged ${TAG}.
PYTHON_VERSIONS=(3.13 3.11 3.9)
DEFAULT_VERSION="${PYTHON_VERSIONS[0]}"

BUILT=()
for PYVER in "${PYTHON_VERSIONS[@]}"; do
  PY_TAG="${PREFIX}videoflow-base:py${PYVER}"
  echo "Building base (python ${PYVER}) -> ${PY_TAG}"
  docker build -f docker/base/Dockerfile --build-arg "PYTHON_VERSION=${PYVER}" -t "${PY_TAG}" .
  BUILT+=("${PY_TAG}")
  if [ "${PYVER}" = "${DEFAULT_VERSION}" ]; then
    DEFAULT_TAG="${PREFIX}videoflow-base:${TAG}"
    docker tag "${PY_TAG}" "${DEFAULT_TAG}"
    BUILT+=("${DEFAULT_TAG}")
  fi
done

echo "Done. Built:"
for t in "${BUILT[@]}"; do echo "  ${t}"; done
if [ -n "$REGISTRY" ]; then
  echo "Push with:"
  for t in "${BUILT[@]}"; do echo "  docker push ${t}"; done
fi
