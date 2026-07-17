#!/usr/bin/env bash
# Build and publish videoflow to PyPI with uv.
set -euo pipefail

rm -rf dist/
uv build                       # builds sdist + wheel into dist/
uv publish                     # reads UV_PUBLISH_TOKEN (or --token) for auth
