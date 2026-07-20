#!/usr/bin/env bash
#
# Build the Sphinx site into docs/public/.
#
# docs/public/ is the *published* directory (it is what Firebase Hosting serves), so
# unlike docs/build/ it is committed to the repo. The pre-commit hook in
# .pre-commit-config.yaml runs this whenever docs/source/ changes and stages the
# result, which keeps the published HTML from drifting behind the sources.
#
# Two details worth keeping:
#
#   * The output directory is wiped first. Sphinx never deletes pages whose source
#     was removed, so an incremental build into a committed directory would keep
#     serving stale pages forever. A cold build takes a few seconds — not worth the
#     ambiguity.
#   * Doctrees go to docs/.doctrees (gitignored), not the default docs/public/.doctrees.
#     They are ~4MB of build-cache pickles that have no business in a hosting root.
#
# Sphinx is not a project dependency — it is only needed to build the docs — so it is
# pulled in per-run from docs/source/requirements.txt on top of the project environment.
# The project env is required as well: autodoc imports videoflow itself (the heavy
# optional deps are mocked in conf.py, but the package must be importable).
#
# Usage: ./scripts/build-docs.sh
set -euo pipefail

cd "$(dirname "$0")/.."

SOURCE_DIR="docs/source"
OUTPUT_DIR="docs/public"
DOCTREES_DIR="docs/.doctrees"

rm -rf "$OUTPUT_DIR"

uv run --with-requirements "$SOURCE_DIR/requirements.txt" \
    sphinx-build -b html -d "$DOCTREES_DIR" "$SOURCE_DIR" "$OUTPUT_DIR"
