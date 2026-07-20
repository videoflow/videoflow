#!/usr/bin/env bash
#
# Build the Sphinx site into docs/public/.
#
# This is what .github/workflows/docs.yml runs before handing docs/public/ to GitHub Pages,
# and it is also how you preview the site locally — same script both places, so a build that
# works here works in CI. The output is gitignored: the published site is rebuilt from source
# on every push to master, never committed.
#
# Two details worth keeping:
#
#   * The output directory is wiped first. Sphinx never deletes pages whose source was
#     removed, so an incremental build would keep a stale page alive across renames and
#     deletions. A cold build takes a few seconds — not worth the ambiguity.
#   * Doctrees go to docs/.doctrees, not the default docs/public/.doctrees. They are ~4MB of
#     build-cache pickles that would otherwise be uploaded to Pages as part of the site.
#
# Sphinx is not a project dependency — it is only needed to build the docs — so it is pulled
# in per-run from docs/source/requirements.txt on top of the project environment. The project
# env is required as well: sphinxcontrib-apidoc + autodoc import videoflow itself to generate
# the API reference (the heavy optional deps are mocked in conf.py, but the package must be
# importable).
#
# Usage: ./scripts/build-docs.sh    then open docs/public/index.html
set -euo pipefail

cd "$(dirname "$0")/.."

SOURCE_DIR="docs/source"
OUTPUT_DIR="docs/public"
DOCTREES_DIR="docs/.doctrees"

rm -rf "$OUTPUT_DIR"

uv run --with-requirements "$SOURCE_DIR/requirements.txt" \
    sphinx-build -b html -d "$DOCTREES_DIR" "$SOURCE_DIR" "$OUTPUT_DIR"
