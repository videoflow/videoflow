# Image for basic nodes (producers/basic, processors/basic, processors/aggregators,
# consumers/basic) — no dependencies beyond the base. Built as its own tag so the
# image registry can reference videoflow-basic uniformly with the other families.
#
#   docker build -f docker/base/Dockerfile   -t videoflow-base:latest .
#   docker build -f docker/basic/Dockerfile  -t videoflow-basic:latest .
ARG BASE_IMAGE=videoflow-base:latest
FROM ${BASE_IMAGE}

RUN uv pip install --system --no-cache ".[blob]"
