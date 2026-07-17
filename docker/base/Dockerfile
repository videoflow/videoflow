# Base image for every videoflow worker. Contains the framework, the broker
# client, and the wire format — enough to run producer/processor/consumer nodes
# that have no heavy extra dependencies. Family images (basic, vision, video-io)
# extend this and add their own deps; the ENTRYPOINT is identical everywhere.
#
# Build from the repo root:
#   docker build -f docker/base/Dockerfile -t videoflow-base:latest .
FROM python:3.9-slim

ENV PYTHONUNBUFFERED=1

WORKDIR /app

# Install uv, then use it to install videoflow into the system environment.
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

COPY pyproject.toml README.md ./
COPY videoflow ./videoflow

RUN uv pip install --system --no-cache ".[distributed]"

ENTRYPOINT ["python", "-m", "videoflow.worker"]
