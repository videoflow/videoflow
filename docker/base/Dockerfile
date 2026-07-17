# Base image for every videoflow worker. Contains the framework, the broker
# client, and the wire format — enough to run producer/processor/consumer nodes
# that have no heavy extra dependencies. Family images (basic, vision, video-io)
# extend this and add their own deps; the ENTRYPOINT is identical everywhere.
#
# Build from the repo root:
#   docker build -f docker/base/Dockerfile -t videoflow-base:latest .
FROM python:3.9-slim

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

COPY setup.py README.md ./
COPY videoflow ./videoflow

RUN pip install --upgrade pip && \
    pip install ".[distributed]"

ENTRYPOINT ["python", "-m", "videoflow.worker"]
