# The videoflow base image: the framework, the broker client, the wire format, and
# every dependency the *built-in* nodes need (OpenCV, ffmpeg, Redis, PyYAML). It can
# run any pipeline made of shipped videoflow nodes on its own.
#
# For your own nodes, build your own image FROM this one — add your dependencies and
# copy in your package (see docker/user-image.example.Dockerfile) — then deploy with
#   videoflow deploy my_flow.py --image <your-image>
#
# Build from the repo root:
#   docker build -f docker/base/Dockerfile -t videoflow-base:latest .
FROM python:3.9-slim

ENV PYTHONUNBUFFERED=1

WORKDIR /app

# System libraries the built-in video/vision nodes need (ffmpeg + OpenCV's runtime).
RUN apt-get update && apt-get install -y --no-install-recommends \
        ffmpeg libglib2.0-0 libgl1 && \
    rm -rf /var/lib/apt/lists/*

# Install uv, then use it to install videoflow (with all built-in node deps) into
# the system environment.
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

COPY pyproject.toml README.md ./
COPY videoflow ./videoflow

RUN uv pip install --system --no-cache ".[all]"

ENTRYPOINT ["python", "-m", "videoflow.worker"]
