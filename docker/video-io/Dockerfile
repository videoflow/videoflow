# Image for video I/O nodes (videoflow/producers/video.py, videoflow/consumers/video.py):
# VideoFileReader, VideoUrlReader, VideofileWriter, etc. Adds ffmpeg + OpenCV's
# video backend dependencies.
#
#   docker build -f docker/base/Dockerfile     -t videoflow-base:latest .
#   docker build -f docker/video-io/Dockerfile -t videoflow-video-io:latest .
ARG BASE_IMAGE=videoflow-base:latest
FROM ${BASE_IMAGE}

RUN apt-get update && apt-get install -y --no-install-recommends \
        ffmpeg libglib2.0-0 libgl1 && \
    rm -rf /var/lib/apt/lists/*

RUN uv pip install --system --no-cache ".[video,blob]"
