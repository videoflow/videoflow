# Image for computer-vision processors (videoflow/processors/vision/*): detectors,
# trackers, pose, segmentation, annotators, transformers. Adds OpenCV (headless)
# plus room for a DL framework. Concrete detector/pose/segmentation backends live
# in external contrib packages and pull their own torch/tensorflow — add the
# specific framework your backends need here, e.g.:
#   RUN pip install torch torchvision --index-url https://download.pytorch.org/whl/cpu
#
#   docker build -f docker/base/Dockerfile   -t videoflow-base:latest .
#   docker build -f docker/vision/Dockerfile -t videoflow-vision:latest .
ARG BASE_IMAGE=videoflow-base:latest
FROM ${BASE_IMAGE}

RUN apt-get update && apt-get install -y --no-install-recommends \
        libglib2.0-0 libgl1 && \
    rm -rf /var/lib/apt/lists/*

RUN uv pip install --system --no-cache ".[vision,blob]"
