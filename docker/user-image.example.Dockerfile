# Example: package YOUR videoflow nodes into a deployable image.
#
# You define processors/producers/consumers in your own Python package, `pip install
# videoflow`, and build an image FROM videoflow-base that adds your dependencies and
# your code. Then point a deploy at it:
#
#   docker build -f docker/user-image.example.Dockerfile -t ghcr.io/me/app:v1 .
#   videoflow deploy my_flow.py:build_flow --nats nats://... --image ghcr.io/me/app:v1
#
# A node that needs a different environment (e.g. a GPU model) can declare its own
# image in the graph — MyDetector(name='det', image='ghcr.io/me/gpu:v1') — or be
# overridden at deploy time with --image-override det=ghcr.io/me/gpu:v1.

FROM videoflow-base:latest

WORKDIR /app

# 1. Your extra dependencies (torch, your libraries, ...).
COPY requirements.txt ./
RUN uv pip install --system --no-cache -r requirements.txt

# 2. Your package containing your node classes. It must be importable by its module
#    path (the same path that appears in VF_NODE_CLASS), so install it, don't just copy.
COPY . ./
RUN uv pip install --system --no-cache .

# The entrypoint is inherited from videoflow-base (python -m videoflow.worker); the
# worker imports and runs whichever node the pod is assigned.
