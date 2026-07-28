# The worker image for tests/integration/k8s/test_k8s_engine.py.
#
# Those tests build flows out of built-in nodes plus fixture_nodes.py, and a worker
# rebuilds its node from `<module>.<Class>` — so `fixture_nodes` has to be importable
# inside the pod. Baking it into an image is how the framework says to do that, and
# it is what every solution image does; the alternative (hostPath-mounting the module
# over the base image's WORKDIR) also works but shadows the source tree that lives
# there, which turns any later import problem into a puzzle.
#
# Built and side-loaded by scripts/kind-up.sh. Build from the repo ROOT:
#   docker build -f tests/integration/k8s/Dockerfile -t videoflow-k8s-fixtures:latest .
ARG BASE_IMAGE=videoflow-base:py3.12
FROM ${BASE_IMAGE}

WORKDIR /app

COPY tests/integration/k8s/fixture_nodes.py ./

# ENTRYPOINT ["python", "-m", "videoflow.worker"] is inherited from videoflow-base.
