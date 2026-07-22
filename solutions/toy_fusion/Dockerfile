# videoflow :: solutions/toy_fusion — REALTIME time-aligned fusion — CPU.
#
# A minimal solution image: no dependencies beyond videoflow-base itself. The
# graph wires the simulated sources and fusion nodes in toy_fusion_nodes.py,
# which workers reconstruct as `toy_fusion_nodes.<Class>`. Python 3.12.
#
# Build from the videoflow repo ROOT (for consistency with the other solutions;
# nothing outside this directory is copied):
#   docker build -f solutions/toy_fusion/Dockerfile -t videoflow-toy-fusion:latest .
#
# Normally you don't build this by hand — `videoflow deploy toy_fusion.py`
# builds it, loads it into the cluster, and runs the flow. See README.md.
ARG BASE_IMAGE=videoflow-base:py3.12
FROM ${BASE_IMAGE}

WORKDIR /app

# No dependency install: requirements.txt is deliberately empty (see the comment
# in it) — the base image already carries everything these nodes need.

# The solution modules: the graph, the importable glue nodes the worker
# reconstructs, and the config loader. There is no prepare.py — this solution
# needs nothing prepared.
COPY solutions/toy_fusion/toy_fusion.py \
     solutions/toy_fusion/toy_fusion_nodes.py \
     solutions/toy_fusion/common.py ./

# ENTRYPOINT ["python", "-m", "videoflow.worker"] is inherited from videoflow-base.
