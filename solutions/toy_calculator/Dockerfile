# videoflow :: solutions/toy_calculator — BATCH diamond over integers — CPU.
#
# The smallest solution image: no dependencies beyond videoflow-base itself. The
# graph wires core nodes to the glue nodes in toy_calculator_nodes.py, which
# workers reconstruct as `toy_calculator_nodes.<Class>`. Python 3.12.
#
# Build from the videoflow repo ROOT (for consistency with the other solutions;
# nothing outside this directory is copied):
#   docker build -f solutions/toy_calculator/Dockerfile -t videoflow-toy-calculator:latest .
#
# Normally you don't build this by hand — `videoflow deploy toy_calculator.py`
# builds it, loads it into the cluster, and runs the flow. See README.md.
ARG BASE_IMAGE=videoflow-base:py3.12
FROM ${BASE_IMAGE}

WORKDIR /app

# No dependency install: requirements.txt is deliberately empty (see the comment
# in it) — the base image already carries everything these nodes need.

# The solution modules: the graph, the importable glue nodes the worker
# reconstructs, the config loader, and the prep hook deploy runs.
COPY solutions/toy_calculator/toy_calculator.py \
     solutions/toy_calculator/toy_calculator_nodes.py \
     solutions/toy_calculator/common.py \
     solutions/toy_calculator/prepare.py ./

# ENTRYPOINT ["python", "-m", "videoflow.worker"] is inherited from videoflow-base.
