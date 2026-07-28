#!/usr/bin/env bash
#
# Delete the kind cluster scripts/kind-up.sh created.
#
#   ./scripts/kind-down.sh            # cluster only
#   ./scripts/kind-down.sh --purge    # cluster and the work root
#
# The work root survives by default: it holds the artifacts of the last run, which
# are the first thing you want when a test failed. --purge when you are done with
# them, or before changing VF_K8S_WORK_ROOT (extraMounts bind at node creation, so
# a new work root needs a new cluster anyway).
#
# Built images are left alone — they are the expensive part of kind-up.sh and
# nothing else on the machine is called videoflow-*.
set -euo pipefail

VF_KIND_CLUSTER="${VF_KIND_CLUSTER:-videoflow}"
VF_K8S_WORK_ROOT="${VF_K8S_WORK_ROOT:-/tmp/videoflow-k8s}"

purge=false
for arg in "$@"; do
    case "$arg" in
        --purge) purge=true ;;
        *) echo "usage: $0 [--purge]" >&2; exit 2 ;;
    esac
done

command -v kind >/dev/null 2>&1 || { echo "error: kind is not on PATH." >&2; exit 1; }

if kind get clusters 2>/dev/null | grep -qx "$VF_KIND_CLUSTER"; then
    echo "==> deleting cluster '$VF_KIND_CLUSTER'"
    kind delete cluster --name "$VF_KIND_CLUSTER"
else
    echo "==> cluster '$VF_KIND_CLUSTER' is not running"
fi

if [ "$purge" = true ]; then
    echo "==> removing $VF_K8S_WORK_ROOT"
    rm -rf "$VF_K8S_WORK_ROOT"
else
    echo "==> keeping $VF_K8S_WORK_ROOT (pass --purge to remove it)"
fi
