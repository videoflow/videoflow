#!/usr/bin/env bash
#
# Stand up the kind cluster tests/integration/k8s/ runs against.
#
#   ./scripts/kind-up.sh
#   uv run pytest tests/integration/k8s -q -rs
#   ./scripts/kind-down.sh
#
# Idempotent and re-runnable: every step checks for what it is about to create, so
# this is also the right thing to run after `kind-down.sh` deleted only the cluster,
# or in CI where the cluster and the base image already exist. It never touches the
# kubectl context of a cluster it did not create.
#
# What it leaves behind:
#   - a kind cluster named $VF_KIND_CLUSTER (context kind-$VF_KIND_CLUSTER)
#   - videoflow-base:py3.12 plus one image per toy solution, side-loaded into it
#   - namespace $VF_K8S_NAMESPACE holding NATS + Redis and the NodePort that
#     publishes NATS on 127.0.0.1:4223
#   - $VF_K8S_WORK_ROOT, bind-mounted into the node at the same path
#
# The tests gate themselves on all of that and skip with the reason when it is
# missing, so a partial run here shows up as a skip rather than a failure.
set -euo pipefail

cd "$(dirname "$0")/.."

VF_KIND_CLUSTER="${VF_KIND_CLUSTER:-videoflow}"
VF_K8S_NAMESPACE="${VF_K8S_NAMESPACE:-videoflow-test}"
VF_K8S_WORK_ROOT="${VF_K8S_WORK_ROOT:-/tmp/videoflow-k8s}"
DEFAULT_WORK_ROOT='/tmp/videoflow-k8s'
SOLUTIONS='toy_calculator toy_router toy_recovery toy_fusion'
BASE_IMAGE='videoflow-base:py3.12'
FIXTURE_IMAGE='videoflow-k8s-fixtures:latest'
NODE_NAME="${VF_KIND_CLUSTER}-control-plane"

for binary in docker kind kubectl; do
    command -v "$binary" >/dev/null 2>&1 || {
        echo "error: $binary is not on PATH." >&2
        echo "  kind:    https://kind.sigs.k8s.io/docs/user/quick-start/#installation" >&2
        exit 1
    }
done

# --- the work root, before the cluster --------------------------------------
# kind binds extraMounts once, when the node container is created. Creating the
# directory afterwards leaves the node with an empty mount that silently swallows
# everything the pods write.
echo "==> work root: $VF_K8S_WORK_ROOT"
mkdir -p "$VF_K8S_WORK_ROOT"

# --- cluster ----------------------------------------------------------------
if kind get clusters 2>/dev/null | grep -qx "$VF_KIND_CLUSTER"; then
    echo "==> cluster '$VF_KIND_CLUSTER' already exists"
else
    echo "==> creating cluster '$VF_KIND_CLUSTER'"
    # kind config files do not interpolate environment variables, so rewrite the
    # work root in place when the caller overrode it.
    sed "s#${DEFAULT_WORK_ROOT}#${VF_K8S_WORK_ROOT}#g" k8s/kind-cluster.yaml \
        | kind create cluster --name "$VF_KIND_CLUSTER" --config -
fi

# --- images -----------------------------------------------------------------
# Not docker/build-images.sh: that also builds the CUDA base, several gigabytes
# this CPU-only cluster has no use for (and kind has no GPU passthrough anyway).
#
# Always build, never "skip if the tag exists". A tag says nothing about which
# source it was built from, and reusing a stale videoflow-base is a genuinely
# confusing failure: every pod starts, imports a videoflow from weeks ago, and dies
# with a ModuleNotFoundError for a module that plainly exists in your checkout.
# Docker's layer cache already makes the no-change case cheap — `COPY videoflow`
# only invalidates when the tree actually changed.
echo "==> building $BASE_IMAGE"
docker build -f docker/base/Dockerfile --build-arg PYTHON_VERSION=3.12 -t "$BASE_IMAGE" .

# The nodes tests/integration/k8s/test_k8s_engine.py runs. Baked into an image
# rather than mounted, because that is how a worker is actually supposed to reach
# a class the framework does not ship — and because the base image's WORKDIR /app
# holds a copy of the source tree, so a hostPath over it would shadow that.
echo "==> building $FIXTURE_IMAGE"
docker build -f tests/integration/k8s/Dockerfile --build-arg "BASE_IMAGE=${BASE_IMAGE}" \
             -t "$FIXTURE_IMAGE" .

images="$BASE_IMAGE $FIXTURE_IMAGE"
for solution in $SOLUTIONS; do
    # videoflow-<dirname>:latest, underscores and all — deploy.build.default_tag
    # computes the same string, so a later autobuild agrees with what is loaded here.
    tag="videoflow-${solution}:latest"
    echo "==> building $tag"
    docker build -f "solutions/${solution}/Dockerfile" \
                 --build-arg "BASE_IMAGE=${BASE_IMAGE}" -t "$tag" .
    images="$images $tag"
done

# One call: kind compares image IDs against the node and skips what is already
# there, so re-running this is cheap. Pre-loading also makes each deploy's own
# `kind load` step nearly free instead of re-saving a gigabyte per test.
echo "==> loading images into the cluster"
# shellcheck disable=SC2086
kind load docker-image $images --name "$VF_KIND_CLUSTER"

# --- namespace, broker, host access -----------------------------------------
echo "==> namespace $VF_K8S_NAMESPACE"
kubectl create namespace "$VF_K8S_NAMESPACE" --dry-run=client -o yaml | kubectl apply -f -

# Install the broker through videoflow's own manifests rather than a fork of
# k8s/nats.yaml, so the tests run against exactly what `videoflow deploy` would
# have installed. Pre-creating it also means each deploy's ensure_infra finds the
# Services already there, reports nothing created, and owns nothing — so no test
# tears the broker out from under the next one.
echo "==> broker (NATS + Redis)"
uv run python - "$VF_K8S_NAMESPACE" <<'PY'
import sys
from videoflow.deploy.infra import ensure_infra, infra_urls, wait_infra_ready

namespace = sys.argv[1]
urls, created = ensure_infra('kubectl', namespace, need_redis = True)
wait_infra_ready('kubectl', namespace, created)
print('   created:', ', '.join(created) if created else '(already present)')
print('   in-cluster URLs:', infra_urls(namespace))
PY

echo "==> host access to NATS (NodePort 30422 -> 127.0.0.1:4223)"
kubectl apply -n "$VF_K8S_NAMESPACE" -f k8s/nats-nodeport.yaml

# --- the check that actually matters ----------------------------------------
# A work root that does not round-trip is the one misconfiguration that produces a
# confusing failure much later: the flow runs, every pod exits 0, and the test fails
# with "report.json was not written". Prove the mapping now instead.
echo "==> verifying the work root round-trips into the node"
sentinel="$VF_K8S_WORK_ROOT/.vf-probe"
echo "vf-$$" > "$sentinel"
if ! docker exec "$NODE_NAME" cat "$sentinel" 2>/dev/null | grep -qx "vf-$$"; then
    echo >&2
    echo "error: $VF_K8S_WORK_ROOT is not visible inside the kind node at the same path." >&2
    echo "  Solution work dirs are hostPath-mounted into the worker pods at the absolute" >&2
    echo "  path baked in at compile time, so without an identity mapping every artifact" >&2
    echo "  is written somewhere the test cannot read." >&2
    echo "  extraMounts bind at node creation: if you changed VF_K8S_WORK_ROOT, recreate" >&2
    echo "  the cluster with ./scripts/kind-down.sh && ./scripts/kind-up.sh" >&2
    exit 1
fi
rm -f "$sentinel"

cat <<EOF

Ready. The test bucket reads these (all defaulted, export only what you changed):

  export VF_KIND_CLUSTER=$VF_KIND_CLUSTER
  export VF_K8S_NAMESPACE=$VF_K8S_NAMESPACE
  export VF_K8S_WORK_ROOT=$VF_K8S_WORK_ROOT
  export VF_K8S_NATS_URL=nats://127.0.0.1:4223

  uv run pytest tests/integration/k8s -q -rs
EOF
