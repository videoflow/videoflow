#!/usr/bin/env bash
#
# Verify and prepare the shared k3s cluster for tests/integration/k8s/ (and the
# kubernetes-level conformance cases).
#
#   ./scripts/k3s-test-up.sh             # dev broker + PVC + images in $VF_K8S_NAMESPACE
#   ./scripts/k3s-test-up.sh --durable   # additionally the durable broker profile in ${VF_K8S_NAMESPACE}-ha
#   uv run pytest tests/integration/k8s -q -rs
#
# The k3s counterpart of scripts/kind-up.sh, which remains the CI path. The one
# difference that matters: kind-up.sh CREATES a cluster, this script never does.
# The k3s cluster is shared infrastructure other people run GPU jobs on, so every
# step here either verifies something or creates a *namespaced* object labelled
# app.kubernetes.io/managed-by=videoflow — all of it removable with
# `kubectl delete ns $VF_K8S_NAMESPACE` (and `${VF_K8S_NAMESPACE}-ha`). It never:
#
#   - switches the kubectl context: it checks the current one and stops if it is
#     not $VF_K8S_CONTEXT, because a silent retarget would push test workloads
#     into whatever cluster you were actually pointed at;
#   - labels a node, changes the GPU Operator ClusterPolicy, or enables MIG;
#   - installs anything cluster-wide (no CRDs, operators, PriorityClasses,
#     StorageClasses — it requires the ones it needs to exist already);
#   - restarts docker or edits its daemon config (images go through crane);
#   - creates a pod that requests a GPU.
#
# The pods it does create (the broker, one probe, and one chmod of the claim's
# own directory when the provisioner made it root-only) carry
# priorityClassName: $VF_K8S_PRIORITY_CLASS (cluster-batch), so they yield to
# training work rather than preempt it.
#
# What it leaves behind:
#   - namespace $VF_K8S_NAMESPACE with the RWX claim $VF_K8S_PVC on nfs-shared
#     (k8s/test-pvc.yaml), NATS + Redis from videoflow's own dev manifests, and
#     the NodePort publishing NATS on every node's :$VF_K8S_NATS_NODEPORT
#   - with --durable: namespace ${VF_K8S_NAMESPACE}-ha with the durable profile
#     (a 3-pod NATS StatefulSet on local-path claims, an append-only Redis)
#   - videoflow-base:py3.12, the k8s fixture image and one image per toy solution
#     in the registry $VF_K8S_IMAGE_REGISTRY, tagged locally under that prefix
#   - nothing on the node filesystems: the work root the tests use is the claim's
#     backing directory under the NFS export, which this host serves
#
# The tests gate themselves on all of that and skip with the reason when it is
# missing, so a partial run here shows up as a skip rather than a failure.
set -euo pipefail

cd "$(dirname "$0")/.."

VF_K8S_CONTEXT="${VF_K8S_CONTEXT:-default}"
VF_K8S_NAMESPACE="${VF_K8S_NAMESPACE:-videoflow-test}"
VF_K8S_PVC="${VF_K8S_PVC:-vf-test-share}"
VF_K8S_IMAGE_REGISTRY="${VF_K8S_IMAGE_REGISTRY:-10.128.81.10:5000}"
VF_K8S_NATS_NODEPORT="${VF_K8S_NATS_NODEPORT:-30422}"
VF_K8S_PRIORITY_CLASS="${VF_K8S_PRIORITY_CLASS:-cluster-batch}"
VF_K8S_STORAGE_CLASS="${VF_K8S_STORAGE_CLASS:-nfs-shared}"
HA_NAMESPACE="${VF_K8S_NAMESPACE}-ha"
VF_K8S_HA_NATS_NODEPORT="${VF_K8S_HA_NATS_NODEPORT:-30423}"
SOLUTIONS='toy_calculator toy_router toy_recovery toy_fusion'
BASE_IMAGE='videoflow-base:py3.12'
FIXTURE_IMAGE='videoflow-k8s-fixtures:latest'
MANAGED_LABEL='app.kubernetes.io/managed-by=videoflow'

durable=false
for arg in "$@"; do
    case "$arg" in
        --durable) durable=true ;;
        *) echo "usage: $0 [--durable]" >&2; exit 2 ;;
    esac
done

die() { echo "error: $*" >&2; exit 1; }

for binary in docker kubectl uv curl; do
    command -v "$binary" >/dev/null 2>&1 || die "$binary is not on PATH."
done

# --- verify: kubeconfig, context, flavor, cluster prerequisites ----------------
kubeconfig="${KUBECONFIG:-$HOME/.kube/config}"
[ -r "${kubeconfig%%:*}" ] || die "no readable kubeconfig at ${kubeconfig} (set KUBECONFIG or install ~/.kube/config)."

current="$(kubectl config current-context 2>/dev/null || true)"
[ -n "$current" ] || die "kubectl has no current context."
if [ "$current" != "$VF_K8S_CONTEXT" ]; then
    die "current kubectl context is '$current', expected '$VF_K8S_CONTEXT'.
  Not switched for you — which cluster kubectl points at is your call. Either
  'kubectl config use-context $VF_K8S_CONTEXT' yourself, or, if '$current' is the
  cluster you mean, export VF_K8S_CONTEXT=$current and re-run."
fi
kubectl cluster-info --request-timeout=10s >/dev/null 2>&1 || die "context '$current' is not reachable."

flavor="$(uv run python -c 'from videoflow.deploy.cluster import detect_cluster; print(detect_cluster())')"
[ "$flavor" = "k3s" ] || die "the cluster at '$current' is '$flavor', not k3s. For kind use ./scripts/kind-up.sh."

kubectl get storageclass "$VF_K8S_STORAGE_CLASS" >/dev/null 2>&1 \
    || die "StorageClass '$VF_K8S_STORAGE_CLASS' does not exist (kubectl get sc); the work root needs an RWX class. This script installs nothing cluster-wide."
kubectl get priorityclass "$VF_K8S_PRIORITY_CLASS" >/dev/null 2>&1 \
    || die "PriorityClass '$VF_K8S_PRIORITY_CLASS' does not exist (kubectl get priorityclasses); set VF_K8S_PRIORITY_CLASS to one that does. This script installs nothing cluster-wide."
echo "==> context '$current' is a k3s cluster with StorageClass $VF_K8S_STORAGE_CLASS and PriorityClass $VF_K8S_PRIORITY_CLASS"

# --- namespace ----------------------------------------------------------------
ensure_namespace() {
    echo "==> namespace $1"
    kubectl apply -f - <<EOF
apiVersion: v1
kind: Namespace
metadata:
  name: $1
  labels:
    ${MANAGED_LABEL%%=*}: ${MANAGED_LABEL#*=}
EOF
}
ensure_namespace "$VF_K8S_NAMESPACE"

# --- the shared work root: an RWX claim ------------------------------------------
echo "==> claim $VF_K8S_PVC ($VF_K8S_STORAGE_CLASS, ReadWriteMany)"
sed -e "s/name: vf-test-share/name: $VF_K8S_PVC/" \
    -e "s/storageClassName: nfs-shared/storageClassName: $VF_K8S_STORAGE_CLASS/" k8s/test-pvc.yaml \
    | kubectl apply -n "$VF_K8S_NAMESPACE" -f -
# nfs-shared binds immediately; a class with WaitForFirstConsumer would sit
# Pending here until a pod used it — the probe below is that pod, so give it a
# short wait and let the probe report a still-unbound claim.
kubectl wait -n "$VF_K8S_NAMESPACE" "pvc/$VF_K8S_PVC" --for=jsonpath='{.status.phase}'=Bound --timeout=60s \
    || echo "    (not Bound yet — the round-trip probe below will say so if it stays that way)"

# --- images: build locally, push with crane -----------------------------------
# Same builds as scripts/kind-up.sh (not docker/build-images.sh, which also
# builds the CUDA base: gigabytes the CPU-only tests have no use for). Always
# rebuild rather than skip on an existing tag — a tag says nothing about which
# source it was built from, and docker's layer cache makes a no-change rebuild
# cheap. `kind load` is replaced by scripts/push-images.sh: the nodes pull from
# the registry, so nothing is side-loaded and nothing needs sudo.
# Extra `docker build` arguments for every image, e.g. the corporate proxy a
# build container needs to reach apt/PyPI mirrors when the host itself goes
# through one (docker forwards http_proxy/https_proxy build args into RUN
# steps): VF_DOCKER_BUILD_ARGS='--build-arg http_proxy=http://proxy:3128 --build-arg https_proxy=http://proxy:3128'.
# A per-build flag, never a docker daemon change.
# shellcheck disable=SC2206
build_args=(${VF_DOCKER_BUILD_ARGS:-})
echo "==> building $BASE_IMAGE"
docker build "${build_args[@]}" -f docker/base/Dockerfile --build-arg PYTHON_VERSION=3.12 -t "$BASE_IMAGE" .
echo "==> building $FIXTURE_IMAGE"
docker build "${build_args[@]}" -f tests/integration/k8s/Dockerfile --build-arg "BASE_IMAGE=${BASE_IMAGE}" \
             -t "$FIXTURE_IMAGE" .
images="$BASE_IMAGE $FIXTURE_IMAGE"
for solution in $SOLUTIONS; do
    # videoflow-<dirname>:latest, underscores and all — deploy.build.default_tag
    # computes the same string, so a later autobuild agrees with what is pushed.
    tag="videoflow-${solution}:latest"
    echo "==> building $tag"
    docker build "${build_args[@]}" -f "solutions/${solution}/Dockerfile" \
                 --build-arg "BASE_IMAGE=${BASE_IMAGE}" -t "$tag" .
    images="$images $tag"
done
echo "==> pushing images to $VF_K8S_IMAGE_REGISTRY"
# shellcheck disable=SC2086
VF_K8S_IMAGE_REGISTRY="$VF_K8S_IMAGE_REGISTRY" ./scripts/push-images.sh $images

# --- broker -------------------------------------------------------------------
# Through videoflow's own manifests rather than a fork of k8s/nats.yaml, so the
# tests run against exactly what `videoflow deploy` would have installed.
# Pre-creating it also means each deploy's ensure_infra finds the Services
# already there, reports nothing created and owns nothing — so no test tears the
# broker out from under the next one.
install_broker() {   # namespace profile
    echo "==> broker in $1 ($2 profile, priorityClassName $VF_K8S_PRIORITY_CLASS)"
    uv run python - "$1" "$2" "$VF_K8S_PRIORITY_CLASS" <<'PY'
import sys
from videoflow.deploy.broker_profiles import broker_profiles
from videoflow.deploy.infra import ensure_infra, infra_urls, wait_infra_ready

namespace, name, priority = sys.argv[1:4]
profile, redis_profile = broker_profiles(name, priority_class = priority)
urls, created = ensure_infra('kubectl', namespace, need_redis = True,
                             profile = profile, redis_profile = redis_profile)
wait_infra_ready('kubectl', namespace, created, timeout_secs = 300, profile = profile)
print('   created:', ', '.join(created) if created else '(already present)')
print('   in-cluster URLs:', infra_urls(namespace))
PY
}
install_broker "$VF_K8S_NAMESPACE" dev
if [ "$durable" = true ]; then
    ensure_namespace "$HA_NAMESPACE"
    install_broker "$HA_NAMESPACE" durable
    # Host access to the durable broker too (the conformance durability cases
    # kill its pods and must keep talking to the survivors: a NodePort spreads
    # reconnects over every pod, a port-forward dies with the pod it chose).
    sed "s/nodePort: 30422/nodePort: $VF_K8S_HA_NATS_NODEPORT/" k8s/nats-nodeport.yaml \
        | kubectl apply -n "$HA_NAMESPACE" -f -
fi

# --- host access to NATS ------------------------------------------------------
echo "==> host access to NATS (NodePort $VF_K8S_NATS_NODEPORT)"
sed "s/nodePort: 30422/nodePort: $VF_K8S_NATS_NODEPORT/" k8s/nats-nodeport.yaml \
    | kubectl apply -n "$VF_K8S_NAMESPACE" -f -
nats_url="nats://127.0.0.1:${VF_K8S_NATS_NODEPORT}"
for _ in 1 2 3 4 5 6 7 8 9 10; do
    if (exec 3<>"/dev/tcp/127.0.0.1/${VF_K8S_NATS_NODEPORT}") 2>/dev/null; then break; fi
    sleep 1
done
if (exec 3<>"/dev/tcp/127.0.0.1/${VF_K8S_NATS_NODEPORT}") 2>/dev/null; then
    echo "    listening at $nats_url"
else
    echo "    WARNING: nothing answers at $nats_url on this host — it is not a cluster node, or the" >&2
    echo "    NodePort is filtered. Fallback (keep it running while the tests run):" >&2
    echo "      kubectl -n $VF_K8S_NAMESPACE port-forward svc/nats ${VF_K8S_NATS_NODEPORT}:4222" >&2
fi

# --- the check that actually matters: the claim round-trips ----------------------
# The work root that does not round-trip is the one misconfiguration that fails
# much later and far from its cause: the flow runs, every pod exits 0, and the
# test fails with "report.json was not written". Prove it now: write a sentinel
# on the claim's backing directory on this host, read it back from a pod that
# mounts the claim — on a node other than this one where possible, because a pod
# on the NFS server itself would see the directory even if the export were
# unreachable over the network.
pv="$(kubectl get pvc -n "$VF_K8S_NAMESPACE" "$VF_K8S_PVC" -o jsonpath='{.spec.volumeName}')"
[ -n "$pv" ] || die "claim $VF_K8S_PVC is not Bound (kubectl describe pvc -n $VF_K8S_NAMESPACE $VF_K8S_PVC)."
share="$(kubectl get pv "$pv" -o jsonpath='{.spec.csi.volumeAttributes.share}')"
subdir="$(kubectl get pv "$pv" -o jsonpath='{.spec.csi.volumeAttributes.subdir}')"
server="$(kubectl get pv "$pv" -o jsonpath='{.spec.csi.volumeAttributes.server}')"
[ -n "$share" ] && [ -n "$subdir" ] || die "PersistentVolume $pv carries no csi.volumeAttributes.share/subdir — is $VF_K8S_STORAGE_CLASS the NFS class?"
work_root="${share%/}/${subdir}"
[ -d "$work_root" ] || die "$work_root does not exist on this host. The claim is served by $server; the tests
  read their artifacts straight from that directory, so run them on the NFS server
  (or mount its export at the same path here)."
if [ ! -w "$work_root" ]; then
    # csi-driver-nfs creates the claim's directory root:root 0755. The tests stage
    # solutions there as this user, so open it once from a pod that mounts the
    # claim — root inside the pod, and the export is no_root_squash. This touches
    # our own claim's directory and nothing else.
    echo "==> $work_root is not writable by $(id -un); opening it from a pod that mounts the claim"
    fixer="vf-pvc-chmod-$$"
    fix_image="${VF_K8S_IMAGE_REGISTRY}/${BASE_IMAGE}"
    fix_overrides="{\"apiVersion\":\"v1\",\"spec\":{\"priorityClassName\":\"$VF_K8S_PRIORITY_CLASS\",\"restartPolicy\":\"Never\",\"containers\":[{\"name\":\"$fixer\",\"image\":\"$fix_image\",\"imagePullPolicy\":\"Always\",\"command\":[\"chmod\",\"0777\",\"/share\"],\"resources\":{\"requests\":{\"cpu\":\"100m\",\"memory\":\"64Mi\"},\"limits\":{\"cpu\":\"500m\",\"memory\":\"256Mi\"}},\"volumeMounts\":[{\"name\":\"share\",\"mountPath\":\"/share\"}]}],\"volumes\":[{\"name\":\"share\",\"persistentVolumeClaim\":{\"claimName\":\"$VF_K8S_PVC\"}}]}}"
    kubectl run "$fixer" -n "$VF_K8S_NAMESPACE" --rm -i --restart=Never --quiet --pod-running-timeout=3m \
        --labels="$MANAGED_LABEL" --image="$fix_image" --overrides="$fix_overrides" >/dev/null 2>&1 || true
fi
[ -w "$work_root" ] || die "$work_root is not writable by $(id -un) on this host, even after chmod from a pod (kubectl get events -n $VF_K8S_NAMESPACE)."

echo "==> verifying the claim round-trips (host -> pod)"
sentinel=".vf-probe-$$"
echo "vf-$$" > "$work_root/$sentinel"
probe="vf-pvc-probe-$$"
probe_image="${VF_K8S_IMAGE_REGISTRY}/${BASE_IMAGE}"
affinity=''
this_node="$(hostname -s)"
if kubectl get node "$this_node" >/dev/null 2>&1; then
    affinity="\"affinity\":{\"nodeAffinity\":{\"requiredDuringSchedulingIgnoredDuringExecution\":{\"nodeSelectorTerms\":[{\"matchExpressions\":[{\"key\":\"kubernetes.io/hostname\",\"operator\":\"NotIn\",\"values\":[\"$this_node\"]}]}]}}},"
fi
# --overrides replaces the generated pod; the container keeps the pod's name so
# the merge edits it rather than adding a second one. imagePullPolicy Always so
# the probe also proves the nodes can pull from the registry. No GPU, small
# requests, cluster-batch priority.
overrides="{\"apiVersion\":\"v1\",\"spec\":{${affinity}\"priorityClassName\":\"$VF_K8S_PRIORITY_CLASS\",\"restartPolicy\":\"Never\",\"containers\":[{\"name\":\"$probe\",\"image\":\"$probe_image\",\"imagePullPolicy\":\"Always\",\"command\":[\"cat\",\"/share/$sentinel\"],\"resources\":{\"requests\":{\"cpu\":\"100m\",\"memory\":\"64Mi\"},\"limits\":{\"cpu\":\"500m\",\"memory\":\"256Mi\"}},\"volumeMounts\":[{\"name\":\"share\",\"mountPath\":\"/share\"}]}],\"volumes\":[{\"name\":\"share\",\"persistentVolumeClaim\":{\"claimName\":\"$VF_K8S_PVC\"}}]}}"
seen="$(kubectl run "$probe" -n "$VF_K8S_NAMESPACE" --rm -i --restart=Never --quiet \
            --pod-running-timeout=3m --labels="$MANAGED_LABEL" \
            --image="$probe_image" --overrides="$overrides" 2>&1 || true)"
rm -f "$work_root/$sentinel"
if ! grep -qx "vf-$$" <<<"$seen"; then
    echo >&2
    echo "error: a pod mounting $VF_K8S_PVC did not see the file written to $work_root." >&2
    echo "  Solution work dirs are staged under that directory and reached in the pods" >&2
    echo "  through --mount-pvc $VF_K8S_PVC:$work_root, so without this round trip every" >&2
    echo "  artifact is written somewhere the test cannot read. The probe said:" >&2
    echo "$seen" | sed 's/^/    /' >&2
    echo "  Check: kubectl describe pv $pv; kubectl get events -n $VF_K8S_NAMESPACE --sort-by=.lastTimestamp | tail" >&2
    exit 1
fi
echo "    ok: $work_root <-> pvc/$VF_K8S_PVC"

cat <<EOF

Ready. The test bucket reads these (all defaulted or derived — export only what
you changed; VF_K8S_WORK_ROOT is derived from the claim when unset):

  export VF_K8S_CONTEXT=$VF_K8S_CONTEXT
  export VF_K8S_NAMESPACE=$VF_K8S_NAMESPACE
  export VF_K8S_PVC=$VF_K8S_PVC
  export VF_K8S_IMAGE_REGISTRY=$VF_K8S_IMAGE_REGISTRY
  export VF_K8S_NATS_URL=$nats_url
  export VF_K8S_WORK_ROOT=$work_root
EOF
if [ "$durable" = true ]; then
    echo "  export VF_K8S_HA_NAMESPACE=$HA_NAMESPACE"
    echo "  export VF_K8S_HA_NATS_URL=nats://127.0.0.1:${VF_K8S_HA_NATS_NODEPORT}"
fi
cat <<EOF

  uv run pytest tests/integration/k8s -q -rs

Remove everything this script created with:
  kubectl delete ns $VF_K8S_NAMESPACE$([ "$durable" = true ] && echo " $HA_NAMESPACE")
EOF
