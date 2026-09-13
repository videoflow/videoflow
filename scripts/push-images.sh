#!/usr/bin/env bash
#
# Push locally built images to the cluster's registry with crane — from user
# space, without a docker daemon that trusts the registry.
#
#   ./scripts/push-images.sh videoflow-base:py3.12 videoflow-toy_calculator:latest ...
#
# Why not `docker push`: the registry (10.128.81.10:5000 by default) speaks plain
# HTTP, and the docker daemon on the cluster nodes does not list it under
# insecure-registries. Teaching it to means editing daemon.json and restarting
# docker — which, with live-restore off, kills every container on the host,
# including other people's. crane talks to the registry itself (`--insecure` =
# "without TLS", a global crane flag) and reads the image from a `docker save`
# tarball, so nothing about the daemon changes. Nothing here needs sudo.
#
# Per image NAME:TAG:
#   docker save NAME:TAG -o <tmp>/NAME-TAG.tar      a file, not a pipe — `crane push`
#                                                   takes a tarball PATH, not stdin
#   crane push --insecure <tmp>/... REGISTRY/NAME:TAG
#   crane manifest --insecure REGISTRY/NAME:TAG     read back: pushed is not the same as present
#   docker tag NAME:TAG REGISTRY/NAME:TAG
#
# The local re-tag is deliberate. `videoflow deploy` runs a solution's prepare.py
# with `docker run <the ref the pods use>`; with the registry-qualified ref
# present locally docker uses it as-is instead of trying (and failing) to pull it
# from a registry it does not trust. It also means the ref exists locally, so
# `deploy` will offer to side-load it into the cluster — videoflow.deploy.cluster
# must skip registry-qualified refs on k3s (the nodes pull them), or that
# side-load turns into a `sudo k3s ctr images import` prompt.
#
# Environment:
#   VF_K8S_IMAGE_REGISTRY   host:port of the registry (default 10.128.81.10:5000)
#   VF_IMAGE_TMPDIR         where the tarballs are written and deleted again
#                           (default $HOME/.cache/videoflow/images). Images are
#                           gigabytes; keep this on the big volume, never on /.
set -euo pipefail

VF_K8S_IMAGE_REGISTRY="${VF_K8S_IMAGE_REGISTRY:-10.128.81.10:5000}"
VF_IMAGE_TMPDIR="${VF_IMAGE_TMPDIR:-${XDG_CACHE_HOME:-$HOME/.cache}/videoflow/images}"

if [ "$#" -eq 0 ]; then
    echo "usage: $0 IMAGE[:TAG] [IMAGE[:TAG] ...]" >&2
    exit 2
fi

command -v docker >/dev/null 2>&1 || { echo "error: docker is not on PATH." >&2; exit 1; }
if ! command -v crane >/dev/null 2>&1; then
    cat >&2 <<'EOF'
error: crane is not on PATH. Install it into ~/.local/bin — user space, no sudo, no
change to docker (release asset name checked against go-containerregistry v0.22.1;
pin a version by replacing 'latest/download' with 'download/vX.Y.Z'):

  mkdir -p ~/.local/bin
  curl -sSL https://github.com/google/go-containerregistry/releases/latest/download/go-containerregistry_Linux_x86_64.tar.gz \
    | tar -xz -C ~/.local/bin crane
  export PATH="$HOME/.local/bin:$PATH"
  crane version
EOF
    exit 1
fi

# A corporate proxy must not sit between crane and a registry on the LAN: crane
# (Go) honours HTTP_PROXY/NO_PROXY, so exempt the registry host explicitly.
registry_host="${VF_K8S_IMAGE_REGISTRY%%:*}"
export NO_PROXY="${NO_PROXY:+$NO_PROXY,}${registry_host}"
export no_proxy="${no_proxy:+$no_proxy,}${registry_host}"

# Reachable at all? A refused connection here is a clearer message than crane's.
if ! curl --noproxy '*' -sf -m 5 -o /dev/null "http://${VF_K8S_IMAGE_REGISTRY}/v2/"; then
    echo "error: registry http://${VF_K8S_IMAGE_REGISTRY}/v2/ did not answer — is VF_K8S_IMAGE_REGISTRY right?" >&2
    exit 1
fi

mkdir -p "$VF_IMAGE_TMPDIR"
for image in "$@"; do
    case "$image" in
        */*) echo "error: pass the local NAME[:TAG] (no registry prefix): $image" >&2; exit 2 ;;
        *:*) ;;
        *) image="$image:latest" ;;
    esac
    docker image inspect "$image" >/dev/null 2>&1 || {
        echo "error: image $image is not built locally." >&2
        exit 1
    }
    ref="${VF_K8S_IMAGE_REGISTRY}/${image}"
    tarball="${VF_IMAGE_TMPDIR}/${image//[:\/]/-}.tar"
    echo "==> $image -> $ref"
    docker save "$image" -o "$tarball"
    # Best effort cleanup of the tarball even when the push fails.
    trap 'rm -f "$tarball"' EXIT
    crane push --insecure "$tarball" "$ref"
    rm -f "$tarball"
    trap - EXIT
    crane manifest --insecure "$ref" >/dev/null
    docker tag "$image" "$ref"
    echo "    pushed and present: $ref"
done
