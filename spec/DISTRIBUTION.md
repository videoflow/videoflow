# Component distribution (marketplace foundation)

How a videoflow component is published, discovered, and resolved. This is the
lightweight foundation — enough for vendors to ship components and for users to
consume them by reference — not a hosted marketplace.

## What ships

A component is two things in a registry:

1. **Container image(s)** — the normal OCI images named by the descriptor's
   `spec.runtime.images.cpu` / `.gpu`, pushed with `docker push` (or any builder).
2. **A descriptor artifact** — the `component.yaml`, pushed as an OCI **artifact**
   (media type `application/vnd.videoflow.component.v1+yaml`,
   artifactType `application/vnd.videoflow.component.v1`) that references those
   images. This is what a user resolves by reference; it carries everything
   videoflow needs to wire, validate, and deploy the component without its source.

Keeping the descriptor as a separate small artifact means a user (or the future
marketplace index) can inspect a component — its params, io, device support,
protocol version — without pulling multi-gigabyte ML images.

## Reference grammar

```
oci://<registry>/<repository>:<tag>
e.g.  oci://ghcr.io/acme/sort-tracker:1.2.0
```

The images the descriptor points at **should** be pinned by digest
(`...@sha256:...`) for reproducibility; tags are accepted.

## CLI

```bash
# Publish (validates the descriptor first; a broken descriptor is never pushed):
videoflow component push ./sort-tracker oci://ghcr.io/acme/sort-tracker:1.2.0

# Inspect without deploying (pulls + caches the descriptor only):
videoflow component inspect oci://ghcr.io/acme/sort-tracker:1.2.0

# Pull + cache (optionally verifying the signature first):
videoflow component pull oci://ghcr.io/acme/sort-tracker:1.2.0 --verify --cosign-arg --key=cosign.pub
```

Resolution is transparent in a graph — `component('oci://...')` pulls and caches the
descriptor on first use, then resolves offline:

```python
tracker = component('oci://ghcr.io/acme/sort-tracker:1.2.0',
                    params={'iou_threshold': 0.4})(detector)
```

Pulled descriptors are cached under `~/.videoflow/components/` (override with
`VIDEOFLOW_COMPONENT_CACHE`).

## Trust

videoflow does **not** run its own PKI. Signature verification delegates to
[cosign](https://docs.sigstore.dev/cosign/): vendors sign their images and the
descriptor artifact, and consumers verify with `--verify` (which shells out to
`cosign verify`, passing through the trust policy via `--cosign-arg`, e.g.
keyless `--certificate-identity=... --certificate-oidc-issuer=...` or keyed
`--key=cosign.pub`). Verification is opt-in for the foundation; a hosted index
would make it mandatory.

At pull time videoflow also checks the descriptor's `spec.protocol` against the
protocol version the local build supports, so an incompatible component fails fast.

## Explicit non-goals (for now)

- No hosted index / search service — a curated `awesome-videoflow-components` list
  suffices until there are more than a handful of external vendors.
- No payments, ratings, or central namespace authority.
- No automatic image signing — vendors run cosign in their own release pipeline.

These are deliberately deferred; the artifact + reference conventions above are
forward-compatible with adding an index later.
