- ~~k3s image loading on this machine needs sudo~~ **Resolved 2026-07-19.** A narrow
  `/etc/sudoers.d/k3s-image-import` rule grants exactly
  `NOPASSWD: /usr/local/bin/k3s ctr images import -`. Note that a bare `sudo -n true` still
  prompts — that is expected, not a regression, and is not a valid probe for whether the fix is
  in place. The real probe is `sudo -n k3s ctr images import - </dev/null`, which should reach
  `ctr: unrecognized image format` rather than a password prompt.

- ~~Why can't `videoflow deploy` also build the images? Same for `run-local`?~~ **Resolved
  2026-07-19.** `deploy` always did auto-build (`--no-build` is the opt-out; there is no
  `--build`). It *looked* manual because the auto-built `:latest` tag was unusable in this
  cluster: core set no `imagePullPolicy`, so k8s inferred `Always` from `:latest` and re-pulled
  a locally loaded image from a registry that never had it → `ImagePullBackOff`, surfacing only
  as `provision Job did not complete within 180s`. The workaround (hand-built `:r1`/`:r2` tags
  passed via `--image`) then suppressed auto-build entirely, so it was never observed working.

  Fixed by rendering an explicit `imagePullPolicy` (default `IfNotPresent`) on every container,
  with `--image-pull-policy` for registry-based workflows. `run-local` gained auto-build too,
  gated so it only fires for a native component that actually needs an image — a pure-Python
  flow still spawns host subprocesses and never builds.

  The `:r1`/`:r2` tagging convention is no longer needed to dodge the pull-policy trap; letting
  auto-build own the tag is fine now.

## Still open

- No contrib component is *native* — every `component.yaml` declares a `pythonClass`. So
  `run-local`'s new build path has no real-world exerciser in these repos and is covered by unit
  tests only. Worth revisiting when a genuinely non-Python component lands.
- `deploy`'s broker teardown can't reach the in-cluster NATS from the host
  (`nats://nats.default.svc:4222`), so it prints a "teardown skipped" hint with a manual
  `videoflow teardown` command each run. Pre-existing and unrelated to the image work, but it
  means run streams accumulate in a long-lived broker.
