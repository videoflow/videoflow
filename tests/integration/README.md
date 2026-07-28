# Integration tests — running the broker

Everything in this directory talks to a **real NATS JetStream server**. Nothing here needs a
Kubernetes cluster: the whole-flow tests use `LocalProcessEngine` / `videoflow run-local`, which
spawns one worker *subprocess* per node replica on your own machine. `k8s/nats.yaml` is for
deploying flows to a cluster and has nothing to do with this suite.

> **These tests skip silently when the broker is down.** A green `pytest` run is not evidence
> they passed — it may mean none of them ran. See [Verify it worked](#verify-it-worked).

## What you need running

| Service | Default URL | Override | Gates |
|---|---|---|---|
| NATS JetStream | `nats://localhost:4222` | `VF_TEST_NATS_URL` | **Everything** in this directory ([conftest.py](conftest.py)) |
| Redis | `redis://localhost:6379/0` | `VF_TEST_REDIS_URL` | [test_blob_reclamation.py](test_blob_reclamation.py) only |

Redis is genuinely optional. `test_blob_reclamation.py` skips without it, and
[test_toy_solutions.py](test_toy_solutions.py) uses it opportunistically — it passes
`--blob-redis-url` to `run-local` only when Redis answers, and otherwise runs the same flow with
inline payloads. CI starts NATS but not Redis, so the blob tests are skipped there too.

The Python clients (`nats-py`, `msgpack`, `protobuf`, `PyYAML`, `redis`) come from the `dev`
dependency group, so a plain `uv sync` at the repo root installs them — no extras flag needed.

## Start the broker

### Docker Compose (recommended)

From the repository root:

```bash
docker compose up -d          # NATS JetStream on :4222 (monitoring :8222), Redis on :6379
```

[docker-compose.yml](../../docker-compose.yml) runs `nats:2.10` with `-js -m 8222` and a `redis:7`
configured the way the CLI provisioners configure it — persistence off, memory capped, eviction
`volatile-lru`. This is the only path that gives you the full suite, blob tests included.

### Bare binaries (what CI does)

```bash
nats-server -js -sd /tmp/nats-jetstream &     # -js is not optional; see below
redis-server --save '' --appendonly no &      # only if you want the blob tests
```

[.github/workflows/ci.yml](../../.github/workflows/ci.yml) uses the first line and nothing else.

## Verify it worked

```bash
docker compose ps                      # both services "running"
curl -s localhost:8222/varz | head     # compose only — the monitoring port comes from -m 8222
uv run pytest tests/integration -q -rs
```

`-rs` is the part that matters: it prints skip reasons, so a broker that isn't up shows as
`SKIPPED ... NATS not reachable at nats://localhost:4222` rather than as a quietly passing run.

For unit tests only — what the pre-push hook runs — use `uv run pytest --ignore=tests/integration`.

## Pointing at a different broker

Both URLs are read from the environment at import time, so a broker on another host or port needs
no code change:

```bash
VF_TEST_NATS_URL=nats://10.0.0.5:4222 \
VF_TEST_REDIS_URL=redis://10.0.0.5:6379/0 \
  uv run pytest tests/integration -q -rs
```

The same variables are read by [conftest.py](conftest.py), [support_broker.py](support_broker.py)
and each test module, so setting them covers the whole directory.

## Troubleshooting

**Everything skips even though the server is up.** The probe is a TCP connect to the host and port
in `VF_TEST_NATS_URL` — check you exported the same URL the server is bound to, and that it is
`localhost` and not a container-internal hostname.

**Tests run but fail during provisioning.** Almost always a server started *without* `-js`.
JetStream is what provides the streams and consumers this suite provisions; a plain `nats-server`
accepts the connection (so the probe passes) and then rejects every stream operation.

**`address already in use` on 4222.** Something else — an earlier `nats-server`, or a compose stack
you forgot — already owns the port. `docker compose down` and `pkill nats-server`, or move the test
broker with `VF_TEST_NATS_URL`.

**Leftover streams from an interrupted run.** Harmless: every test mints a fresh `flow_id`/`run_id`
(`ids()` in [support_broker.py](support_broker.py)), so old streams are never reused, only clutter.
Clear them with `docker compose down -v`, or by deleting the `-sd` store directory and restarting
the server.

**Do not change the probe to use `nats.connect`.** nats-py retries a refused connection internally,
so a handshake against a dead port costs ~2 minutes regardless of `connect_timeout`. Each module
used to do that at import time, which made merely *collecting* this suite take ~14 minutes with no
server running. The socket check settles it in under a second and the result is cached per run.

## What the tests actually do

Two shapes live here. The broker-level tests (`test_ack_semantics.py`, `test_dlq_lifecycle.py`,
`test_delivery_ladder.py`, `test_blob_reclamation.py`, …) call `provision_flow_sync` and drive a
`NATSMessenger` in-process, which keeps the delivery ladder deterministic — publish one message,
fail it, look at what the broker holds. The whole-flow tests (`test_toy_solutions.py`,
`test_eos_replicas.py`) run real flows: the toy-solution tests copy each solution under
[solutions/](../../solutions) into a tmpdir and drive it with `videoflow run-local` in a subprocess,
asserting on the solution's own self-checking artifact. Both shapes need only the local broker.
