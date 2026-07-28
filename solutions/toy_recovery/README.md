# toy_recovery — error handling, end to end

A deployable solution whose subject is **what videoflow does when things go
wrong**. It produces two failures on purpose, because the framework treats them
differently and the difference is the point:

```
events ──> fragile ──> ledger
```

| Failure | What it is | What happens |
|---|---|---|
| `fragile.poison_values` | a **bad message** — it will never parse | dead-lettered on its *first* failure; the rest of the stream is untouched |
| `fragile.crash_at` | a **sick worker** — it cannot process anything | the message is handed back, the worker exits, the supervisor restarts it, and the fresh worker succeeds |

The event that killed a worker ends up **delivered**. The events that were
unparseable end up **dead-lettered**. Nothing is lost and nothing is delivered
twice, which is what the success artifact claims:

> every event either arrived exactly once or was dead-lettered —
> never both, never neither.

`prepare.py` bakes that split before the run, so `recovery_report.json` →
`"matches_expected": true` is a statement about correctness rather than a
description of whatever happened.

## Run it

Locally, one worker subprocess per node:

```bash
videoflow run-local toy_recovery.py
```

You will see the worker die once and come back:

```
node fragile replica 0 failed; restarting in 1s (attempt 1/3)
```

That restart is the same policy Kubernetes applies through a Job's
`backoffLimit` — matching it locally is deliberate, so a crash that the cluster
absorbs is absorbed here too instead of hanging. Pass `--no-restart` to turn it
off and watch the flow fail instead.

On a cluster (config Q&A, image build, broker, run and teardown in one command):

```bash
videoflow deploy toy_recovery.py
```

## Look at what failed

The dead-letter queue is scoped to the **flow**, not the run, so tearing the run
down does not delete it — which matters, because the moment you want the
evidence is right after a run that failed and was cleaned up:

```bash
videoflow dlq ls --flow-id toy-recovery
```

```
  #  NODE       CODE                 DELIV  RUN / ERROR
  1  fragile    VF_POISON_SCHEMA         1  49c4273 / event 7 is malformed and will never parse
  2  fragile    VF_POISON_SCHEMA         1  49c4273 / event 23 is malformed and will never parse

by code: VF_POISON_SCHEMA=2
```

`DELIV 1` is worth noticing: a poison message is dead-lettered on its first
failure rather than retried four times on its way to the same queue.

Then look at one in full, payload included:

```bash
videoflow dlq show --flow-id toy-recovery --id 1
```

And once the producer is fixed, put them back:

```bash
videoflow dlq replay --flow-id toy-recovery --to-run <run-id>
```

## Configuration

See [config.example.yaml](config.example.yaml) for every knob. The ones that
change what the run demonstrates:

- `fragile.poison_values` — which events are unparseable. More of them means a
  bigger `dead_lettered` list; the delivered set shrinks to match, and
  `matches_expected` stays true.
- `fragile.crash_at` — the event whose worker dies once. Set it to `null` to drop
  the recovery half and keep only the quarantine half.
- `max_retries` — redeliveries before a *transient* failure is dead-lettered.
  It deliberately has no effect on the poison path.

## Artifacts

Everything lands in `work_dir`:

| File | Written by | What it is |
|---|---|---|
| `expected_recovery.json` | `prepare.py` | the delivered/dead-lettered split a correct run must produce |
| `ledger.jsonl` | `ledger` | every event as it arrived |
| `recovery_report.json` | `ledger.close()` | the self-check, including `matches_expected` |
| `crashed_once.marker` | `fragile` | records that the once-off crash already happened, so the restarted worker succeeds |

## Why BATCH

The report makes a *completeness* claim, and only at-least-once retention can
support one. Under REALTIME the broker may legitimately drop a message, and
"delivered or dead-lettered, never neither" would stop being true through no
fault of the error handling.

## See also

- [Error handling and recovery](../../docs/source/user-documentation/error-handling-and-recovery.rst)
  — the full model this solution demonstrates.
- [RFC 0005](../../spec/rfcs/0005-error-taxonomy-and-abort.md) — why the taxonomy
  and the ABORT marker exist.
