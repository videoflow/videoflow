What's structurally wrong
"This run is over" is a latched fact, and it's being carried on a fire-and-forget event channel with no retention. Repeating is the standard way to fake a latch over a lossy channel — it works, but only while the announcer is alive. For LocalProcessEngine that assumption is provably true (the reaping loop is the announcer's lifetime), which is why the fix is sound where I put it. It doesn't generalize:

Kubernetes: there is no supervisor during the run. deploy may have exited. A node whose Job exhausts backoffLimit mid-run has nobody to announce it — resource deletion covers an operator-initiated teardown, not a give-up nobody is watching.
videoflow stop (cli.py:776): the same one-shot publish, racing against any worker that is restarting or still connecting.
So the repeat fixes the reported hang and the local engine's class of it, and leaves those two.

The better long-term fix, and it's already half-built
Make the receiver self-sufficient. A node that has been waiting on work it can never get should end itself, with no announcer, no matter who died or which engine is running. That's the end-to-end argument, and ProgressDeadline already is that layer — _abort_downstream's own docstring names it as the backstop behind the supervisor's control-abort.

Except it can't fire. deadline.check() is only called at task.py:388 and task.py:451 — both after receive_message() returned a group and it was processed. When the node is blocked inside receive_message, which is precisely this hang, check() is never reached. The deadline is dead code for its own motivating case.

And it would have gotten the verdict exactly right. pending_count sums queued + in-group + broker pending/unacked across parents; for joined that was 27 (broker_pending=19, unacked=8 from right). Nothing acked for 300s with 27 pending is ProgressStalled — a correct, self-diagnosed death with a real error message, instead of a silent wait for a message from a supervisor that might not exist.

The fix is not to call check() from the messenger (that inverts the layering — the deadline is core, injected by runtime). The clean shape is a watchdog thread in runtime/worker.py that polls the deadline and, when it trips, writes the termination reason and exits the process — which works regardless of where the loop is blocked, and keeps core pure.

Second, lower priority: latch the stop. Give the run a durable "aborted" fact — a one-message JetStream stream or KV entry per run — that a worker reads at startup and then subscribes to live. A single publish then suffices, late-connecting workers discover it whenever they arrive, and the k8s and CLI publishers get correctness for free rather than each needing its own repeat loop. Cost is real: topology, provisioning and teardown changes, an RFC (a new stream is a routing change), and a decision about the fact outliving teardown.

What I'd recommend
Do the watchdog. It's contained, it removes the "somebody must be alive to tell you" assumption entirely, and it turns a hang into a diagnosed failure in every engine. The latched stop is the more complete answer but costs a protocol change; with the watchdog in place it becomes an optimization (fail in 2s instead of 300s) rather than a correctness requirement.

Keep the repeat either way — it's what makes the fast path reliable locally, and with the watchdog behind it the two layers are belt-and-braces rather than one load-bearing rope. But I'd stop describing the progress deadline as a backstop in docstrings and spec/PROTOCOL.md until it actually is one.
