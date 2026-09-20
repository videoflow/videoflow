# Broker teardown audit

Reviewed **2026-09-20**, against core `73e3b84`.

**Status: Still valid** when the deployment host cannot reach the broker's in-cluster address.

The default broker URL is `nats://nats.<namespace>.svc:4222` ([infra.py](../videoflow/deploy/infra.py#L331), lines 331–334). `KubernetesExecutionEngine.teardown()` passes that same URL to a direct host-side NATS connection. On connection failure it warns that teardown was skipped and proceeds with Kubernetes resource deletion ([kubernetes.py](../videoflow/engines/kubernetes.py#L637), lines 637–655 and 674–704). There is no in-cluster cleanup or forwarding fallback. Manual `videoflow teardown` has the same reachability limitation ([deploy/cli.py](../videoflow/deploy/cli.py#L1349), lines 1349–1367).

Consequently, run streams can accumulate in a reused or long-lived broker even though the run's Kubernetes workloads are deleted. Deleting an ephemeral broker may hide the symptom; a host-reachable broker URL avoids this particular failure.

**Remaining work:** make broker cleanup reach the actual broker from the deployment environment, such as through an in-cluster cleanup operation or a managed forwarding connection. Verify stream removal with the default in-cluster URL while preserving other runs and flow-scoped DLQ evidence. Kubernetes deletion should still proceed if broker cleanup fails, with an accurate incomplete-cleanup diagnostic.

**Verification:** a Python probe mocked `_publish_stop` to raise `OSError` and mocked `delete_resources`; it confirmed that teardown attempts `nats://nats.default.svc:4222`, emits the skipped-cleanup warning, and still deletes workloads. No live broker or Kubernetes integration test was run. Existing conformance support explicitly uses a host-reachable NodePort to clean up streams ([tests/conformance/_k8s.py](../tests/conformance/_k8s.py#L367), line 367), so that workaround does not establish that the default teardown path is fixed.

## Historical notes (preserved)

`deploy`'s broker teardown can't reach the in-cluster NATS from the host
  (`nats://nats.default.svc:4222`), so it prints a "teardown skipped" hint with a manual
  `videoflow teardown` command each run. Pre-existing and unrelated to the image work, but it
  means run streams accumulate in a long-lived broker.
