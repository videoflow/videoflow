'''
Negative controls for the kubernetes-level durability cases (MSG-021, MSG-022,
MSG-023, PAY-011): each reproduces the reviewed defect as a planner or outcome
substitute the paired oracle must catch. Test-tree only; nothing under
``videoflow/`` imports this. The model-level reproductions live here; the one
kubernetes-level control (MSG-022 against the ephemeral dev broker) is a test in
``test_msg_durability.py`` because it needs the cluster.
'''
from __future__ import absolute_import, division, print_function

import dataclasses
from typing import Any, Callable

from videoflow.backends import capabilities
from videoflow.backends.capabilities import FlowRequirements
from videoflow.backends.outcomes import known


def name_certifying_planner() -> Callable[..., Any]:
    '''
    The reviewed defect behind MSG-021: durability was "certified" by the
    existence of durable names and a broker Deployment, never by the stream's
    replica count or the volume under its file store. Reproduced as a planner
    that drops the tolerated-failure requirement before planning.
    '''
    real = capabilities.plan_composition

    def planner(requirements : FlowRequirements, messaging : Any, **kwargs : Any) -> Any:
        return real(dataclasses.replace(requirements, tolerated_failures = 0), messaging, **kwargs)
    return planner


def broker_durability_planner() -> Callable[..., Any]:
    '''
    The reviewed defect behind PAY-011: a replicated, persistent broker was read
    as durability for the *payloads* too, although the Redis they lived in was an
    emptyDir cache. Reproduced as a planner that reports the payload store as
    durable whenever the broker is.
    '''
    real = capabilities.plan_composition

    def planner(requirements : FlowRequirements, messaging : Any, **kwargs : Any) -> Any:
        payload = kwargs.get('payload')
        if payload is not None and isinstance(messaging.persistent_storage, known(True).__class__) \
                and messaging.persistent_storage.value:
            kwargs['payload'] = dataclasses.replace(payload, durable = known(True), persistent_storage = known(True))
        return real(requirements, messaging, **kwargs)
    return planner


def accepting_on_timeout(outcome : Any) -> Any:
    '''
    The reviewed defect behind MSG-023 (the old ``_publish`` retry loop): a send
    whose acknowledgement never came was retried and, once the retries ran out,
    treated as done — a durable receipt minted from silence. Reproduced as an
    outcome substitution the oracle must reject.
    '''
    from videoflow.backends.outcomes import Accepted, PublicationUnknown
    if isinstance(outcome, PublicationUnknown):
        return Accepted(outcome.publication_id, None, False, 'stream')
    return outcome


def flow_scoped_names(monkeypatch : Any) -> None:
    '''
    The reviewed defect behind RUN-047: every run of a flow rendered the same
    resource names (``vf-<flow>-<node>``, ``vf-<flow>-specs``, ...), so a second
    concurrent run ``kubectl apply``ed over the first. Reproduced by dropping the
    run id from ``manifests.run_name`` and the run-id term from the selectors.
    '''
    from videoflow.deploy import manifests

    monkeypatch.setattr(manifests, 'run_name', lambda flow_id, run_id, *parts: manifests.k8s_name('vf', flow_id, *parts))
    monkeypatch.setattr(manifests, '_selector', lambda flow_id, run_id, node: {
        manifests.LABEL_FLOW_ID: manifests.k8s_name(flow_id), 'videoflow.io/node': manifests.k8s_name(node)})


def free_namespace_on_unread(monkeypatch : Any) -> None:
    '''
    The companion defect: a failed listing of the namespace read as "no other
    run", so ``--single-run`` started beside a run it could not see.
    '''
    from videoflow.backends.outcomes import known
    from videoflow.deploy import cluster

    monkeypatch.setattr(cluster, 'active_runs_observed', lambda kubectl, namespace, flow_id: known(set()))
