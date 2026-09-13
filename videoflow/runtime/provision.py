'''
Provisioning entrypoint (``python -m videoflow.provision``): creates every stream
and durable consumer a flow needs, before its workers start. Run as a one-shot
Kubernetes init Job so that BATCH interest-retention streams have their consumers
registered before any message is published (otherwise early messages are dropped).

Driven by environment variables::

    VF_NATS_URL         nats://host:port
    VF_FLOW_ID          stable flow id
    VF_RUN_ID           per-run id
    VF_FLOW_TYPE        realtime | batch
    VF_STREAM_REPLICAS  optional; stream copies to request (a replicated broker profile).
    VF_MAX_RETRIES      optional; BATCH redelivery attempts (default 3)
    VF_FLOW_SPECS_JSON  the compiled NodeSpecs as a JSON list, OR
    VF_FLOW_SPECS_PATH  path to a file with that JSON (default /etc/videoflow/specs.json)
    VF_BLOB_REDIS_URL   optional; the payload store the workers offload to — read
                        back for the admission below, never written here.
    VF_PROFILE_REQUESTS_JSON  optional; the operator's explicit channel profiles
                        (deploy --require-profile) as JSON. Absent => the
                        flow-type presets, i.e. today's behaviour.
    VF_ADMISSION_TIMEOUT_SECONDS optional; how long each read-back below may take,
                        connect included (default 60). A service that has not
                        answered by then is Unknown, never assumed.

Admission (RFC 0006 ENV-13). With explicit profile requests, or under the
``VF_RFC0006`` switch, the composition is admitted **before** anything is
created — the same admission ``videoflow deploy`` ran on the operator's machine
(``deploy.admission``), so a Job and a deploy never disagree — except that here
the broker and the store are always read back live
(``jetstream_capabilities_observed`` / ``redis_payload_capabilities_observed``):
``plan_composition`` admits them or rejects by name. A definite incompatibility
is binding (``IncompatibleProfile``, exit 2); an unobservable capability binds
only with explicit requests (``UnobservableState``, exit 3) and is a warning
under the switch alone. After provisioning, explicit requests are checked once
more against what the broker actually holds (``topology.read_back_streams`` +
``verify_channel_profiles``): a stream whose effective configuration contradicts
its requested profile — ``reliable_work`` on a limits/discard-old stream, fewer
copies than ``VF_STREAM_REPLICAS`` — is ``IncompatibleProfile`` whatever the
switch says, because the request was explicit. Without requests and with the
switch off nothing here changes: the streams are created exactly as they always
were.

A typed rejection is rendered the way the CLI renders one (``ERROR [code]:
message`` and the remedy, on stderr — ``VideoflowError.render``) and ends the
process with the error's exit code rather than a traceback: the Job's
termination message is what the deploy watchdog shows the operator. ``main``
raises ``SystemExit`` itself because the frozen ``videoflow.provision`` shim
calls it bare and discards a return value.
'''
from __future__ import absolute_import, division, print_function

import json
import logging
import os
import sys
from typing import Sequence

from ..backends.capabilities import (
    ADMISSION_TIMEOUT_ENV,
    PROFILE_REQUESTS_ENV,
    ProfileRequest,
    admission_timeout_from_env,
    requests_from_env,
)
from ..core import constants
from ..core.compiler import NodeSpec
from ..core.errors import VideoflowError
from ..deploy.admission import (
    admit,
    enforce_admission,
    jetstream_capabilities_observed,
    redis_payload_capabilities_observed,
    requirements_for,
    run_stream_names,
    unknown_admission,
)
from ..messaging.topology import provision_flow_sync, read_back_streams, verify_channel_profiles

logger = logging.getLogger('videoflow.provision')

def _load_specs() -> list:
    raw = os.environ.get('VF_FLOW_SPECS_JSON')
    if raw is None:
        path = os.environ.get('VF_FLOW_SPECS_PATH', '/etc/videoflow/specs.json')
        with open(path) as f:
            raw = f.read()
    return [NodeSpec.from_dict(d) for d in json.loads(raw)]

def admit_composition(nats_url : str, blob_redis_url : str | None, specs : Sequence[NodeSpec], flow_id : str,
                      run_id : str, flow_type : str, explicit : Sequence[ProfileRequest], timeout : float) -> None:
    '''
    The CLI's admission, run again where the broker and store are actually
    reachable and always read back live. ``fail_fast = False``: this Job may
    start while the broker it was deployed beside is still coming up, so the
    client's own retry schedule applies within ``timeout``.

    - Raises:
        - IncompatibleProfile / UnobservableState: exactly as ``deploy.admission.admit``.
    '''
    messaging = jetstream_capabilities_observed(nats_url, timeout = timeout,
                                                stream_names = run_stream_names(flow_id, run_id, specs),
                                                fail_fast = False)
    payload = redis_payload_capabilities_observed(blob_redis_url, timeout = timeout) if blob_redis_url else None
    admit(requirements_for(flow_type, specs, explicit), messaging, payload,
          payload_refs_in_use = blob_redis_url is not None, enforce = enforce_admission(explicit),
          unknown_is_fatal = unknown_admission(explicit), where = 'provision')

def verify_provisioned_profiles(nats_url : str, specs : Sequence[NodeSpec], flow_id : str, run_id : str,
                                flow_type : str, explicit : Sequence[ProfileRequest], replicas : int,
                                timeout : float) -> None:
    '''
    Read the explicitly requested channels' streams back after provisioning and
    bind the requests to what the broker holds — profile semantics *and* the
    requested fields (``config_mismatches = True``: this entrypoint knows the
    full request, ``VF_STREAM_REPLICAS`` included). Nothing to do without
    explicit requests.

    - Raises:
        - IncompatibleProfile: a stream contradicts its requested profile or request.
        - BrokerUnavailable: a requested channel's stream is not there after all.
        - UnobservableState: a requested channel could not be read back.
    '''
    names = {spec.name for spec in specs}
    requested = [r for r in explicit if r.channel in names]
    if not requested:
        return
    read_back = read_back_streams(nats_url, flow_id, run_id, [r.channel for r in requested], flow_type,
                                  timeout = timeout, replicas = replicas, fail_fast = False)
    verify_channel_profiles(read_back, requested, unknown_is_fatal = True, where = 'provision',
                            config_mismatches = True)
    logger.info('explicit channel profiles verified against the provisioned streams: '
                + ', '.join(f'{r.channel}={r.profile}' for r in requested))

def provision() -> None:
    '''
    The whole entrypoint, raising typed errors for ``main`` to render: admission
    (when it applies), provisioning, verification (when requests were explicit).
    '''
    specs = _load_specs()
    flow_id = os.environ['VF_FLOW_ID']
    run_id = os.environ['VF_RUN_ID']
    flow_type = os.environ.get('VF_FLOW_TYPE', 'realtime')
    max_retries = int(os.environ.get('VF_MAX_RETRIES', '3'))
    # A replicated broker profile asks for that many stream copies (STREAM-14);
    # absent ⇒ the server default, which is what a single-server render gets.
    replicas = int(os.environ.get('VF_STREAM_REPLICAS', '1'))
    nats_url = os.environ['VF_NATS_URL']
    blob_redis_url = os.environ.get('VF_BLOB_REDIS_URL') or None
    explicit = requests_from_env(os.environ.get(PROFILE_REQUESTS_ENV))
    timeout = admission_timeout_from_env(os.environ.get(ADMISSION_TIMEOUT_ENV))
    if explicit or constants.RFC0006:
        admit_composition(nats_url, blob_redis_url, specs, flow_id, run_id, flow_type, explicit, timeout)
    provision_flow_sync(nats_url, specs, flow_id, run_id, flow_type,
                        max_retries = max_retries, replicas = replicas)
    logger.info(f'Provisioned {len(specs)} node streams for flow {flow_id} run {run_id}')
    if explicit:
        verify_provisioned_profiles(nats_url, specs, flow_id, run_id, flow_type, explicit, replicas, timeout)

def main() -> None:
    logging.basicConfig(level = logging.INFO,
                        format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    try:
        provision()
    except VideoflowError as e:
        print(e.render(), file = sys.stderr)
        raise SystemExit(e.exit_code) from e

if __name__ == '__main__':
    main()
