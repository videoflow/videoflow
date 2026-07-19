'''
Provisioning entrypoint (``python -m videoflow.provision``): creates every stream
and durable consumer a flow needs, before its workers start. Run as a one-shot
Kubernetes init Job so that BATCH interest-retention streams have their consumers
registered before any message is published (otherwise early messages are dropped).

Driven by environment variables:

    VF_NATS_URL         nats://host:port
    VF_FLOW_ID          stable flow id
    VF_RUN_ID           per-run id
    VF_FLOW_TYPE        realtime | batch
    VF_MAX_RETRIES      optional; BATCH redelivery attempts (default 3)
    VF_FLOW_SPECS_JSON  the compiled NodeSpecs as a JSON list, OR
    VF_FLOW_SPECS_PATH  path to a file with that JSON (default /etc/videoflow/specs.json)
'''
from __future__ import absolute_import, division, print_function

import json
import logging
import os

from ..core.compiler import NodeSpec
from ..messaging.topology import provision_flow_sync

logger = logging.getLogger('videoflow.provision')

def _load_specs() -> list:
    raw = os.environ.get('VF_FLOW_SPECS_JSON')
    if raw is None:
        path = os.environ.get('VF_FLOW_SPECS_PATH', '/etc/videoflow/specs.json')
        with open(path) as f:
            raw = f.read()
    return [NodeSpec.from_dict(d) for d in json.loads(raw)]

def main() -> None:
    logging.basicConfig(level = logging.INFO,
                        format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    specs = _load_specs()
    flow_id = os.environ['VF_FLOW_ID']
    run_id = os.environ['VF_RUN_ID']
    flow_type = os.environ.get('VF_FLOW_TYPE', 'realtime')
    max_retries = int(os.environ.get('VF_MAX_RETRIES', '3'))
    provision_flow_sync(os.environ['VF_NATS_URL'], specs, flow_id, run_id, flow_type,
                        max_retries = max_retries)
    logger.info(f'Provisioned {len(specs)} node streams for flow {flow_id} run {run_id}')

if __name__ == '__main__':
    main()
