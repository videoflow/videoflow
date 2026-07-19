'''
Logging configuration for workers. Opt into JSON structured logs (one object per
line, easy to ship to a log aggregator) by setting ``VF_STRUCTURED_LOGS=1``;
otherwise a plain human-readable format is used. Node-scoped fields (flow/run/node/
replica/trace ids) are included when a log record carries them as ``extra=...``.
'''
from __future__ import absolute_import, division, print_function

import json
import logging
import os

_CONTEXT_FIELDS = ('flow_id', 'run_id', 'node_name', 'replica_id', 'trace_id', 'span_id', 'edge_id')

class JsonFormatter(logging.Formatter):
    def format(self, record : logging.LogRecord) -> str:
        payload = {
            'ts': self.formatTime(record),
            'level': record.levelname,
            'logger': record.name,
            'msg': record.getMessage(),
        }
        for field in _CONTEXT_FIELDS:
            value = getattr(record, field, None)
            if value is not None:
                payload[field] = value
        if record.exc_info:
            payload['exc'] = self.formatException(record.exc_info)
        return json.dumps(payload)

def configure_logging() -> None:
    structured = os.environ.get('VF_STRUCTURED_LOGS', '').lower() in ('1', 'true', 'yes')
    handler = logging.StreamHandler()
    if structured:
        handler.setFormatter(JsonFormatter())
    else:
        handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
    root = logging.getLogger()
    root.handlers = [handler]
    root.setLevel(logging.INFO)
