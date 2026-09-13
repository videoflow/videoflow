import logging
import os

BATCH = 'batch'
REALTIME = 'realtime'
FLOW_TYPES = [BATCH, REALTIME]

GPU = 'gpu'
CPU = 'cpu'
DEVICE_TYPES = [CPU, GPU]

LOGGING_LEVEL = logging.INFO

#: RFC 0006 switch. Every wire- or routing-observable change proposed by RFC 0006
#: (per-replica final sequences on terminators, source-epoch trace ids, member-hashed
#: time-group ids, owner metadata on streams, ledger-budgeted redelivery, durable
#: control state) is gated here and OFF by default until the RFC is accepted, so the
#: default path stays byte-identical while the conformance suite exercises the new
#: semantics with ``VF_RFC0006=1``. The off-path is deleted when the RFC lands.
RFC0006 = os.environ.get('VF_RFC0006', '') == '1'
