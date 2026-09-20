'''
Broker naming and stream/consumer configuration (videoflow.messaging.topology).

Pure functions over strings and JetStream config objects — no broker, no cluster.
The provisioning half of topology (which does need a live JetStream server) lives
in tests/integration/broker/test_topology.py; these were there too until the
directory grew its NATS gate, which skipped them for want of a server they never
touch. The names and stream configs themselves are pinned byte-for-byte by
tests/test_render_goldens.py (a 205-tuple corpus and both flow types); what is
left here is the arithmetic no golden covers.
'''
import pytest

from videoflow.core.constants import BATCH, REALTIME
from videoflow.messaging import topology


def test_stream_replicas_are_requested_only_when_more_than_one():
    # A single-server request is exactly what it always was (the field stays unset).
    assert topology.stream_config_for('f', 'r', 'n', BATCH).num_replicas is None
    assert topology.stream_config_for('f', 'r', 'n', BATCH, replicas = 1).num_replicas is None
    assert topology.stream_config_for('f', 'r', 'n', REALTIME, replicas = 3).num_replicas == 3
    assert topology.dlq_stream_config('f').num_replicas is None
    assert topology.dlq_stream_config('f', replicas = 3).num_replicas == 3

def test_consumer_credit_follows_the_replica_count_for_a_shared_durable():
    # STREAM-15: nb_tasks x (item_credit + prefetch) shared, item_credit + prefetch per partitioned replica.
    assert topology.consumer_credit(1, False) == 1 + topology.DEFAULT_PREFETCH
    assert topology.consumer_credit(10, False) == 10 * (1 + topology.DEFAULT_PREFETCH)
    assert topology.consumer_credit(10, True) == 1 + topology.DEFAULT_PREFETCH
    assert topology.consumer_credit(4, False, item_credit = 2, prefetch = 1) == 12
    assert topology.LEGACY_BIND_CREDIT == 6 and topology.DEFAULT_MAX_ACK_PENDING == 8

if __name__ == '__main__':
    pytest.main([__file__])
