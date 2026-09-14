'''
Broker naming and stream/consumer configuration (videoflow.messaging.topology).

Pure functions over strings and JetStream config objects — no broker, no cluster.
The provisioning half of topology (which does need a live JetStream server) lives
in tests/integration/broker/test_topology.py; these were there too until the
directory grew its NATS gate, which skipped them for want of a server they never
touch.
'''
import pytest
from nats.js.api import DiscardPolicy, RetentionPolicy

from videoflow.core.constants import BATCH, REALTIME
from videoflow.messaging import topology


def test_names_are_run_scoped():
    assert topology.subject_for('f', 'r', 'node') == 'vf.f.r.node'
    assert topology.stream_name_for('f', 'r', 'node') == 'vf-f-r-node'
    assert topology.control_subject_for('f', 'r') == 'vf.f.r._control.stop'
    # The DLQ is the deliberate exception to run scoping: it must outlive the
    # run's teardown, which is exactly when its contents are wanted (RFC 0005).
    assert topology.dlq_stream_name('f') == 'vf-f-dlq'
    assert topology.dlq_subject_for('f', 'r', 'n') == 'vf.f._dlq.r.n'
    # Different runs never collide.
    assert topology.stream_name_for('f', 'r1', 'n') != topology.stream_name_for('f', 'r2', 'n')

def test_durable_is_child_from_parent():
    assert topology.durable_name_for('child', 'parent') == 'child--from--parent'

def test_realtime_stream_config_drops_old():
    cfg = topology.stream_config_for('f', 'r', 'n', REALTIME)
    assert cfg.retention == RetentionPolicy.LIMITS
    assert cfg.discard == DiscardPolicy.OLD
    assert cfg.max_msgs == 1

def test_batch_stream_config_is_interest_discard_new():
    cfg = topology.stream_config_for('f', 'r', 'n', BATCH)
    # Interest retention frees acked messages; Discard=NEW rejects publishes when
    # full → real backpressure instead of silent loss.
    assert cfg.retention == RetentionPolicy.INTEREST
    assert cfg.discard == DiscardPolicy.NEW
    assert cfg.max_msgs == topology.DEFAULT_BATCH_MAX_MSGS

def test_names_sanitize_illegal_chars():
    # dots in a node name would break subject tokenization → sanitized to underscore.
    assert '.' not in topology.subject_for('f', 'r', 'a.b').split('vf.f.r.')[1]

if __name__ == '__main__':
    pytest.main([__file__])


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
