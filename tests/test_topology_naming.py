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
