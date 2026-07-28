'''
The dead-letter queue as a queue rather than a graveyard.

Two things were wrong with it. It was scoped to the *run*, and teardown — which
runs in a ``finally``, on success and failure alike — deleted it, so its
week-long retention applied to almost nobody and the evidence vanished at exactly
the moment somebody wanted it. And nothing could drain it: a queue you can read
but not replay from does not close the loop between "a message failed" and "the
work got done".

Needs a reachable NATS JetStream server.
'''
from __future__ import absolute_import, division, print_function

import pytest
from support_broker import (
    NATS_URL,
    StubNode,
    cleanup,
    ids,
    publish_parent_message,
    read_dlq,
    spec,
    stream_exists,
)

from videoflow.core.constants import BATCH
from videoflow.core.errors import EXIT_ENVIRONMENT, SchemaError
from videoflow.deploy import cli
from videoflow.messaging import topology
from videoflow.messaging.nats_messenger import NATSMessenger
from videoflow.messaging.topology import provision_flow_sync

pytestmark = pytest.mark.timeout(90)


def _dead_letter(flow_id, run_id, values):
    '''Runs `values` through a node that rejects them all, so each dead-letters.'''
    specs = [spec('parent', [], 'producer', True),
            spec('child', ['parent'], 'consumer', False)]
    provision_flow_sync(NATS_URL, specs, flow_id, run_id, BATCH, max_retries = 0)
    m = NATSMessenger(StubNode('child'), ['parent'], NATS_URL, flow_id, BATCH, run_id,
                    max_retries = 0, ack_wait = 3)
    try:
        for i, value in enumerate(values):
            publish_parent_message(flow_id, run_id, 'parent', f't{i}', i, value)
            m.receive_message()
            m.fail_inputs(SchemaError(f'cannot parse {value}'))
    finally:
        m.close()


def test_the_dlq_survives_run_teardown():
    '''
    The bug: teardown deleted the dead letters along with the run, so the record
    of what a failed run lost disappeared with the run that lost it.
    '''
    flow_id, run_id = ids('dlq')
    try:
        _dead_letter(flow_id, run_id, [{'v': 1}])
        assert len(read_dlq(flow_id)) == 1

        # Exactly what happens at the end of every deploy.
        import asyncio

        import nats

        async def _teardown():
            nc = await nats.connect(NATS_URL)
            await topology.delete_run_streams(nc, flow_id, run_id)
            await nc.drain()

        asyncio.run(_teardown())

        assert not stream_exists(topology.stream_name_for(flow_id, run_id, 'parent'))
        assert stream_exists(topology.dlq_stream_name(flow_id))     # the evidence remains
    finally:
        cleanup(flow_id, run_id)


def test_two_runs_of_one_flow_coexist_and_stay_attributable():
    '''
    Flow-scoping is only useful if an entry still says which run produced it —
    otherwise a shared queue is worse than a deleted one.
    '''
    flow_id, run_a = ids('dlq')
    _run_b = ids('dlq')[1]
    try:
        _dead_letter(flow_id, run_a, [{'v': 'a'}])
        _dead_letter(flow_id, _run_b, [{'v': 'b'}, {'v': 'b2'}])

        assert len(read_dlq(flow_id)) == 3                       # one queue
        assert len(read_dlq(flow_id, run_id = run_a)) == 1       # filterable by run
        assert len(read_dlq(flow_id, run_id = _run_b)) == 2
        assert len(read_dlq(flow_id, node = 'child')) == 3       # and by node
        assert read_dlq(flow_id, node = 'nobody') == []
    finally:
        cleanup(flow_id, run_a)
        cleanup(flow_id, _run_b)


def test_ls_groups_by_code_and_show_decodes_the_payload(capsys):
    flow_id, run_id = ids('dlq')
    try:
        _dead_letter(flow_id, run_id, [{'v': 41}, {'v': 42}])
        cli.main(['dlq', 'ls', '--flow-id', flow_id, '--nats', NATS_URL])
        out = capsys.readouterr().out
        assert 'VF_POISON_SCHEMA' in out
        assert 'VF_POISON_SCHEMA=2' in out           # the by-code summary

        cli.main(['dlq', 'show', '--flow-id', flow_id, '--id', '1', '--nats', NATS_URL])
        shown = capsys.readouterr().out
        assert 'VF-Code=VF_POISON_SCHEMA' in shown
        assert 'cannot parse' in shown
        assert "'v': 41" in shown                    # the original payload survived
    finally:
        cleanup(flow_id, run_id)


def test_replay_republishes_onto_the_origin_subject(capsys):
    '''
    The loop-closing property: after a fix, the dead letters go back in and the
    work gets done. The replayed copy must carry a **fresh** message id — reusing
    the original would land inside the stream's de-duplication window and
    JetStream would silently discard the very message being replayed.
    '''
    flow_id, run_id = ids('dlq')
    try:
        _dead_letter(flow_id, run_id, [{'v': 'retry-me'}])
        assert len(read_dlq(flow_id)) == 1

        cli.main(['dlq', 'replay', '--flow-id', flow_id, '--run-id', run_id,
                '--nats', NATS_URL])
        assert 'Replayed 1' in capsys.readouterr().out

        # It is back on the input subject the failing node reads from, decodable
        # and unchanged.
        m = NATSMessenger(StubNode('child'), ['parent'], NATS_URL, flow_id, BATCH, run_id,
                        max_retries = 0, ack_wait = 3)
        try:
            inputs = m.receive_message()
            assert inputs['parent']['message'] == {'v': 'retry-me'}
            m.ack_inputs()
        finally:
            m.close()
    finally:
        cleanup(flow_id, run_id)


def test_replay_dry_run_changes_nothing(capsys):
    flow_id, run_id = ids('dlq')
    try:
        _dead_letter(flow_id, run_id, [{'v': 1}])
        cli.main(['dlq', 'replay', '--flow-id', flow_id, '--run-id', run_id,
                '--nats', NATS_URL, '--dry-run'])
        out = capsys.readouterr().out
        assert 'would replay' in out and '--dry-run' in out
        assert len(read_dlq(flow_id)) == 1           # still there, nothing republished
    finally:
        cleanup(flow_id, run_id)


def test_replay_can_select_by_code(capsys):
    flow_id, run_id = ids('dlq')
    try:
        _dead_letter(flow_id, run_id, [{'v': 1}, {'v': 2}])
        cli.main(['dlq', 'replay', '--flow-id', flow_id, '--run-id', run_id,
                '--code', 'VF_NO_SUCH_CODE', '--nats', NATS_URL])
        assert 'Nothing to replay' in capsys.readouterr().out
    finally:
        cleanup(flow_id, run_id)


def test_purge_is_the_only_thing_that_deletes_dead_letters(capsys):
    flow_id, run_id = ids('dlq')
    try:
        _dead_letter(flow_id, run_id, [{'v': 1}])
        assert stream_exists(topology.dlq_stream_name(flow_id))
        cli.main(['dlq', 'purge', '--flow-id', flow_id, '--nats', NATS_URL])
        assert 'Purged' in capsys.readouterr().out
        assert not stream_exists(topology.dlq_stream_name(flow_id))
    finally:
        cleanup(flow_id, run_id)


def test_reading_a_flow_with_no_dlq_reports_it_without_a_traceback(capsys):
    '''
    A flow that never dead-lettered anything has no stream. That is worth saying
    clearly — and saying it as a message with a fix, not a stack trace.
    '''
    flow_id, _run_id = ids('dlq')
    code = cli.main(['dlq', 'ls', '--flow-id', flow_id, '--nats', NATS_URL])
    err = capsys.readouterr().err
    assert code == EXIT_ENVIRONMENT
    assert 'No dead-letter stream' in err
    assert 'has no DLQ stream yet' in err             # names why, not just what
    assert 'Traceback' not in err


if __name__ == '__main__':
    pytest.main([__file__])
