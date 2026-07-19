'''
Partitioned parallelism. A processor with nb_tasks > 1 normally load-balances:
every message goes to whichever replica is free (competing consumers). Set
partition_by and the routing changes -- each message is instead pinned to one
replica chosen by hash(key) % nb_tasks, so all messages sharing a key always
land on the same replica. That stickiness is what lets a *replicated* processor
safely keep per-key state.

Here partition_by='trace_id' routes each message by its lineage id. Each replica
tags its output with its own replica index (read from VF_REPLICA_ID, which the
engine sets for every worker) so the routing is visible in the printed output --
a given input integer is always handled by the same replica.

The ReplicaTagProcessor lives in examples/example_nodes.py (custom nodes must be
in an importable module so each worker can reconstruct them); this script puts
that directory on sys.path, which the local engine re-exports to the spawned workers.

    python examples/partitioned_processing.py
'''
import os
import sys

_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, _HERE)

from example_nodes import ReplicaTagProcessor  # noqa: E402

from videoflow.consumers import CommandlineConsumer  # noqa: E402
from videoflow.core import Flow  # noqa: E402
from videoflow.core.constants import BATCH  # noqa: E402
from videoflow.producers import IntProducer  # noqa: E402


def build_flow():
    numbers = IntProducer(0, 20, 0.05, name = 'numbers')
    tagged = ReplicaTagProcessor(nb_tasks = 4, partition_by = 'trace_id', name = 'tagged')(numbers)
    printer = CommandlineConsumer(name = 'printer')(tagged)
    return Flow([printer], flow_type = BATCH)


if __name__ == '__main__':
    from videoflow.engines.local import LocalProcessEngine
    flow = build_flow()
    flow.run(LocalProcessEngine())
    flow.join()
