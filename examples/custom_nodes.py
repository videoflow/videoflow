'''
Writing your own nodes. videoflow ships a handful of generic producers,
processors and consumers, but the real power is subclassing the three base node
types to plug in your own logic:

  - ProducerNode.next()          -> emit the next item (raise StopIteration to end)
  - ProcessorNode.process(inp)   -> transform an item
  - ConsumerNode.consume(item)   -> sink an item (a side effect)

Every node also has open()/close() lifecycle hooks. They run once inside the
worker process -- open() before the first item, close() after the last -- and
are where you set up and tear down resources (files, sockets, model sessions).

The node classes live in examples/example_nodes.py rather than in this script,
because each node is reconstructed inside its own worker process by importing
its class -- which only works for classes in a real, importable module (see the
note in example_nodes.py). This script puts that module's directory on PYTHONPATH
so the spawned workers can import it.

    python examples/custom_nodes.py
'''
import os
import sys

# Make examples/ importable by the worker subprocesses (they inherit os.environ).
_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, _HERE)
os.environ['PYTHONPATH'] = _HERE + os.pathsep + os.environ.get('PYTHONPATH', '')

from example_nodes import PrefixConsumer, SentenceProducer, TitleCaseProcessor  # noqa: E402

from videoflow.core import Flow  # noqa: E402
from videoflow.core.constants import BATCH  # noqa: E402


def build_flow():
    producer = SentenceProducer('the quick brown fox jumps over the lazy dog', name = 'sentence')
    titled = TitleCaseProcessor(name = 'titlecase')(producer)
    printer = PrefixConsumer(prefix = 'word:', name = 'printer')(titled)
    return Flow([printer], flow_type = BATCH)


if __name__ == '__main__':
    from videoflow.engines.local import LocalProcessEngine
    flow = build_flow()
    flow.run(LocalProcessEngine())
    flow.join()
