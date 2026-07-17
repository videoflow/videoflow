'''
Sending flow output to a file with FileAppenderConsumer, fed by a custom
processor. FileAppenderConsumer appends one line per received item to the given
path (creating the file if needed). Once the flow finishes we read the file back
to show what landed there.

The custom SquareLineProcessor lives in examples/example_nodes.py (see the note
there on why custom nodes must be in an importable module); this script puts that
directory on PYTHONPATH so the spawned workers can import it.

    python examples/file_output.py
'''
import os
import sys
import tempfile

_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, _HERE)
os.environ['PYTHONPATH'] = _HERE + os.pathsep + os.environ.get('PYTHONPATH', '')

from example_nodes import SquareLineProcessor  # noqa: E402

from videoflow.consumers import FileAppenderConsumer  # noqa: E402
from videoflow.core import Flow  # noqa: E402
from videoflow.core.constants import BATCH  # noqa: E402
from videoflow.producers import IntProducer  # noqa: E402


def build_flow(filepath : str):
    numbers = IntProducer(1, 10, 0.02, name = 'numbers')
    squared = SquareLineProcessor(name = 'square')(numbers)
    writer = FileAppenderConsumer(filepath = filepath, name = 'writer')(squared)
    return Flow([writer], flow_type = BATCH)


if __name__ == '__main__':
    from videoflow.engines.local import LocalProcessEngine
    out = os.path.join(tempfile.gettempdir(), 'videoflow_squares.txt')
    # Start clean: the consumer appends, so a stale file would accumulate.
    if os.path.exists(out):
        os.remove(out)

    flow = build_flow(out)
    flow.run(LocalProcessEngine())
    flow.join()

    print(f'\n--- contents of {out} ---')
    with open(out) as f:
        print(f.read(), end = '')
