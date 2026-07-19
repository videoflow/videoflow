'''
Custom node classes shared by the example scripts.

Why a separate importable module instead of defining these inside each script?
A videoflow flow runs distributed: the engine starts one worker process per node,
and each worker reconstructs its node by importing the node's class by its
fully-qualified path (module + class name). Classes defined in a script's
``__main__`` have the path ``__main__.<Class>``, which no *other* process can
import. So any custom node has to live in a real, importable module -- exactly
how you would ship nodes in your own package. The example scripts put this
module's directory on ``sys.path``; the local engine re-exports those additions
to each worker's PYTHONPATH, so the subprocesses can import it.
'''
import os

from videoflow.core.node import ConsumerNode, ProcessorNode, ProducerNode


class SentenceProducer(ProducerNode):
    '''Emits the words of a sentence, one at a time (finite -> ends the stream).'''
    def __init__(self, sentence : str, **kwargs) -> None:
        self._sentence = sentence  # stored as _sentence so get_params() recovers it
        super().__init__(**kwargs)

    def open(self) -> None:
        # Derived, per-run state: build it here, not in __init__.
        self._words = self._sentence.split()
        self._idx = 0

    def next(self) -> str:
        if self._idx >= len(self._words):
            raise StopIteration()
        word = self._words[self._idx]
        self._idx += 1
        return word


class TitleCaseProcessor(ProcessorNode):
    '''Capitalizes each incoming word. No constructor args -> nothing to capture.'''
    def process(self, inp : str) -> str:
        return inp.capitalize()


class PrefixConsumer(ConsumerNode):
    '''Prints each item behind a fixed prefix.'''
    def __init__(self, prefix : str = '>>', **kwargs) -> None:
        self._prefix = prefix
        super().__init__(**kwargs)

    def consume(self, item : str) -> None:
        print(f'{self._prefix} {item}')


class SquareLineProcessor(ProcessorNode):
    '''Formats each integer as a "n -> n^2" line of text.'''
    def process(self, inp : int) -> str:
        return f'{inp} -> {inp * inp}'


class ReplicaTagProcessor(ProcessorNode):
    '''Echoes each integer, tagged with the replica index that handled it.'''
    def open(self) -> None:
        self._replica = os.environ.get('VF_REPLICA_ID', '?')

    def process(self, inp : int) -> str:
        return f'n={inp:>2} handled by replica {self._replica}'
