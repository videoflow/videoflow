'''
Analyze videos with the TwelveLabs Pegasus model.

This flow emits video URLs, sends each one to Pegasus for a prompt-based
analysis, and prints the generated text to the command line.

Requirements:
    pip install videoflow[twelvelabs]
    export TWELVELABS_API_KEY=<your key>   # free key at https://twelvelabs.io

Run:
    python examples/twelvelabs_pegasus.py
'''
from videoflow.core import Flow
from videoflow.core.node import ProducerNode
from videoflow.processors.vision.twelvelabs import PegasusAnalyzer
from videoflow.consumers import CommandlineConsumer


class UrlProducer(ProducerNode):
    '''
    Produces a fixed list of video URLs, one per ``next()`` call.
    '''
    def __init__(self, urls, *args, **kwargs):
        self._urls = list(urls)
        self._idx = 0
        super(UrlProducer, self).__init__(*args, **kwargs)

    def next(self):
        if self._idx >= len(self._urls):
            raise StopIteration()
        url = self._urls[self._idx]
        self._idx += 1
        return url


urls = [
    'https://commondatastorage.googleapis.com/gtv-videos-bucket/sample/ForBiggerBlazes.mp4',
]

producer = UrlProducer(urls)
analyzer = PegasusAnalyzer('Summarize this video in one sentence.')(producer)
printer = CommandlineConsumer()(analyzer)

flow = Flow([producer], [printer])
flow.run()
flow.join()
