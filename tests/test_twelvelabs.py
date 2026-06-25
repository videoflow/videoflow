import os

import pytest

from videoflow.processors.vision.twelvelabs import PegasusAnalyzer


def test_invalid_input_type():
    with pytest.raises(ValueError):
        PegasusAnalyzer('Summarize this video', input_type='bogus')


def test_open_without_api_key(monkeypatch):
    monkeypatch.delenv('TWELVELABS_API_KEY', raising=False)
    node = PegasusAnalyzer('Summarize this video')
    with pytest.raises(ValueError):
        node.open()


def test_process_wiring_no_network(monkeypatch):
    '''
    Verifies process() builds the analyze() call correctly without hitting
    the network, by injecting a fake client.
    '''
    captured = {}

    class FakeResponse:
        data = 'a short summary'

    class FakeClient:
        def analyze(self, **kwargs):
            captured.update(kwargs)
            return FakeResponse()

    node = PegasusAnalyzer('Summarize this video', model_name='pegasus1.5', max_tokens=256)
    node._client = FakeClient()

    out = node.process('https://example.com/video.mp4')

    assert out == 'a short summary'
    assert captured['model_name'] == 'pegasus1.5'
    assert captured['prompt'] == 'Summarize this video'
    assert captured['max_tokens'] == 256
    # default input_type is 'url' -> VideoContext_Url carrying the reference
    assert getattr(captured['video'], 'url', None) == 'https://example.com/video.mp4'


@pytest.mark.skipif(
    not os.environ.get('TWELVELABS_API_KEY'),
    reason='TWELVELABS_API_KEY not set; skipping live Pegasus call',
)
def test_pegasus_live():
    node = PegasusAnalyzer('In one sentence, what happens in this video?')
    node.open()
    result = node.process('https://sample-videos.com/video321/mp4/720/big_buck_bunny_720p_1mb.mp4')
    node.close()
    assert isinstance(result, str) and len(result) > 0
