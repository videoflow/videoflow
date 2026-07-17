import os

import pytest

from videoflow.utils import downloader
from videoflow.utils.downloader import get_file


def test_get_file_single_origin_backward_compat(tmp_path, monkeypatch):
    '''A plain string origin still works exactly as before.'''
    calls = []

    def fake_urlretrieve(url, filename, hook=None):
        calls.append(url)
        with open(filename, 'wb') as f:
            f.write(b'ok')

    monkeypatch.setattr(downloader, 'urlretrieve', fake_urlretrieve)
    path = get_file('w.bin', 'http://example.com/w.bin', cache_dir=str(tmp_path))
    assert os.path.exists(path)
    assert calls == ['http://example.com/w.bin']


def test_get_file_falls_back_to_second_url(tmp_path, monkeypatch):
    '''A list origin tries URLs in order; the first success wins.'''
    calls = []

    def fake_urlretrieve(url, filename, hook=None):
        calls.append(url)
        if 'primary' in url:
            raise Exception('boom')
        with open(filename, 'wb') as f:
            f.write(b'mirror-bytes')

    monkeypatch.setattr(downloader, 'urlretrieve', fake_urlretrieve)
    path = get_file('w.bin', ['http://primary/w.bin', 'http://mirror/w.bin'],
                    cache_dir=str(tmp_path))
    assert os.path.exists(path)
    assert calls == ['http://primary/w.bin', 'http://mirror/w.bin']
    with open(path, 'rb') as f:
        assert f.read() == b'mirror-bytes'


def test_get_file_raises_when_all_urls_fail(tmp_path, monkeypatch):
    '''If every candidate URL fails, get_file raises and leaves no partial file.'''
    def fake_urlretrieve(url, filename, hook=None):
        raise Exception('boom ' + url)

    monkeypatch.setattr(downloader, 'urlretrieve', fake_urlretrieve)
    with pytest.raises(Exception):
        get_file('w.bin', ['http://a/w.bin', 'http://b/w.bin'], cache_dir=str(tmp_path))
    assert not os.path.exists(os.path.join(str(tmp_path), 'models', 'w.bin'))


def test_get_file_uses_cache_and_skips_download(tmp_path, monkeypatch):
    '''An already-cached file is not re-downloaded.'''
    calls = []

    def fake_urlretrieve(url, filename, hook=None):
        calls.append(url)
        with open(filename, 'wb') as f:
            f.write(b'ok')

    monkeypatch.setattr(downloader, 'urlretrieve', fake_urlretrieve)
    get_file('w.bin', ['http://a/w.bin'], cache_dir=str(tmp_path))
    get_file('w.bin', ['http://a/w.bin'], cache_dir=str(tmp_path))
    assert calls == ['http://a/w.bin']  # downloaded once
