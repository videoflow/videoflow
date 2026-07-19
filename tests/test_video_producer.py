'''
Lifecycle tests for the cv2-backed video producers.

``__init__`` runs on the machine that builds the graph and does no I/O; the
capture object is only created in ``open()``, which runs in the worker. These
tests pin the contract for the window in between.
'''
from __future__ import absolute_import, division, print_function

import pytest

pytest.importorskip('cv2')   # optional 'vision'/'video' extra

from videoflow.producers.video import VideoFileReader


def test_next_before_open_raises_runtime_error():
    '''
    ``next()`` before ``open()`` is lifecycle misuse, not a crash: ``_video`` is
    still None, and reading it used to surface as an opaque
    ``AttributeError: 'NoneType' object has no attribute 'isOpened'``.
    '''
    reader = VideoFileReader('/nonexistent/video.mp4')
    with pytest.raises(RuntimeError, match = r'open\(\)'):
        reader.next()


def test_close_before_open_is_a_noop():
    '''``close()`` must tolerate a node that was never opened (teardown after a
    failed open runs unconditionally).'''
    VideoFileReader('/nonexistent/video.mp4').close()


if __name__ == '__main__':
    pytest.main([__file__])
