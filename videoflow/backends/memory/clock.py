'''
A controllable clock for deterministic models: monotonic and wall time advance
together, only when asked. Timers fire on ``advance``.
'''
from __future__ import absolute_import, division, print_function

import heapq
import itertools
from typing import Callable


class FakeClock:
    def __init__(self, start : float = 1_000.0, wall_start : float = 1_700_000_000.0) -> None:
        self._mono = start
        self._wall = wall_start
        self._timers : list[tuple[float, int, Callable[[], None]]] = []
        self._ids = itertools.count()

    def monotonic(self) -> float:
        return self._mono

    def now(self) -> float:
        return self._mono

    def time(self) -> float:
        return self._wall

    def __call__(self) -> float:
        return self._mono

    def at(self, seconds_from_now : float, callback : Callable[[], None]) -> None:
        heapq.heappush(self._timers, (self._mono + seconds_from_now, next(self._ids), callback))

    def advance(self, seconds : float) -> None:
        '''Move time forward, firing every timer due on the way in order.'''
        if seconds < 0:
            raise ValueError('a clock only advances')
        target = self._mono + seconds
        while self._timers and self._timers[0][0] <= target:
            due, _, callback = heapq.heappop(self._timers)
            self._wall += due - self._mono
            self._mono = due
            callback()
        self._wall += target - self._mono
        self._mono = target
