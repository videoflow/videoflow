'''
Glue nodes for the toy-fusion solution.

These live in their own importable module (not in the graph module) because
distributed workers reconstruct each node from its class path — recorded as
``toy_fusion_nodes.<Class>`` — and importing the graph module in a worker would
re-run graph-level code. Every constructor argument is stored as
``self._<name>`` so the node round-trips through ``get_params()`` unchanged.

The simulated sources stand in for real hardware (fixed-rate cameras and a
high-rate IMU) with none of the I/O: what this solution exercises is the
**time-aligned join** — independent producers with no shared lineage, grouped
by the event timestamps they stamp on their own messages.
'''
from __future__ import annotations

import json
import math
import os
import time
from typing import Any

from videoflow.core.context import RuntimeContext
from videoflow.core.node import ConsumerNode, OneTaskProcessorNode, ProducerNode


def _aligned_start() -> float:
    '''
    The producer's tick-grid origin: the next-but-one whole epoch second.

    Every simulated source anchors its timeline here rather than at its own
    ``open()`` instant, because the workers open seconds apart (subprocess or
    pod startup) and a time-mode join can only group event timestamps that
    actually coincide. Aligning to whole epoch seconds puts every producer on
    one shared grid — the simulation stand-in for the synchronized capture
    clocks (PTP, genlock) that real multi-camera rigs rely on. A late-starting
    source simply joins the grid at a later second, and until then the others'
    moments emit at quorum — exactly what happens when a real camera comes
    online late.
    '''
    return float(math.ceil(time.time()) + 1)


class SimCameraProducer(ProducerNode):
    '''
    Simulated fixed-rate camera. Emits one small "frame summary" dict per tick
    and stamps each message with the tick's ideal capture time via
    ``ctx.set_event_timestamp`` — the value time-mode joins group on. A
    per-camera phase offset of a few milliseconds keeps the cameras genuinely
    unsynchronized, so the fusion tolerance is doing real work.

    With ``duration_s`` unset it never stops (``is_finite=False`` — a live
    source, deployed as a long-running Deployment on Kubernetes). With it set,
    ``next()`` raises ``StopIteration`` after that many seconds of stream time,
    which is what lets a smoke run finish on its own.
    '''

    def __init__(self, fps: float = 10.0, phase_ms: float = 0.0,
                 duration_s: float | None = None, **kwargs: Any) -> None:
        self._fps = fps
        self._phase_ms = phase_ms
        self._duration_s = duration_s
        self._t0: float | None = None
        self._n = 0
        # is_finite is derived from duration_s; pop it so reconstruction via
        # get_params() doesn't pass it twice.
        kwargs.pop('is_finite', None)
        super().__init__(is_finite=duration_s is not None, **kwargs)

    def open(self) -> None:
        self._t0 = _aligned_start()

    def next(self, ctx: RuntimeContext | None = None) -> dict:
        if self._duration_s is not None and self._n / self._fps >= self._duration_s:
            raise StopIteration()
        assert self._t0 is not None
        t = self._t0 + self._phase_ms / 1000.0 + self._n / self._fps
        # Pace to wall clock so several producers interleave in real time.
        now = time.time()
        if t > now:
            time.sleep(t - now)
        self._n += 1
        if ctx is not None:
            ctx.set_event_timestamp(t)
        return {
            'frame_idx': self._n - 1,
            'brightness': round(50.0 + 40.0 * math.sin(self._n / 20.0), 3),
        }


class SimSensorProducer(ProducerNode):
    '''
    Simulated high-rate sensor (an IMU running an order of magnitude faster
    than the cameras). It is meant to be a *collect* parent of the fusion node:
    it never gates a join group; instead every sample near a group's moment is
    delivered to ``process()`` as a list.
    '''

    def __init__(self, hz: float = 100.0, duration_s: float | None = None, **kwargs: Any) -> None:
        self._hz = hz
        self._duration_s = duration_s
        self._t0: float | None = None
        self._n = 0
        # is_finite is derived from duration_s; pop it so reconstruction via
        # get_params() doesn't pass it twice.
        kwargs.pop('is_finite', None)
        super().__init__(is_finite=duration_s is not None, **kwargs)

    def open(self) -> None:
        self._t0 = _aligned_start()

    def next(self, ctx: RuntimeContext | None = None) -> dict:
        if self._duration_s is not None and self._n / self._hz >= self._duration_s:
            raise StopIteration()
        assert self._t0 is not None
        t = self._t0 + self._n / self._hz
        now = time.time()
        if t > now:
            time.sleep(t - now)
        self._n += 1
        if ctx is not None:
            ctx.set_event_timestamp(t)
        return {'sample': self._n - 1, 'accel': round(math.sin(self._n / 10.0), 4)}


class MomentFusion(OneTaskProcessorNode):
    '''
    Fuses one synchronized "moment": the camera summaries whose event times
    fall within the join tolerance, plus every sensor sample the collect window
    gathered around them. Quorum emissions pass missing cameras as ``None``;
    ``ctx.input_info`` supplies each input's exact event time, which is how the
    spread is measured without changing any payload.

    Stateful (it numbers the moments), hence a ``OneTaskProcessorNode`` — and a
    multi-parent join must be single-replica anyway unless it is partitioned.

    - Arguments:
        - camera_names: the camera parents' node names, in the same order the \
            cameras are wired into this node.
        - sensor_name: the collect parent's node name.
    '''

    def __init__(self, camera_names: list[str], sensor_name: str, **kwargs: Any) -> None:
        self._camera_names = list(camera_names)
        self._sensor_name = sensor_name
        self._moment = 0
        super().__init__(**kwargs)

    # One positional argument per parent (cameras first, then the sensor's
    # collected list), so the signature can't match the single-input base method.
    def process(self, *inputs: Any, ctx: RuntimeContext | None = None) -> dict:  # type: ignore[override]
        cameras = list(inputs[:len(self._camera_names)])
        samples = inputs[len(self._camera_names)] or []
        info = ctx.input_info if ctx is not None else None

        views = [name for name, frame in zip(self._camera_names, cameras) if frame is not None]
        times = []
        if info is not None:
            times = [info[name]['event_ts'] for name in views
                     if info.get(name) is not None and info[name]['event_ts'] is not None]
        brightness = [frame['brightness'] for frame in cameras if frame is not None]
        accels = [sample['accel'] for sample in samples]

        self._moment += 1
        return {
            'moment': self._moment,
            'views': len(views),
            'expected_views': len(self._camera_names),
            'spread_ms': round((max(times) - min(times)) * 1000.0, 3) if len(times) > 1 else 0.0,
            'brightness_mean': round(sum(brightness) / len(brightness), 3) if brightness else None,
            'sensor_samples': len(accels),
            'accel_mean': round(sum(accels) / len(accels), 4) if accels else None,
        }


class LiveStateWriter(ConsumerNode):
    '''
    A REALTIME-shaped sink: atomically rewrites ``latest.json`` on every fused
    moment, so an operator (or a verifier) can watch the file's content and
    mtime advance while the flow runs. ``close()`` — reached only when the
    sources are bounded or the flow is stopped — writes a run summary.
    '''

    def __init__(self, latest_path: str, summary_path: str, **kwargs: Any) -> None:
        self._latest_path = latest_path
        self._summary_path = summary_path
        self._moments = 0
        self._complete = 0
        self._started: float | None = None
        super().__init__(**kwargs)

    def open(self) -> None:
        self._started = time.time()

    def consume(self, moment: dict) -> None:
        self._moments += 1
        if moment['views'] == moment['expected_views']:
            self._complete += 1
        self._write(self._latest_path, {**moment, 'updated_unix': round(time.time(), 3)})

    def close(self) -> None:
        elapsed = time.time() - self._started if self._started is not None else None
        self._write(self._summary_path, {
            'moments': self._moments,
            'complete_moments': self._complete,
            'quorum_moments': self._moments - self._complete,
            'elapsed_s': round(elapsed, 3) if elapsed is not None else None,
        })

    def _write(self, path: str, payload: dict) -> None:
        # Atomic replace: a reader polling the file never sees a half-written JSON.
        tmp = path + '.tmp'
        with open(tmp, 'w') as f:
            json.dump(payload, f, indent=2)
        os.replace(tmp, path)
