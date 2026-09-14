'''
Plumbing for the benchmark-level cases (MSG-026, PAY-017, PAY-022, ALLOC-032).

A benchmark passes only against thresholds *the operator supplied* — the
catalog is explicit that no case may invent a throughput or camera-count
target — so the ``bench`` gate reads ``VF_BENCH_THRESHOLDS_JSON`` (a path to a
JSON file, or the JSON inline; ``bench/thresholds.example.json`` is the shape)
and a case whose entry is absent is NOT_RUN. What a benchmark measured is always
written as evidence, thresholds or not; the verdict is the comparison.

The environment manifest records what the numbers were measured on (host, CPU,
memory, Python, broker and store versions, GPU driver), because a measurement
without its environment is not reproducible.
'''
from __future__ import absolute_import, division, print_function

import json
import os
import pathlib
import platform
import statistics
import subprocess
import time
import urllib.request
from typing import Any, Dict, List, Optional

THRESHOLDS_ENV = 'VF_BENCH_THRESHOLDS_JSON'
MONITOR_ENV = 'VF_TEST_NATS_MONITOR_URL'
EXAMPLE = pathlib.Path(__file__).parent / 'bench' / 'thresholds.example.json'


def load_thresholds() -> tuple[Optional[str], Dict[str, Any]]:
    '''``(reason_not_ready, thresholds)``: the whole document keyed by case id.'''
    raw = os.environ.get(THRESHOLDS_ENV, '').strip()
    if not raw:
        return (f'{THRESHOLDS_ENV} unset (a benchmark passes only against your own SLOs: copy '
                f'{EXAMPLE.relative_to(EXAMPLE.parents[2])}, edit it, export the path)'), {}
    try:
        text = pathlib.Path(raw).read_text() if not raw.lstrip().startswith('{') else raw
        document = json.loads(text)
    except (OSError, ValueError) as e:
        return f'{THRESHOLDS_ENV} could not be read as JSON ({e})', {}
    if not isinstance(document, dict):
        return f'{THRESHOLDS_ENV} must be a JSON object keyed by case id', {}
    return None, document


def thresholds_for(case_id : str) -> tuple[Optional[str], Dict[str, Any]]:
    reason, document = load_thresholds()
    if reason is not None:
        return reason, {}
    entry = document.get(case_id)
    if not isinstance(entry, dict):
        return f'{THRESHOLDS_ENV} has no thresholds for {case_id} (see bench/thresholds.example.json)', {}
    return None, entry


def monitor_url() -> str:
    return os.environ.get(MONITOR_ENV, 'http://localhost:8222').rstrip('/')


def jsz(timeout : float = 5.0) -> Optional[Dict[str, Any]]:
    '''The broker's ``/jsz`` (streams, consumers, messages, bytes, memory, storage), or None when unreachable.'''
    try:
        with urllib.request.urlopen(f'{monitor_url()}/jsz', timeout = timeout) as response:
            return json.loads(response.read().decode('utf-8'))
    except Exception:  # noqa: BLE001 — the monitoring port is optional evidence
        return None


def varz(timeout : float = 5.0) -> Optional[Dict[str, Any]]:
    '''The broker's ``/varz`` (version, mem, cpu, connections), or None.'''
    try:
        with urllib.request.urlopen(f'{monitor_url()}/varz', timeout = timeout) as response:
            return json.loads(response.read().decode('utf-8'))
    except Exception:  # noqa: BLE001
        return None


def broker_resources() -> Dict[str, Any]:
    '''One broker resource sample: stream/consumer counts and process memory/CPU.'''
    js = jsz() or {}
    v = varz() or {}
    return {'at': time.time(), 'streams': js.get('streams'), 'consumers': js.get('consumers'),
            'messages': js.get('messages'), 'bytes': js.get('bytes'), 'mem': v.get('mem'), 'cpu': v.get('cpu'),
            'connections': v.get('connections')}


def _command(args : List[str]) -> str:
    try:
        return subprocess.check_output(args, text = True, timeout = 10, stderr = subprocess.DEVNULL).strip()
    except (OSError, subprocess.SubprocessError):
        return ''


def environment_manifest(extra : Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    '''What the numbers were measured on.'''
    mem = ''
    try:
        with open('/proc/meminfo') as f:
            mem = next((line.split()[1] + ' kB' for line in f if line.startswith('MemTotal')), '')
    except OSError:
        pass
    manifest = {
        'host': platform.node(), 'platform': platform.platform(), 'python': platform.python_version(),
        'cpu_count': os.cpu_count(), 'memory_total': mem,
        'nats_server': (varz() or {}).get('version'),
        'gpu_driver': _command(['nvidia-smi', '--query-gpu=driver_version', '--format=csv,noheader']).splitlines()[:1],
        'commit': _command(['git', 'rev-parse', 'HEAD']),
        'measured_at': time.strftime('%Y-%m-%dT%H:%M:%S%z'),
    }
    manifest.update(extra or {})
    return manifest


def percentile(values : List[float], p : float) -> float:
    if not values:
        return float('nan')
    ordered = sorted(values)
    k = max(0, min(len(ordered) - 1, int(round(p / 100.0 * (len(ordered) - 1)))))
    return ordered[k]


def summary(values : List[float]) -> Dict[str, float]:
    if not values:
        return {'count': 0}
    return {'count': len(values), 'min': min(values), 'max': max(values), 'mean': statistics.fmean(values),
            'p50': percentile(values, 50), 'p95': percentile(values, 95), 'p99': percentile(values, 99)}


class Verdict:
    '''Threshold comparisons collected as records; ``failed`` lists the ones that did not hold.'''
    def __init__(self) -> None:
        self.checks : List[Dict[str, Any]] = []

    def check(self, name : str, value : Any, holds : bool, threshold : Any) -> None:
        self.checks.append({'name': name, 'value': value, 'threshold': threshold, 'holds': bool(holds)})

    @property
    def failed(self) -> List[Dict[str, Any]]:
        return [c for c in self.checks if not c['holds']]
