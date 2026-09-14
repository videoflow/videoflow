'''
GPU fixtures for the conformance suite: which devices a gpu-level case may
touch, and an independent CUDA-runtime probe of what a mask exposes.

Gate, never provision — and never touch a device with work on it. The
``gpu`` fixture opens only when ``VF_TEST_GPU_UUIDS`` names devices, the
``cuda-python`` group is installed, ``nvidia-smi`` lists no compute process
on any of them (re-checked at teardown, so a case that leaked a process is
reported), and none of them is one of this host's own busy cards (plan §F:
lnmcltappgke02's GPUs 0 and 1 hold a vLLM service; they are refused by
identity, not by trust in the environment variable).

The probe (``_gpu_probe.py``) runs as a *subprocess* under the
``CUDA_VISIBLE_DEVICES`` under test: the CUDA runtime resolves a mask once per
process, so the parent's own view proves nothing about a child's. It reports
what the runtime enumerates (count, order, UUIDs), whether pairs have peer
access, and — in ``hold`` mode — allocates a declared number of bytes and holds
them so ``nvidia-smi --query-compute-apps`` can measure the peak independently.
'''
from __future__ import absolute_import, division, print_function

import importlib.util
import json
import os
import pathlib
import socket
import subprocess
import sys
import time
from typing import Any, Dict, List, Optional

from videoflow.backends.outcomes import Unknown
from videoflow.utils.system import host_devices_observed

HERE = pathlib.Path(__file__).resolve().parent
PROBE = HERE / '_gpu_probe.py'
FAKE_SMI = HERE / 'tools' / 'fake_nvidia_smi.py'

#: This host's cards that must never be used by a test (plan §Environment):
#: refused by UUID, whatever VF_TEST_GPU_UUIDS says.
PROTECTED_HOST = 'lnmcltappgke02'
PROTECTED_ORDINALS = (0, 1)


def compute_apps() -> Optional[Dict[str, List[int]]]:
    '''``uuid -> [pid, ...]`` of every compute process ``nvidia-smi`` reports, or None when it could not be read.'''
    try:
        out = subprocess.check_output(['nvidia-smi', '--query-compute-apps=gpu_uuid,pid,used_memory',
                                       '--format=csv,noheader,nounits'], timeout = 10, text = True)
    except (OSError, subprocess.SubprocessError):
        return None
    apps : Dict[str, List[int]] = {}
    for line in out.splitlines():
        parts = [p.strip() for p in line.split(',')]
        if len(parts) >= 2 and parts[0] and parts[1].isdigit():
            apps.setdefault(parts[0], []).append(int(parts[1]))
    return apps


def gpu_status() -> tuple[Optional[str], Dict[str, Any]]:
    '''``(reason_not_ready, facts)`` for the ``gpu`` gate; the reason names what to do.'''
    facts : Dict[str, Any] = {'host': socket.gethostname()}
    raw = os.environ.get('VF_TEST_GPU_UUIDS', '')
    uuids = [u.strip() for u in raw.split(',') if u.strip()]
    if not uuids:
        return 'VF_TEST_GPU_UUIDS unset (export the UUIDs of idle devices a test may use; never a busy one)', facts
    if importlib.util.find_spec('cuda') is None or importlib.util.find_spec('cuda.bindings') is None:
        return 'cuda-python is not installed (uv sync --group gpu-test)', facts
    observed = host_devices_observed()
    if isinstance(observed, Unknown):
        return f'nvidia-smi could not enumerate this host ({observed.reason})', facts
    by_uuid = {d.uuid: d for d in observed.value if d.uuid and d.mig_uuid is None}
    missing = [u for u in uuids if u not in by_uuid]
    if missing:
        return f'VF_TEST_GPU_UUIDS names devices this host does not have: {missing}', facts
    if facts['host'].startswith(PROTECTED_HOST):
        protected = [u for u in uuids if by_uuid[u].ordinal in PROTECTED_ORDINALS]
        if protected:
            return f'refusing {protected}: GPUs {PROTECTED_ORDINALS} of {PROTECTED_HOST} host a service', facts
    apps = compute_apps()
    if apps is None:
        return 'nvidia-smi --query-compute-apps failed; cannot prove the devices idle', facts
    busy = {u: apps[u] for u in uuids if apps.get(u)}
    if busy:
        return f'GPU busy: compute processes on {busy}', facts
    facts['uuids'] = uuids
    facts['devices'] = [{'ordinal': by_uuid[u].ordinal, 'uuid': u, 'product': by_uuid[u].product,
                         'memory_bytes': by_uuid[u].memory_bytes} for u in uuids]
    return None, facts


def busy_now(uuids : List[str]) -> Dict[str, List[int]]:
    '''Compute processes on the given devices right now (teardown re-check).'''
    apps = compute_apps() or {}
    return {u: apps[u] for u in uuids if apps.get(u)}


def run_probe(mask : Optional[str], mode : str = 'enumerate', hold_bytes : int = 0, hold_seconds : float = 0.0,
              timeout : float = 60.0, extra_env : Optional[Dict[str, str]] = None) -> Dict[str, Any]:
    '''
    The CUDA probe as a child process under ``mask`` (None = unset the
    variable). Returns its JSON report; a probe that crashed reports
    ``{'error': ...}`` rather than raising, so a case can assert on the failure.
    '''
    env = dict(os.environ)
    env.pop('CUDA_VISIBLE_DEVICES', None)
    if mask is not None:
        env['CUDA_VISIBLE_DEVICES'] = mask
    env.update(extra_env or {})
    args = [sys.executable, str(PROBE), mode, str(hold_bytes), str(hold_seconds)]
    try:
        proc = subprocess.run(args, capture_output = True, text = True, timeout = timeout, env = env, check = False)
    except subprocess.TimeoutExpired:
        return {'error': f'probe timed out after {timeout}s', 'mask': mask}
    if proc.returncode != 0 or not proc.stdout.strip():
        return {'error': f'probe exited {proc.returncode}: {proc.stderr.strip()[-500:]}', 'mask': mask}
    report = json.loads(proc.stdout.splitlines()[-1])
    report['mask'] = mask
    return report


def start_holder(mask : str, hold_bytes : int, hold_seconds : float, extra_env : Optional[Dict[str, str]] = None) -> subprocess.Popen:
    '''A probe holding ``hold_bytes`` on the masked device(s) for ``hold_seconds`` — one worker of ALLOC-016.'''
    env = dict(os.environ)
    env['CUDA_VISIBLE_DEVICES'] = mask
    env.update(extra_env or {})
    return subprocess.Popen([sys.executable, str(PROBE), 'hold', str(hold_bytes), str(hold_seconds)],
                            stdout = subprocess.PIPE, stderr = subprocess.PIPE, text = True, env = env)


def wait_for_processes(uuids : List[str], expected : int, timeout : float = 30.0) -> Dict[str, List[int]]:
    '''Polls ``nvidia-smi`` until ``expected`` compute processes sit on the devices (or the timeout passes).'''
    deadline = time.monotonic() + timeout
    seen : Dict[str, List[int]] = {}
    while time.monotonic() < deadline:
        seen = busy_now(uuids)
        if sum(len(v) for v in seen.values()) >= expected:
            return seen
        time.sleep(0.5)
    return seen


def measured_memory(uuids : List[str]) -> Dict[int, int]:
    '''``pid -> bytes`` in use on the devices, from ``nvidia-smi --query-compute-apps`` (independent of the holder).'''
    try:
        out = subprocess.check_output(['nvidia-smi', '--query-compute-apps=gpu_uuid,pid,used_memory',
                                       '--format=csv,noheader,nounits'], timeout = 10, text = True)
    except (OSError, subprocess.SubprocessError):
        return {}
    used : Dict[int, int] = {}
    for line in out.splitlines():
        parts = [p.strip() for p in line.split(',')]
        if len(parts) >= 3 and parts[0] in uuids and parts[1].isdigit() and parts[2].isdigit():
            used[int(parts[1])] = used.get(int(parts[1]), 0) + int(parts[2]) * 1024 * 1024
    return used
