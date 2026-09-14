'''
Host-side MIG plumbing for the gpu-level allocation cases (ALLOC-002, ALLOC-032):
``nvidia-smi mig`` queries parsed into plain records, and the privileged
operations — MIG mode on/off, instance create/destroy — behind one ``sudo -n``
wrapper so a case can tell "no privilege" (NOT_RUN) from a refusal by the driver
(evidence).

Independence: nothing here consults ``videoflow.deploy.mig``. The placement query
(``-lgipp``) is the vendor's own answer about what the card can hold, which is what
makes it an oracle for the solver; the instance listing (``-lgi``) is what proves a
layout was instantiated exactly. Every command that can change a card is recorded
in a timeline so a case can show that a rejected layout changed nothing.
'''
from __future__ import absolute_import, division, print_function

import re
import subprocess
import time
from typing import Any, Dict, List, Optional

_PLACEMENT_RE = re.compile(r'GPU\s+(\d+)\s+Profile ID\s+(\d+)\s+Placements?\s*:\s*\{([\d,\s]*)\}:(\d+)')
_PROFILE_RE = re.compile(r'\|\s+(\d+)\s+MIG\s+(\S+)\s+(\d+)\s+(\d+)/(\d+)\s+([\d.]+)')
_INSTANCE_RE = re.compile(r'\|\s+(\d+)\s+MIG\s+(\S+)\s+(\d+)\s+(\d+)\s+(\d+):(\d+)\s+\|')


class Timeline:
    '''Every nvidia-smi invocation a case made, with its outcome — the mutation record.'''
    def __init__(self) -> None:
        self.entries : List[Dict[str, Any]] = []

    def record(self, args : List[str], rc : int, out : str, privileged : bool) -> None:
        self.entries.append({'at': time.time(), 'command': ' '.join(args), 'privileged': privileged,
                             'exit_code': rc, 'output': out.strip()[-400:]})

    def mutations(self) -> List[Dict[str, Any]]:
        return [e for e in self.entries if e['privileged']]


def smi(*args : str, timeline : Optional[Timeline] = None, timeout : float = 60.0) -> tuple[int, str]:
    '''An unprivileged ``nvidia-smi`` query: ``(exit code, stdout+stderr)``.'''
    cmd = ['nvidia-smi', *args]
    try:
        proc = subprocess.run(cmd, capture_output = True, text = True, timeout = timeout, check = False)
    except (OSError, subprocess.SubprocessError) as e:
        if timeline is not None:
            timeline.record(cmd, 1, str(e), False)
        return 1, str(e)
    out = proc.stdout + proc.stderr
    if timeline is not None:
        timeline.record(cmd, proc.returncode, out, False)
    return proc.returncode, out


def sudo_available() -> Optional[str]:
    '''None when ``sudo -n`` works here, else the reason MIG mutations cannot run.'''
    try:
        proc = subprocess.run(['sudo', '-n', 'true'], capture_output = True, text = True, timeout = 10, check = False)
    except (OSError, subprocess.SubprocessError) as e:
        return f'sudo unavailable ({e})'
    if proc.returncode != 0:
        return f'passwordless sudo is not available ({proc.stderr.strip() or "denied"}); nvidia-smi mig -cgi needs root'
    return None


def device_holders(index : int) -> List[Dict[str, Any]]:
    '''
    Processes holding ``/dev/nvidia<index>`` open (``fuser``, privileged): a
    driver client that runs no compute kernels still blocks instance creation
    ("In use by another client"), and is never ours to kill — a container that
    was started with every device visible holds them all.
    '''
    try:
        proc = subprocess.run(['sudo', '-n', 'fuser', f'/dev/nvidia{index}'], capture_output = True, text = True,
                              timeout = 20, check = False)
    except (OSError, subprocess.SubprocessError):
        return []
    holders = []
    for token in proc.stdout.split():
        pid = ''.join(ch for ch in token if ch.isdigit())
        if not pid:
            continue
        try:
            comm = open(f'/proc/{pid}/comm').read().strip()
        except OSError:
            comm = '?'
        holders.append({'pid': int(pid), 'command': comm})
    return holders


def sudo_smi(*args : str, timeline : Optional[Timeline] = None, timeout : float = 120.0) -> tuple[int, str]:
    '''A privileged ``nvidia-smi`` operation through ``sudo -n``; always recorded as a mutation.'''
    cmd = ['sudo', '-n', 'nvidia-smi', *args]
    try:
        proc = subprocess.run(cmd, capture_output = True, text = True, timeout = timeout, check = False)
    except (OSError, subprocess.SubprocessError) as e:
        if timeline is not None:
            timeline.record(cmd, 1, str(e), True)
        return 1, str(e)
    out = proc.stdout + proc.stderr
    if timeline is not None:
        timeline.record(cmd, proc.returncode, out, True)
    return proc.returncode, out


def placements_query(index : int, timeline : Optional[Timeline] = None) -> Dict[int, Dict[str, Any]]:
    '''``nvidia-smi mig -lgipp -i N`` parsed: profile id -> {starts, width}. Works with MIG mode off.'''
    rc, out = smi('mig', '-lgipp', '-i', str(index), timeline = timeline)
    if rc != 0:
        raise RuntimeError(f'nvidia-smi mig -lgipp failed: {out.strip()[-300:]}')
    result : Dict[int, Dict[str, Any]] = {}
    for m in _PLACEMENT_RE.finditer(out):
        starts = tuple(int(s) for s in m.group(3).replace(' ', '').split(',') if s)
        result[int(m.group(2))] = {'starts': starts, 'width': int(m.group(4))}
    return result


def profiles_query(index : int, timeline : Optional[Timeline] = None) -> Dict[str, Dict[str, Any]]:
    '''``nvidia-smi mig -lgip -i N`` parsed: profile name -> {id, free, total, memory_gib}.'''
    rc, out = smi('mig', '-lgip', '-i', str(index), timeline = timeline)
    if rc != 0:
        raise RuntimeError(f'nvidia-smi mig -lgip failed: {out.strip()[-300:]}')
    result : Dict[str, Dict[str, Any]] = {}
    for m in _PROFILE_RE.finditer(out):
        result[m.group(2)] = {'id': int(m.group(3)), 'free': int(m.group(4)), 'total': int(m.group(5)),
                              'memory_gib': float(m.group(6))}
    return result


def instances_query(index : int, timeline : Optional[Timeline] = None) -> List[Dict[str, Any]]:
    '''``nvidia-smi mig -lgi -i N`` parsed: one record per GPU instance (name, profile id, instance id, start, width).'''
    rc, out = smi('mig', '-lgi', '-i', str(index), timeline = timeline)
    if rc != 0:
        if 'No GPU instances found' in out or 'Not Found' in out:
            return []
        raise RuntimeError(f'nvidia-smi mig -lgi failed: {out.strip()[-300:]}')
    return [{'name': m.group(2), 'profile_id': int(m.group(3)), 'instance_id': int(m.group(4)),
             'start': int(m.group(5)), 'width': int(m.group(6))} for m in _INSTANCE_RE.finditer(out)]


def mig_mode(index : int) -> tuple[str, str]:
    '''``(current, pending)`` MIG mode of the device, as ``Enabled``/``Disabled``.'''
    rc, out = smi('--query-gpu=mig.mode.current,mig.mode.pending', '--format=csv,noheader', '-i', str(index))
    if rc != 0:
        raise RuntimeError(f'nvidia-smi query failed: {out.strip()[-300:]}')
    parts = [p.strip() for p in out.strip().split(',')]
    return (parts[0], parts[1] if len(parts) > 1 else parts[0])


def set_mig_mode(index : int, enabled : bool, timeline : Optional[Timeline] = None, timeout : float = 120.0) -> str:
    '''
    Switch MIG mode and wait until ``current`` reports it; a device that answers
    "pending" is reset (``--gpu-reset``, which needs the device idle) so the change
    takes effect now. Returns the current mode.
    '''
    want = 'Enabled' if enabled else 'Disabled'
    current, _pending = mig_mode(index)
    if current == want:
        return current
    rc, out = sudo_smi('-i', str(index), '-mig', '1' if enabled else '0', timeline = timeline)
    if rc != 0:
        raise RuntimeError(f'nvidia-smi -mig {int(enabled)} failed: {out.strip()[-300:]}')
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        current, pending = mig_mode(index)
        if current == want:
            return current
        if pending == want:
            rc, out = sudo_smi('--gpu-reset', '-i', str(index), timeline = timeline)
            if rc != 0:
                raise RuntimeError(f'MIG mode is pending and the device would not reset: {out.strip()[-300:]}')
        time.sleep(2)
    raise RuntimeError(f'MIG mode did not become {want} within {timeout}s (current {current})')


def create_instances(index : int, profile_ids : List[int], timeline : Optional[Timeline] = None) -> tuple[int, str]:
    '''``nvidia-smi mig -cgi <ids> -C -i N``: GPU instances plus one compute instance each.'''
    return sudo_smi('mig', '-cgi', ','.join(str(i) for i in profile_ids), '-C', '-i', str(index), timeline = timeline)


def destroy_instances(index : int, timeline : Optional[Timeline] = None) -> None:
    '''Destroy every compute and GPU instance on the device (idempotent; a bare device is fine).'''
    sudo_smi('mig', '-dci', '-i', str(index), timeline = timeline)
    sudo_smi('mig', '-dgi', '-i', str(index), timeline = timeline)
