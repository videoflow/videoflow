#!/usr/bin/env python3
'''
A ``nvidia-smi`` stand-in for process-level cases (ALLOC-014, RUN-039, RUN-044):
put this file's directory first on PATH (as ``nvidia-smi``) and describe the
host in ``VF_FAKE_SMI_JSON`` — a list of ``{"index", "uuid", "name",
"memory_total_mib", "memory_used_mib", "mig": [{"uuid", "profile"}]}`` — so a
launcher and its children see one, zero or many GPUs without any driver.
``VF_FAKE_SMI_FAIL=1`` exits non-zero (a wedged driver); ``VF_FAKE_SMI_HANG=1``
sleeps past the caller's timeout. ``VF_FAKE_SMI_P2P`` lists ``"i-j"`` pairs with
peer access for ``topo -p2p r``.

Running this file is the slow path: one interpreter start per read, and the
production code reads two or three times per operation. The ``fake_smi``
fixture (``conftest.py``) therefore pre-renders the answers to every argv in
``KNOWN_QUERIES`` with ``prerender`` and installs an ``sh`` shim that ``cat``s
them, falling back to this file for any argv it has not seen — the same PATH,
argv and stdout contract at a twentieth of the cost.
'''
import json
import os
import re
import sys
import time
from typing import Any, Dict, List, Sequence, Tuple

#: Every argv the production code issues; ``prerender`` answers these ahead of time.
KNOWN_QUERIES : Tuple[Tuple[str, ...], ...] = (
    ('-L',),
    ('topo', '-p2p', 'r'),
    ('--query-gpu=index,uuid,name,memory.total,memory.used', '--format=csv,noheader,nounits'),
    ('--query-compute-apps=gpu_uuid,pid,used_memory', '--format=csv,noheader,nounits'),
)


def render(devices : List[Dict[str, Any]], args : Sequence[str], p2p : str = '') -> Tuple[str, str, int]:
    '''``(stdout, stderr, exit code)`` for one ``nvidia-smi`` argv against ``devices``.'''
    args = list(args)
    if args[:1] == ['-L']:
        lines = []
        for d in devices:
            lines.append(f'GPU {d["index"]}: {d["name"]} (UUID: {d["uuid"]})')
            for i, mig in enumerate(d.get('mig', [])):
                lines.append(f'  MIG {mig["profile"]}     Device  {i}: (UUID: {mig["uuid"]})')
        return ''.join(line + '\n' for line in lines), '', 0
    if args[:1] == ['topo']:
        pairs = set(p2p.split(','))
        n = len(devices)
        lines = ['\t' + '\t'.join(f'GPU{i}' for i in range(n))]
        for i in range(n):
            cells = ['X' if i == j else ('OK' if f'{i}-{j}' in pairs or f'{j}-{i}' in pairs else 'NS') for j in range(n)]
            lines.append(f' GPU{i}\t' + '\t'.join(cells) + '\t')
        lines.append('\nLegend:\n\n  X    = Self\n  OK   = Status Ok\n  CNS  = Chipset not supported')
        return ''.join(line + '\n' for line in lines), '', 0
    for arg in args:
        if arg.startswith('--query-gpu='):
            fields = arg[len('--query-gpu='):].split(',')
            lines = []
            for d in devices:
                row = {'index': d['index'], 'uuid': d['uuid'], 'name': d['name'],
                       'memory.total': d.get('memory_total_mib', 0), 'memory.used': d.get('memory_used_mib', 0),
                       'driver_version': '595.71.05'}
                lines.append(', '.join(str(row.get(f, '')) for f in fields))
            return ''.join(line + '\n' for line in lines), '', 0
        if arg.startswith('--query-compute-apps='):
            fields = arg[len('--query-compute-apps='):].split(',')
            lines = []
            for d in devices:
                for app in d.get('apps', []):
                    row = {'gpu_uuid': d['uuid'], 'pid': app['pid'], 'used_memory': app.get('used_mib', 0)}
                    lines.append(', '.join(str(row.get(f, '')) for f in fields))
            return ''.join(line + '\n' for line in lines), '', 0
    return '', 'fake nvidia-smi: unsupported arguments ' + ' '.join(args) + '\n', 2


def answer_key(args : Sequence[str]) -> str:
    '''The file name a pre-rendered answer is kept under — what the shim's ``tr`` makes of ``"$*"``.'''
    return re.sub(r'[^A-Za-z0-9]', '_', ' '.join(args))


def prerender(directory : str, devices : List[Dict[str, Any]], p2p : str = '') -> None:
    '''Writes the answer to every known argv under ``directory`` for the shim to serve.'''
    os.makedirs(directory, exist_ok = True)
    for args in KNOWN_QUERIES:
        out, _err, code = render(devices, args, p2p)
        assert code == 0, args
        with open(os.path.join(directory, answer_key(args)), 'w') as f:
            f.write(out)


def shim_script(python : str) -> str:
    '''The ``sh`` shim: a failing or hanging driver first, then a pre-rendered answer, else this file.'''
    return (
        '#!/bin/sh\n'
        'if [ -n "$VF_FAKE_SMI_FAIL" ]; then\n'
        '  echo "NVIDIA-SMI has failed because it couldn\'t communicate with the NVIDIA driver." >&2\n'
        '  exit 9\n'
        'fi\n'
        'if [ -n "$VF_FAKE_SMI_HANG" ]; then sleep 3600; fi\n'
        'if [ -n "$VF_FAKE_SMI_DIR" ]; then\n'
        '  key=$(printf \'%s\' "$*" | tr -c \'A-Za-z0-9\' \'_\')\n'
        '  if [ -f "$VF_FAKE_SMI_DIR/$key" ]; then cat "$VF_FAKE_SMI_DIR/$key"; exit 0; fi\n'
        'fi\n'
        f'exec {python} {os.path.abspath(__file__)} "$@"\n'
    )


def main() -> int:
    if os.environ.get('VF_FAKE_SMI_FAIL'):
        print('NVIDIA-SMI has failed because it couldn\'t communicate with the NVIDIA driver.', file = sys.stderr)
        return 9
    if os.environ.get('VF_FAKE_SMI_HANG'):
        time.sleep(3600)
    devices = json.loads(os.environ.get('VF_FAKE_SMI_JSON', '[]'))
    out, err, code = render(devices, sys.argv[1:], os.environ.get('VF_FAKE_SMI_P2P', ''))
    sys.stdout.write(out)
    sys.stderr.write(err)
    return code


if __name__ == '__main__':
    sys.exit(main())
