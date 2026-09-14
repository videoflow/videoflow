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
'''
import json
import os
import sys
import time


def main() -> int:
    if os.environ.get('VF_FAKE_SMI_FAIL'):
        print('NVIDIA-SMI has failed because it couldn\'t communicate with the NVIDIA driver.', file = sys.stderr)
        return 9
    if os.environ.get('VF_FAKE_SMI_HANG'):
        time.sleep(3600)
    devices = json.loads(os.environ.get('VF_FAKE_SMI_JSON', '[]'))
    args = sys.argv[1:]
    if args[:1] == ['-L']:
        for d in devices:
            print(f'GPU {d["index"]}: {d["name"]} (UUID: {d["uuid"]})')
            for i, mig in enumerate(d.get('mig', [])):
                print(f'  MIG {mig["profile"]}     Device  {i}: (UUID: {mig["uuid"]})')
        return 0
    if args[:1] == ['topo']:
        pairs = set(os.environ.get('VF_FAKE_SMI_P2P', '').split(','))
        n = len(devices)
        print('\t' + '\t'.join(f'GPU{i}' for i in range(n)))
        for i in range(n):
            cells = ['X' if i == j else ('OK' if f'{i}-{j}' in pairs or f'{j}-{i}' in pairs else 'NS') for j in range(n)]
            print(f' GPU{i}\t' + '\t'.join(cells) + '\t')
        print('\nLegend:\n\n  X    = Self\n  OK   = Status Ok\n  CNS  = Chipset not supported')
        return 0
    for arg in args:
        if arg.startswith('--query-gpu='):
            fields = arg[len('--query-gpu='):].split(',')
            for d in devices:
                row = {'index': d['index'], 'uuid': d['uuid'], 'name': d['name'],
                       'memory.total': d.get('memory_total_mib', 0), 'memory.used': d.get('memory_used_mib', 0),
                       'driver_version': '595.71.05'}
                print(', '.join(str(row.get(f, '')) for f in fields))
            return 0
        if arg.startswith('--query-compute-apps='):
            fields = arg[len('--query-compute-apps='):].split(',')
            for d in devices:
                for app in d.get('apps', []):
                    row = {'gpu_uuid': d['uuid'], 'pid': app['pid'], 'used_memory': app.get('used_mib', 0)}
                    print(', '.join(str(row.get(f, '')) for f in fields))
            return 0
    print('fake nvidia-smi: unsupported arguments ' + ' '.join(args), file = sys.stderr)
    return 2


if __name__ == '__main__':
    sys.exit(main())
