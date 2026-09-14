'''
An independent CUDA-runtime probe, run as a subprocess by ``_gpu.py``.

``enumerate``: what this process's ``CUDA_VISIBLE_DEVICES`` exposes according
to the CUDA runtime — the count, each logical index's UUID and name, total
memory, and peer access between every pair — as one JSON line on stdout.
``hold``: additionally allocates ``hold_bytes`` on device 0 (a resident block)
and keeps it for ``hold_seconds`` so an outside ``nvidia-smi`` can measure it.
Nothing here consults videoflow: the point is a second opinion.
'''
from __future__ import absolute_import, division, print_function

import json
import os
import sys
import time


def main() -> int:
    mode = sys.argv[1] if len(sys.argv) > 1 else 'enumerate'
    hold_bytes = int(sys.argv[2]) if len(sys.argv) > 2 else 0
    hold_seconds = float(sys.argv[3]) if len(sys.argv) > 3 else 0.0
    from cuda.bindings import runtime as cudart

    def check(result):
        err = result[0]
        if err != cudart.cudaError_t.cudaSuccess:
            raise RuntimeError(f'CUDA error {err}')
        return result[1] if len(result) == 2 else result[1:]

    report = {'mask': os.environ.get('CUDA_VISIBLE_DEVICES'), 'pid': os.getpid(),
              'driver': check(cudart.cudaDriverGetVersion()), 'runtime': check(cudart.cudaRuntimeGetVersion())}
    err, count = cudart.cudaGetDeviceCount()
    if err != cudart.cudaError_t.cudaSuccess:
        # cudaErrorNoDevice is the runtime's answer to an empty/unresolvable mask — a
        # fact worth reporting, not a probe failure.
        report.update({'count': 0, 'devices': [], 'peer': {}, 'enumeration_error': str(err)})
        print(json.dumps(report))
        return 0
    devices = []
    for i in range(count):
        props = check(cudart.cudaGetDeviceProperties(i))
        name = props.name.decode('utf-8', 'replace') if isinstance(props.name, bytes) else str(props.name)
        raw = bytes(props.uuid.bytes)
        hexed = raw.hex()
        devices.append({'index': i, 'name': name.rstrip('\x00'),
                        'uuid': f'GPU-{hexed[:8]}-{hexed[8:12]}-{hexed[12:16]}-{hexed[16:20]}-{hexed[20:]}',
                        'total_memory': int(props.totalGlobalMem)})
    peer = {}
    for a in range(count):
        for b in range(count):
            if a != b:
                peer[f'{a}-{b}'] = bool(check(cudart.cudaDeviceCanAccessPeer(a, b)))
    report.update({'count': count, 'devices': devices, 'peer': peer})
    if mode == 'hold' and count:
        check(cudart.cudaSetDevice(0))
        ptr = check(cudart.cudaMalloc(hold_bytes)) if hold_bytes else None
        check(cudart.cudaMemset(ptr, 1, hold_bytes)) if ptr is not None else None
        check(cudart.cudaDeviceSynchronize())
        free, total = check(cudart.cudaMemGetInfo())
        report.update({'held_bytes': hold_bytes, 'free_after_hold': int(free), 'total': int(total)})
        print(json.dumps(report), flush = True)
        time.sleep(hold_seconds)
        if ptr is not None:
            check(cudart.cudaFree(ptr))
        return 0
    print(json.dumps(report))
    return 0


if __name__ == '__main__':
    sys.exit(main())
