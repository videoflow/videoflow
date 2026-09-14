'''
An independent CUDA-runtime probe, run as a subprocess by ``_gpu.py``.

``enumerate``: what this process's ``CUDA_VISIBLE_DEVICES`` exposes according
to the CUDA runtime — the count, each logical index's UUID and name, total
memory, and peer access between every pair — as one JSON line on stdout.
``hold``: additionally allocates ``hold_bytes`` on device 0 (a resident block)
and keeps it for ``hold_seconds`` so an outside ``nvidia-smi`` can measure it.
``workload``: a representative transfer-bound loop on device 0 — a resident
block, then ``input_bytes`` copied host-to-device, touched on the device and
copied back, paced at ``rate_hz`` for ``duration`` seconds — reporting the
delivered rate, per-item latency percentiles and the peak device memory it
occupied (ALLOC-032's measurement, with the profile under test as the mask).
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
    if mode == 'workload' and count:
        # argv: workload <input_bytes> <rate_hz> <duration_seconds> <resident_bytes>
        input_bytes = hold_bytes
        rate_hz = hold_seconds
        duration = float(sys.argv[4]) if len(sys.argv) > 4 else 5.0
        resident_bytes = int(sys.argv[5]) if len(sys.argv) > 5 else 0
        check(cudart.cudaSetDevice(0))
        free_before, total = check(cudart.cudaMemGetInfo())
        resident = check(cudart.cudaMalloc(resident_bytes)) if resident_bytes else None
        device_buf = check(cudart.cudaMalloc(input_bytes))
        host_in = bytearray(input_bytes)
        host_out = bytearray(input_bytes)
        free_after, _ = check(cudart.cudaMemGetInfo())
        latencies = []
        period = 1.0 / rate_hz if rate_hz > 0 else 0.0
        end = time.monotonic() + duration
        next_at = time.monotonic()
        while time.monotonic() < end:
            t0 = time.perf_counter()
            check(cudart.cudaMemcpy(device_buf, host_in, input_bytes, cudart.cudaMemcpyKind.cudaMemcpyHostToDevice))
            check(cudart.cudaMemset(device_buf, 7, input_bytes))
            check(cudart.cudaMemcpy(host_out, device_buf, input_bytes, cudart.cudaMemcpyKind.cudaMemcpyDeviceToHost))
            check(cudart.cudaDeviceSynchronize())
            latencies.append(time.perf_counter() - t0)
            next_at += period
            delay = next_at - time.monotonic()
            if delay > 0:
                time.sleep(delay)
        ordered = sorted(latencies)

        def pct(p):
            return ordered[max(0, min(len(ordered) - 1, int(round(p / 100.0 * (len(ordered) - 1)))))] if ordered else None
        report.update({'workload': {'input_bytes': input_bytes, 'rate_hz': rate_hz, 'duration_seconds': duration,
                                    'resident_bytes': resident_bytes, 'items': len(latencies),
                                    'delivered_rate_hz': len(latencies) / duration if duration else None,
                                    'latency_ms': {'p50': (pct(50) or 0) * 1000, 'p95': (pct(95) or 0) * 1000,
                                                   'p99': (pct(99) or 0) * 1000, 'max': (max(ordered) if ordered else 0) * 1000},
                                    'peak_memory_bytes': int(free_before - free_after), 'device_total_bytes': int(total)}})
        check(cudart.cudaFree(device_buf))
        if resident is not None:
            check(cudart.cudaFree(resident))
        print(json.dumps(report))
        return 0
    print(json.dumps(report))
    return 0


if __name__ == '__main__':
    sys.exit(main())
