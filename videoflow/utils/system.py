'''
Host GPU inspection.

The one place videoflow enumerates physical GPU ordinals. Two faces of the
multi-GPU visibility contract (RFC 0003) live here: ``granted_gpus()`` is the
component-facing side — inside a worker, the visible devices are exactly the
granted devices, numbered ``0..n-1`` — and ``visible_physical_gpus()`` is the
engine-facing side, the pool of physical ordinals the local engine partitions
across workers (``engines/local.py``). Kubernetes workers normally don't need
either (the device plugin already masks the container to its grant), but
non-CUDA runtimes and native wrappers can call ``granted_gpus()`` instead of
parsing ``CUDA_VISIBLE_DEVICES`` themselves.
'''
from __future__ import absolute_import, division, print_function

import logging
import os
import re
import subprocess

logger = logging.getLogger(__package__)

#: Bound on the ``nvidia-smi`` probe — a wedged driver otherwise hangs the
#: local engine's launch path indefinitely. Normal responses take well under 1s.
NVIDIA_SMI_TIMEOUT_SECONDS = 5


def get_number_of_gpus() -> int:
    '''
    The number of physical GPUs in the system, 0 when ``nvidia-smi`` is missing,
    failing or hung. Counts the ``GPU <n>:`` lines of ``nvidia-smi -L``, so MIG
    instances are **not** enumerated: a MIG-enabled card counts once, even though
    its compute is only reachable through its MIG devices, which videoflow does
    not address locally (see ``visible_physical_gpus()``).
    '''
    try:
        output = subprocess.check_output(['nvidia-smi', '-L'],
                                         timeout = NVIDIA_SMI_TIMEOUT_SECONDS).decode('utf-8', errors = 'replace')
        # Anchored on the physical-GPU lines: on a MIG host every indented
        # 'MIG ... (UUID: MIG-...)' instance line also contains 'UUID', so a bare
        # substring count overcounts. Immune to the '(UUID: N/A)' output of some
        # old consumer boards, too.
        return len(re.findall(r'^GPU \d+:', output, flags = re.MULTILINE))
    except FileNotFoundError:
        # No NVIDIA driver installed — the normal CPU-only machine, not worth a warning.
        return 0
    except (OSError, subprocess.SubprocessError) as e:
        logger.warning('nvidia-smi failed (%s); assuming 0 GPUs', e)
        return 0

def get_system_gpus() -> set[int]:
    '''
    Returns the ids of gpus in the machine as a set of integers
    '''
    n = get_number_of_gpus()
    return set(range(n))

def visible_physical_gpus() -> list[int]:
    '''
    The physical device ordinals visible to the calling process, sorted: the
    system's GPUs intersected with ``CUDA_VISIBLE_DEVICES`` (unset ⇒ all of
    them; entries that are not plain integers are ignored, with a warning).
    Engine-facing: this is the pool the local engine partitions into per-worker
    ``CUDA_VISIBLE_DEVICES`` masks (``engines/local.py``). These are **not**
    valid ``cuda:i`` indices under a ``CUDA_VISIBLE_DEVICES`` mask — CUDA
    renumbers the masked devices to ``0..n-1``; components should use
    ``granted_gpus()`` instead.

    MIG caveat: a MIG-enabled card contributes a single ordinal — its instances
    are not enumerated, and an integer ``CUDA_VISIBLE_DEVICES`` entry cannot
    address a MIG slice (slices are pinned by ``MIG-<uuid>`` or granted by the
    Kubernetes device plugin), so the local engine never partitions below a
    whole card.
    '''
    system_devices = get_system_gpus()
    env_var = os.environ.get('CUDA_VISIBLE_DEVICES')
    if env_var is None:
        visible_devices = set(system_devices)
    else:
        visible_devices = set()
        dropped = []
        for device in env_var.strip().split(','):
            try:
                visible_devices.add(int(device))
            except ValueError:
                # Blank entries stay silent: CUDA_VISIBLE_DEVICES='' is the standard
                # hide-all idiom, and a trailing comma yields a stray ''.
                if device.strip():
                    dropped.append(device.strip())
        if dropped:
            logger.warning(
                f'CUDA_VISIBLE_DEVICES entries {", ".join(dropped)} are not integer ordinals and '
                f'were dropped — videoflow maps local devices by integer ordinal, so a UUID-pinned '
                f'pool cannot be partitioned across workers. Pin by ordinal '
                f'(e.g. CUDA_VISIBLE_DEVICES=0,1) or unset it.')
    return sorted(system_devices & visible_devices)

def granted_gpus() -> list[int]:
    '''
    The calling process's granted devices as valid CUDA indices ``[0..n-1]``
    (RFC 0003). Correct in a pod (the device plugin renumbers the grant) and
    locally (CUDA renumbers the ``CUDA_VISIBLE_DEVICES`` mask), so a component
    can both check ``len(granted_gpus())`` against a hard device minimum in
    ``open()`` and place work with ``.to(f'cuda:{i}')`` for each entry ``i``.

    Counts whole physical cards, never MIG instances. Inside a pod granted one
    MIG device this still returns ``[0]`` — the correct index, since CUDA
    exposes that slice as ``cuda:0`` — but there is never one entry per MIG
    slice, and CUDA itself addresses at most one MIG instance per process.
    '''
    return list(range(len(visible_physical_gpus())))

#: Deprecated pre-RFC-0003 name for ``granted_gpus()``.
get_gpus_available_to_process = granted_gpus
