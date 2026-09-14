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
from typing import Sequence

from ..backends.allocation import DeviceIdentity
from ..backends.outcomes import Observation, Unknown, known, unknown

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
    them). A mask with ``GPU-…``/``MIG-…`` entries is resolved the way the CUDA
    runtime resolves it (``apply_mask``: identities via ``nvidia-smi -L``, and
    nothing past the first entry that names no device), with a warning naming
    the entries the runtime would not expose.
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
    env_var = os.environ.get('CUDA_VISIBLE_DEVICES')
    if env_var is not None and any(not e.isdigit() for e in mask_entries(env_var) or []):
        # A UUID-pinned mask (``GPU-…``/``MIG-…`` entries): resolved against the
        # host's identities, the way the CUDA runtime resolves it, to the physical
        # ordinals of the addressed cards (a MIG entry addresses its parent card).
        # Entries that resolve to nothing are dropped, as the runtime drops them.
        return sorted({d.ordinal for d in visible_devices() if d.ordinal is not None})
    system_devices = get_system_gpus()
    if env_var is None:
        visible_devices_ = set(system_devices)
    else:
        visible_devices_ = set()
        for device in env_var.strip().split(','):
            # Blank entries stay silent: CUDA_VISIBLE_DEVICES='' is the standard
            # hide-all idiom, and a trailing comma yields a stray ''.
            if device.strip():
                visible_devices_.add(int(device))
    return sorted(system_devices & visible_devices_)

def granted_gpus() -> list[int]:
    '''
    The calling process's granted devices as valid CUDA indices ``[0..n-1]``
    (RFC 0003). Correct in a pod (the device plugin renumbers the grant) and
    locally (CUDA renumbers the ``CUDA_VISIBLE_DEVICES`` mask), so a component
    can both check ``len(granted_gpus())`` against a hard device minimum in
    ``open()`` and place work with ``.to(f'cuda:{i}')`` for each entry ``i``.

    Under an integer mask (or none) this counts whole physical cards. Under a
    UUID mask it counts the mask's resolved entries — one per ``GPU-…`` card and
    one per ``MIG-…`` instance, in mask order, which is what CUDA enumerates
    (whether a driver exposes more than one MIG instance to a process is the
    driver's call; the count reports what the mask names, and ``runtime.gpucheck``
    reports what was delivered).
    '''
    mask = mask_entries(os.environ.get('CUDA_VISIBLE_DEVICES'))
    if mask is not None and any(not e.isdigit() for e in mask):
        return list(range(len(visible_devices())))
    return list(range(len(visible_physical_gpus())))

#: Deprecated pre-RFC-0003 name for ``granted_gpus()``.
get_gpus_available_to_process = granted_gpus


# -- device identities (RFC 0006 plan Phase 4) -------------------------------------------

#: The ``nvidia-smi`` query the local allocation backend inventories from: one CSV
#: row per physical GPU, memory in MiB (``nounits``). ``-L`` adds the MIG UUIDs.
NVIDIA_SMI_QUERY = ('--query-gpu=index,uuid,name,memory.total,memory.used', '--format=csv,noheader,nounits')


def _smi(args : Sequence[str], timeout : float = NVIDIA_SMI_TIMEOUT_SECONDS) -> str:
    return subprocess.check_output(['nvidia-smi', *args], timeout = timeout).decode('utf-8', errors = 'replace')


def parse_smi_query(csv_text : str) -> list[DeviceIdentity]:
    '''``nvidia-smi --query-gpu=index,uuid,name,memory.total,memory.used`` rows as identities (memory in bytes).'''
    out : list[DeviceIdentity] = []
    for line in csv_text.splitlines():
        parts = [p.strip() for p in line.split(',')]
        if len(parts) < 4 or not parts[0].isdigit():
            continue
        total = int(parts[3]) * 1024 * 1024 if parts[3].isdigit() else None
        out.append(DeviceIdentity(node = None, ordinal = int(parts[0]), uuid = parts[1] or None, mig_uuid = None,
                                  product = parts[2], memory_bytes = total, mig_profile = None))
    return out


def parse_smi_used(csv_text : str) -> dict[str, int]:
    '''``uuid -> memory.used`` in bytes from the same query, for peak-memory admission.'''
    used : dict[str, int] = {}
    for line in csv_text.splitlines():
        parts = [p.strip() for p in line.split(',')]
        if len(parts) >= 5 and parts[1] and parts[4].isdigit():
            used[parts[1]] = int(parts[4]) * 1024 * 1024
    return used


_MIG_LINE = re.compile(r'^\s+MIG\s+(?P<profile>\S+)\s+Device\s+(?P<index>\d+):\s+\(UUID:\s+(?P<uuid>MIG-[^)]+)\)', re.MULTILINE)
_GPU_LINE = re.compile(r'^GPU (?P<ordinal>\d+): (?P<product>.*?) \(UUID: (?P<uuid>[^)]+)\)', re.MULTILINE)


def parse_smi_list(text : str) -> list[DeviceIdentity]:
    '''
    ``nvidia-smi -L`` as identities: one per physical GPU, and one per MIG
    instance (``mig_uuid`` set, ``mig_profile`` its ``1g.24gb``-style name,
    ``uuid`` its parent card's). A MIG-enabled card's compute is reachable only
    through its instances, which is why they are enumerated here and not by
    ``get_number_of_gpus``.
    '''
    out : list[DeviceIdentity] = []
    current : DeviceIdentity | None = None
    for line in text.splitlines():
        gpu = _GPU_LINE.match(line)
        if gpu:
            current = DeviceIdentity(None, int(gpu.group('ordinal')), gpu.group('uuid'), None, gpu.group('product').strip(),
                                     None, None)
            out.append(current)
            continue
        mig = _MIG_LINE.match(line)
        if mig and current is not None:
            out.append(DeviceIdentity(None, current.ordinal, current.uuid, mig.group('uuid'), current.product, None,
                                      mig.group('profile')))
    return out


def host_devices_observed() -> Observation[list[DeviceIdentity]]:
    '''
    Every physical GPU (and MIG instance) of this host, with UUIDs and memory —
    or ``Unknown`` when ``nvidia-smi`` is absent, failing or hung. Never "no
    GPUs" for a failed read: the local allocation backend refuses to grant on
    an unobserved host rather than under-deliver silently (ALLOC-014).
    '''
    try:
        rows = parse_smi_query(_smi(NVIDIA_SMI_QUERY))
        listed = parse_smi_list(_smi(['-L']))
    except FileNotFoundError:
        return unknown('missing', 'nvidia-smi is not on PATH (no NVIDIA driver)')
    except subprocess.TimeoutExpired:
        return unknown('timeout', f'nvidia-smi did not answer within {NVIDIA_SMI_TIMEOUT_SECONDS}s')
    except (OSError, subprocess.SubprocessError) as e:
        return unknown('failed', f'{type(e).__name__}: {e}')
    by_uuid = {d.uuid: d for d in rows if d.uuid}
    devices : list[DeviceIdentity] = []
    for device in listed:
        card = by_uuid.get(device.uuid) if device.uuid else None
        memory = card.memory_bytes if card is not None else device.memory_bytes
        devices.append(DeviceIdentity(None, device.ordinal, device.uuid, device.mig_uuid, device.product, memory,
                                      device.mig_profile))
    if not devices:
        devices = rows
    return known(devices)


def mask_entries(value : str | None) -> list[str] | None:
    '''The entries of a ``CUDA_VISIBLE_DEVICES`` value (None when unset; ``[]`` for the hide-all idiom ``''``).'''
    if value is None:
        return None
    return [entry.strip() for entry in value.split(',') if entry.strip()]


def apply_mask(devices : Sequence[DeviceIdentity], mask : Sequence[str] | None) -> list[DeviceIdentity]:
    '''
    The devices a ``CUDA_VISIBLE_DEVICES`` mask exposes, in mask order, as the
    CUDA runtime resolves it: an integer entry is a physical ordinal, a
    ``GPU-…`` entry a card UUID (a unique prefix suffices), a ``MIG-…`` entry a
    MIG instance UUID. The runtime's rules, as observed on driver R595 (plan
    Phase 4, ALLOC-015): enumeration stops at the first entry that names no
    device — an unknown UUID, an out-of-range ordinal, or an entry of the other
    form in a mixed list — and a mask naming one device twice is invalid as a
    whole (``cudaErrorInvalidDevice``: nothing is exposed). ``None`` (unset)
    exposes every physical card; MIG instances are never exposed implicitly.
    '''
    cards = [d for d in devices if d.mig_uuid is None]
    if mask is None:
        return list(cards)
    out : list[DeviceIdentity] = []
    form : str | None = None
    for entry in mask:
        kind = 'ordinal' if entry.isdigit() else 'uuid'
        if form is None:
            form = kind
        elif kind != form:
            break                                   # a mixed list ends at the first foreign entry
        if entry.isdigit():
            match = [d for d in cards if d.ordinal == int(entry)]
        elif entry.startswith('MIG-'):
            match = [d for d in devices if d.mig_uuid and d.mig_uuid.startswith(entry)]
        else:
            match = [d for d in cards if d.uuid and d.uuid.startswith(entry)]
        if len(match) != 1:
            break                                   # the runtime stops at the first invalid entry
        if match[0] in out:
            return []                               # a duplicate invalidates the whole mask
        out.append(match[0])
    return out


def visible_devices() -> list[DeviceIdentity]:
    '''
    The devices the calling process can address, honouring an inherited
    ``CUDA_VISIBLE_DEVICES`` by ordinal, card UUID or MIG UUID (RFC 0006 plan
    Phase 4; the identity-carrying counterpart of ``visible_physical_gpus``).
    Empty when the host cannot be observed: callers that must tell "no GPUs"
    from "no answer" use ``host_devices_observed``.
    '''
    observed = host_devices_observed()
    if isinstance(observed, Unknown):
        return []
    entries = mask_entries(os.environ.get('CUDA_VISIBLE_DEVICES'))
    resolved = apply_mask(observed.value, entries)
    if entries and len(resolved) < len(entries):
        dropped = entries[len(resolved):]
        logger.warning(
            f'CUDA_VISIBLE_DEVICES entries {", ".join(dropped)} name no device on this host (or repeat one), '
            f'so the CUDA runtime exposes only the {len(resolved)} entr{"y" if len(resolved) == 1 else "ies"} '
            f'before them. Check `nvidia-smi -L` for the UUIDs.')
    return resolved
