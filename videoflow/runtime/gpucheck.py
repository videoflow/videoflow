'''
Worker-side verification of a GPU grant: what the node was granted versus what
the process can actually see, decided before the node is opened.

The allocator (a Kubernetes device plugin, the local backend, one day a DRA
driver) decides *which* devices a worker gets; this module never allocates. It
answers the component-side questions RFC 0006's plan (Phase 4, RUN-039/043/044)
puts to a worker:

- **How many usable devices did I receive, by which identities?** The
  ``DeliveredGrant`` the launcher recorded (``VF_GPU_GRANT_JSON``) when there is
  one; otherwise the CUDA-visible namespace enumerated through the inherited
  ``CUDA_VISIBLE_DEVICES`` mask by ordinal, card UUID or MIG UUID — never by
  counting integers in the mask, which is how a valid UUID grant used to read as
  "no GPUs".
- **Is a short grant fatal?** Only when the node says so (``gpu_fallback = 'none'``);
  by default a shortfall is reported, the CPU fallback is named, and the node opens.
- **Does the grant have the topology the node requires?** ``requires_peer_access``
  is verified with ``nvidia-smi topo -p2p r`` (driver-level P2P capability, no
  CUDA dependency); a count-only grant cannot satisfy it silently.

What could not be observed stays "unknown" in the report: a missing ``nvidia-smi``
in a CPU image is not evidence of zero devices.
'''
from __future__ import absolute_import, division, print_function

import logging
import os
import re
import subprocess
from dataclasses import dataclass, field
from typing import Any, Mapping

from ..backends.allocation import DeliveredGrant, DeviceIdentity
from ..backends.outcomes import Observation, Unknown, known, unknown
from ..core.constants import GPU
from ..core.errors import ResourceUnavailable
from ..core.node import Node, ProcessorNode
from ..deploy.allocation_local import grant_from_env
from ..utils.system import NVIDIA_SMI_TIMEOUT_SECONDS, apply_mask, host_devices_observed, mask_entries

logger = logging.getLogger(__package__)

EXECUTION_GPU = 'gpu'
EXECUTION_CPU = 'cpu'
EXECUTION_UNKNOWN = 'unknown'

FALLBACK_CPU = 'cpu'
FALLBACK_NONE = 'none'
GPU_FALLBACKS = (FALLBACK_CPU, FALLBACK_NONE)


@dataclass(frozen = True)
class GrantReport:
    '''
    - Arguments:
        - requested: the node's ``gpu_count`` (0 for a CPU node).
        - delivered: the devices the worker can use, in CUDA index order — the \
            grant's when it was observed, else the enumerated namespace.
        - source: ``grant`` (a launcher-recorded ``DeliveredGrant``), ``enumeration`` \
            (``nvidia-smi`` through the mask) or ``unobserved`` (neither answered).
        - execution: ``gpu``, ``cpu`` (declared fallback taken) or ``unknown``.
        - exclusive: whether the launcher promised exclusive devices (never assumed).
        - peer_access: ``Known(True/False)`` once verified, ``Unknown`` otherwise.
        - problems: every shortfall found, in words; empty when the grant is whole.
    '''
    requested : int
    delivered : tuple[DeviceIdentity, ...]
    source : str
    execution : str
    exclusive : bool
    mask : str | None
    peer_access : Observation[bool] | None = None
    problems : tuple[str, ...] = ()
    evidence : Mapping[str, Any] = field(default_factory = dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            'requested': self.requested,
            'delivered': [d.mig_uuid or d.uuid or str(d.ordinal) for d in self.delivered],
            'source': self.source, 'execution': self.execution, 'exclusive': self.exclusive,
            'mask': self.mask,
            'peer_access': (None if self.peer_access is None else
                            self.peer_access.value if not isinstance(self.peer_access, Unknown) else 'unknown'),
            'problems': list(self.problems), 'evidence': dict(self.evidence),
        }


_P2P_ROW = re.compile(r'^\s*GPU(?P<i>\d+)\t(?P<cells>.*)$')


def parse_p2p_matrix(text : str) -> dict[tuple[int, int], str]:
    '''``nvidia-smi topo -p2p r`` as ``(i, j) -> status`` (``OK``, ``NS``, ``CNS``, ``GNS``, ``X``).'''
    out : dict[tuple[int, int], str] = {}
    for line in text.splitlines():
        row = _P2P_ROW.match(line)
        if not row:
            continue
        i = int(row.group('i'))
        for j, cell in enumerate(c.strip() for c in row.group('cells').split('\t')):
            if cell:
                out[(i, j)] = cell
    return out


def peer_access_observed(devices : tuple[DeviceIdentity, ...]) -> Observation[bool]:
    '''
    Whether every pair of the granted physical devices reports P2P read
    capability ``OK`` in ``nvidia-smi topo -p2p r``. ``Unknown`` when the
    matrix cannot be read or a device has no host ordinal (a MIG slice never has
    peer access; two slices of one card are not peers).
    '''
    ordinals = [d.ordinal for d in devices]
    if any(o is None for o in ordinals) or any(d.mig_uuid for d in devices):
        return unknown('malformed', 'peer access needs whole physical devices with host ordinals')
    try:
        text = subprocess.check_output(['nvidia-smi', 'topo', '-p2p', 'r'],
                                       timeout = NVIDIA_SMI_TIMEOUT_SECONDS).decode('utf-8', errors = 'replace')
    except FileNotFoundError:
        return unknown('missing', 'nvidia-smi is not on PATH')
    except subprocess.TimeoutExpired:
        return unknown('timeout', f'nvidia-smi topo did not answer within {NVIDIA_SMI_TIMEOUT_SECONDS}s')
    except (OSError, subprocess.SubprocessError) as e:
        return unknown('failed', f'{type(e).__name__}: {e}')
    matrix = parse_p2p_matrix(text)
    if not matrix:
        return unknown('malformed', 'nvidia-smi topo -p2p r printed no matrix')
    ints = [int(o) for o in ordinals if o is not None]
    for a in ints:
        for b in ints:
            if a != b and matrix.get((a, b)) != 'OK':
                return known(False)
    return known(True)


def enumerate_devices(environ : Mapping[str, str] = os.environ) -> Observation[list[DeviceIdentity]]:
    '''The devices this process can address through its mask, or Unknown when the host cannot be read.'''
    observed = host_devices_observed()
    if isinstance(observed, Unknown):
        return observed
    return known(apply_mask(observed.value, mask_entries(environ.get('CUDA_VISIBLE_DEVICES'))))


def report_backend(requested : int, environ : Mapping[str, str] = os.environ,
                   grant : DeliveredGrant | None = None) -> GrantReport:
    '''
    What this worker really has: the launcher's grant when it recorded one and
    observed the host, else the enumerated namespace, else "unobserved". Never
    raises; ``verify_grant`` turns the report into a decision.
    '''
    grant = grant if grant is not None else grant_from_env(environ)
    mask = environ.get('CUDA_VISIBLE_DEVICES')
    evidence : dict[str, Any] = {}
    if grant is not None and grant.host == 'observed':
        delivered = tuple(grant.devices)
        source = 'grant'
        exclusive = grant.exclusive
        evidence['policy'] = grant.policy
    else:
        enumerated = enumerate_devices(environ)
        exclusive = False
        if isinstance(enumerated, Unknown):
            evidence['enumeration'] = f'{enumerated.reason}: {enumerated.detail}'
            if grant is not None:
                evidence['policy'] = grant.policy
            return GrantReport(requested, (), 'unobserved', EXECUTION_UNKNOWN, False, mask,
                               problems = (f'the GPU namespace could not be enumerated ({enumerated.reason}: '
                                           f'{enumerated.detail})',), evidence = evidence)
        delivered = tuple(enumerated.value)
        source = 'enumeration'
    problems : list[str] = []
    if len(delivered) < requested:
        problems.append(f'requested {requested} device(s), delivered {len(delivered)}'
                        + (f' ({", ".join(d.mig_uuid or d.uuid or str(d.ordinal) for d in delivered)})' if delivered else ''))
    execution = EXECUTION_GPU if delivered else EXECUTION_CPU
    return GrantReport(requested, delivered, source, execution, exclusive, mask, problems = tuple(problems),
                       evidence = evidence)


def verify_grant(requested : int, fallback : str = FALLBACK_CPU, requires_peer_access : bool = False,
                 environ : Mapping[str, str] = os.environ, grant : DeliveredGrant | None = None,
                 node_name : str = 'node') -> GrantReport:
    '''
    The decision a worker makes before opening a GPU node.

    - Raises:
        - ResourceUnavailable: a hard requirement (``fallback='none'``) met a short, \
            empty or unobservable grant; or ``requires_peer_access`` could not be \
            verified true for the delivered devices.
    '''
    if fallback not in GPU_FALLBACKS:
        raise ValueError(f'gpu_fallback must be one of {GPU_FALLBACKS}, got {fallback!r}')
    report = report_backend(requested, environ, grant)
    if report.source == 'unobserved':
        if fallback == FALLBACK_NONE:
            raise ResourceUnavailable(
                f'Node {node_name} requires {requested} GPU(s) but its grant cannot be verified: '
                + '; '.join(report.problems) + '.',
                remedy = 'Run the node in an image with the NVIDIA driver utilities (nvidia-smi), or declare '
                         "gpu_fallback = 'cpu' if the node can run without its devices.")
        logger.warning(f'node {node_name}: {"; ".join(report.problems)} — opening it anyway '
                       f'(gpu_fallback={fallback!r}); execution is unknown, not a confirmed CPU run')
        return report
    if report.problems:
        if fallback == FALLBACK_NONE:
            raise ResourceUnavailable(
                f'Node {node_name} requires {requested} GPU(s); {"; ".join(report.problems)} '
                f'(mask {report.mask!r}, source {report.source}).',
                remedy = 'Grant the node its devices (gpu_count on Kubernetes, --gpu-policy strict locally), '
                         "or declare gpu_fallback = 'cpu' if it can run short.")
        logger.warning(f'node {node_name}: {"; ".join(report.problems)}; running with execution='
                       f'{report.execution} (declared gpu_fallback={fallback!r})')
    if requires_peer_access and requested > 1 and not report.problems:
        peer = peer_access_observed(report.delivered)
        report = GrantReport(report.requested, report.delivered, report.source, report.execution, report.exclusive,
                             report.mask, peer, report.problems, report.evidence)
        if isinstance(peer, Unknown):
            raise ResourceUnavailable(
                f'Node {node_name} requires peer access between its {requested} devices, which could not be '
                f'verified ({peer.reason}: {peer.detail}).',
                remedy = 'Run the node where nvidia-smi can report the P2P topology, or drop requires_peer_access.')
        if not peer.value:
            raise ResourceUnavailable(
                f'Node {node_name} requires peer access between its devices, and the delivered pair '
                f'{[d.uuid or d.ordinal for d in report.delivered]} has none (nvidia-smi topo -p2p r).',
                remedy = 'Schedule the node on devices with NVLink/PCIe peer access (a topology constraint), '
                         'or drop requires_peer_access if the component can run without it.')
    return report


def verify_node_grant(node : Node, environ : Mapping[str, str] = os.environ) -> GrantReport | None:
    '''The worker's entry point: nothing for a CPU node, else ``verify_grant`` with the node's declarations.'''
    # Only processors carry a device type and a count (producers and consumers are CPU nodes).
    if not isinstance(node, ProcessorNode) or node.device_type != GPU:
        return None
    report = verify_grant(node.gpu_count, node.gpu_fallback, node.requires_peer_access, environ,
                          node_name = node.name)
    logger.info(f'node {node.name} GPU grant: {report.to_dict()}')
    return report
