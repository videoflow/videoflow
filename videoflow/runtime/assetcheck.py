'''
Worker-side verification of a node's declared assets (plan Phase 4, RUN-033).

A model file on a hostPath exists on the host that has it, not on the host a
rescheduled pod landed on; a claim or a download can put the bytes everywhere,
but only a digest says they are the *declared* bytes. So before a node is
opened the worker walks ``node.required_assets()`` and refuses — with
``ResourceUnavailable``, naming the host, the path and the fix — to process
with a missing or different asset. Nothing here fetches or distributes: the
deployment (a PVC, an image layer, ``prepare.py``) owns distribution; the
runtime only verifies identity.
'''
from __future__ import absolute_import, division, print_function

import hashlib
import logging
import os
import socket
from dataclasses import dataclass
from typing import Sequence

from ..core.errors import ResourceUnavailable
from ..core.node import AssetRequirement, Node

logger = logging.getLogger(__package__)

_CHUNK = 1 << 20


@dataclass(frozen = True)
class AssetReport:
    path : str
    expected : str
    actual : str | None
    portable : bool
    status : str          # 'verified' | 'missing' | 'mismatch'


def sha256_of(path : str) -> str:
    digest = hashlib.sha256()
    with open(path, 'rb') as f:
        for chunk in iter(lambda: f.read(_CHUNK), b''):
            digest.update(chunk)
    return digest.hexdigest()


def check_assets(requirements : Sequence[AssetRequirement]) -> list[AssetReport]:
    '''One report per requirement; never raises — ``verify_assets`` decides.'''
    reports : list[AssetReport] = []
    for req in requirements:
        expected = req.sha256.lower().removeprefix('sha256:')
        if not os.path.isfile(req.path):
            reports.append(AssetReport(req.path, expected, None, req.portable, 'missing'))
            continue
        actual = sha256_of(req.path)
        reports.append(AssetReport(req.path, expected, actual, req.portable,
                                   'verified' if actual == expected else 'mismatch'))
    return reports


def verify_assets(requirements : Sequence[AssetRequirement], node_name : str = 'node') -> list[AssetReport]:
    '''
    - Raises:
        - ResourceUnavailable: an asset is missing on this host or its bytes are \
            not the declared ones. The remedy distinguishes a local-only asset \
            (pin the worker to the host that has it, or make it portable) from a \
            portable one (the distribution delivered other bytes).
    '''
    reports = check_assets(requirements)
    host = socket.gethostname()
    for report in reports:
        if report.status == 'verified':
            continue
        what = 'is missing' if report.status == 'missing' else f'has sha256 {report.actual}, not the declared {report.expected}'
        if report.portable:
            remedy = ('Re-run the distribution step that places the asset (prepare.py, the claim contents, '
                      'the image layer) and check its digest; the declared identity is what the node was built for.')
        else:
            remedy = ('This is a local-only asset (a hostPath): run the node on the host that has it '
                      '(--gpu-nodes / a node selector), or put it on a claim and declare it portable.')
        raise ResourceUnavailable(f'Node {node_name} on {host}: asset {report.path} {what}.', remedy = remedy)
    if reports:
        logger.info(f'node {node_name}: {len(reports)} asset(s) verified on {host}')
    return reports


def verify_node_assets(node : Node) -> list[AssetReport]:
    '''The worker's entry point: ``verify_assets`` over the node's declaration.'''
    return verify_assets(node.required_assets(), node.name)
