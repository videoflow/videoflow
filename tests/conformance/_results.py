'''
The results writer: turns per-test outcomes into the kit's ``run_results.json``
shape plus a ``run_manifest.json`` that pins what was actually run.

Why a manifest: the design package is explicit that the same broker name with a
different replication factor, or the same GPU with MIG enabled, is a different
qualified composition. A results file without the commit, the seed, the gates that
were open and the read-back configuration would be a number without a unit.
'''
from __future__ import absolute_import, division, print_function

import dataclasses
import hashlib
import json
import os
import pathlib
import platform
import subprocess
import sys
import time
from typing import Any, Dict, List, Optional

from _status import FAIL, INVALID_TEST, NOT_RUN, PASS, STATUSES, UNSUPPORTED

FAILED_AS_EXPECTED = 'FAILED_AS_EXPECTED'
DEFECT_NOT_DETECTED = 'DEFECT_NOT_DETECTED'

#: Environment variables whose presence means a gate *may* be open. Recorded, not
#: interpreted: the fixtures decide whether the resource actually answered.
GATE_ENV = (
    'VF_RFC0006', 'VF_TEST_SEED', 'VF_TEST_NATS_URL', 'VF_TEST_NATS_CLUSTER_URLS',
    'VF_TEST_NATS_PROXIED_URL', 'VF_TEST_NATS_RESTRICTED_URL', 'VF_TEST_TOXIPROXY_URL',
    'VF_TEST_REDIS_URL', 'VF_TEST_REDIS_DURABLE_URL', 'VF_TEST_REDIS_SMALL_URL',
    'VF_K8S_CONTEXT', 'VF_K8S_NAMESPACE', 'VF_K8S_PVC', 'VF_K8S_NATS_URL',
    'VF_K8S_IMAGE_REGISTRY', 'VF_K8S_GPU_NODES', 'VF_K8S_MIG_NODE',
    'VF_TEST_GPU_UUIDS', 'VF_TEST_MIG_GPU_UUID', 'VF_BENCH_THRESHOLDS_JSON',
)

@dataclasses.dataclass
class VariantResult:
    variant : str
    status : str
    reason : str = ''
    nodeid : str = ''

@dataclasses.dataclass
class CaseResult:
    case_id : str
    variants : List[VariantResult] = dataclasses.field(default_factory = list)
    fault_injection_observed : Optional[Dict[str, int]] = None
    negative_control_result : Optional[str] = None
    evidence_paths : List[str] = dataclasses.field(default_factory = list)
    notes : Optional[str] = None

    def primary_status(self) -> str:
        '''
        One status per case: FAIL if any variant failed, INVALID_TEST if any variant
        or the negative control was invalid, else the *primary* variant's status
        (the first recorded, which the harness orders as the catalog's primary level),
        else NOT_RUN.
        '''
        statuses = [v.status for v in self.variants]
        if self.negative_control_result == DEFECT_NOT_DETECTED:
            return INVALID_TEST
        if FAIL in statuses:
            return FAIL
        if INVALID_TEST in statuses:
            return INVALID_TEST
        if not statuses:
            return NOT_RUN
        # A case whose primary variant could not run but whose other variants passed
        # is still not a pass at its primary level: report the primary honestly.
        for v in self.variants:
            if v.variant == 'primary':
                return v.status
        return statuses[0]

class RunResults:
    '''Accumulates outcomes during a session and writes the two JSON artifacts.'''
    def __init__(self, case_ids : List[str]) -> None:
        self._cases : Dict[str, CaseResult] = {cid: CaseResult(cid) for cid in case_ids}
        self._started = time.time()
        self._run_id = time.strftime('%Y%m%dT%H%M%SZ', time.gmtime(self._started))

    def record(self, case_id : str, variant : str, status : str, reason : str = '',
               nodeid : str = '') -> None:
        assert status in STATUSES, status
        case = self._cases.setdefault(case_id, CaseResult(case_id))
        case.variants.append(VariantResult(variant, status, reason, nodeid))

    def record_negative_control(self, case_id : str, detected : bool) -> None:
        case = self._cases.setdefault(case_id, CaseResult(case_id))
        case.negative_control_result = FAILED_AS_EXPECTED if detected else DEFECT_NOT_DETECTED

    def record_faults(self, case_id : str, fired : Dict[str, int]) -> None:
        case = self._cases.setdefault(case_id, CaseResult(case_id))
        merged = dict(case.fault_injection_observed or {})
        for name, count in fired.items():
            merged[name] = merged.get(name, 0) + count
        case.fault_injection_observed = merged

    def add_evidence(self, case_id : str, path : str) -> None:
        case = self._cases.setdefault(case_id, CaseResult(case_id))
        if path not in case.evidence_paths:
            case.evidence_paths.append(path)

    def executed(self) -> int:
        return sum(1 for c in self._cases.values()
                   if any(v.status in (PASS, FAIL, UNSUPPORTED, INVALID_TEST) for v in c.variants))

    def manifest(self, seed : int) -> Dict[str, Any]:
        return {
            'run_id': self._run_id,
            'started_at': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime(self._started)),
            'source': {'core_commit': _git_head(), 'dirty': _git_dirty()},
            'python': sys.version.split()[0],
            'platform': platform.platform(),
            'hostname': platform.node(),
            'seed': seed,
            'gates': {name: os.environ.get(name) for name in GATE_ENV if os.environ.get(name) is not None},
        }

    def write(self, directory : pathlib.Path, seed : int) -> pathlib.Path:
        directory.mkdir(parents = True, exist_ok = True)
        manifest = self.manifest(seed)
        manifest_text = json.dumps(manifest, indent = 2, sort_keys = True) + '\n'
        digest = hashlib.sha256(manifest_text.encode()).hexdigest()
        (directory / 'run_manifest.json').write_text(manifest_text)
        results = {
            'template_only': False,
            'run_id': self._run_id,
            'manifest_sha256': digest,
            'backend_scenarios_executed': self.executed(),
            'summary': self.summary(),
            'results': [
                {
                    'case_id': c.case_id,
                    'status': c.primary_status(),
                    'variants': [dataclasses.asdict(v) for v in c.variants],
                    'fault_injection_observed': c.fault_injection_observed,
                    'negative_control_result': c.negative_control_result,
                    'evidence_paths': c.evidence_paths,
                    'notes': c.notes,
                }
                for c in sorted(self._cases.values(), key = lambda c: _sort_key(c.case_id))
            ],
        }
        path = directory / 'run_results.json'
        path.write_text(json.dumps(results, indent = 2) + '\n')
        return path

    def summary(self) -> Dict[str, Dict[str, int]]:
        by_status : Dict[str, int] = {s: 0 for s in STATUSES}
        by_family : Dict[str, Dict[str, int]] = {}
        for c in self._cases.values():
            status = c.primary_status()
            by_status[status] += 1
            fam = c.case_id.split('-')[0]
            by_family.setdefault(fam, {s: 0 for s in STATUSES})[status] += 1
        return {'by_status': by_status, 'by_family': by_family}

def _sort_key(case_id : str) -> tuple:
    family, number = case_id.split('-')
    order = {'MSG': 0, 'PAY': 1, 'ALLOC': 2, 'RUN': 3}
    return order.get(family, 9), int(number)

def _git_head() -> Optional[str]:
    try:
        out = subprocess.run(['git', 'rev-parse', 'HEAD'], capture_output = True, text = True,
                             check = False, timeout = 10)
    except (OSError, subprocess.TimeoutExpired):
        return None
    return out.stdout.strip() or None

def _git_dirty() -> Optional[bool]:
    try:
        out = subprocess.run(['git', 'status', '--porcelain', '--untracked-files=no'],
                             capture_output = True, text = True, check = False, timeout = 10)
    except (OSError, subprocess.TimeoutExpired):
        return None
    if out.returncode != 0:
        return None
    return bool(out.stdout.strip())
