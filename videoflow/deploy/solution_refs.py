'''
Solution references: ``<repo>://<name>[:factory]`` names a solution shipped in
a videoflow repository — ``videoflow-contrib://human_tracking`` is
``solutions/human_tracking/human_tracking.py`` in
https://github.com/videoflow/videoflow-contrib — so a PyPI install can run and
deploy the shipped solutions without cloning anything by hand.

Resolution is a one-time shallow clone of the repository at the tag matching the
installed videoflow version (the two repos release in lockstep, so ``vX.Y.Z``
exists in both) into ``~/.videoflow/solutions/<repo>@vX.Y.Z``, reused afterwards.
The tag is immutable, so the cache never refreshes itself: ``rm -rf`` the
directory to refetch. The solution's ``config.yaml`` and outputs live inside that
directory, next to the graph, exactly as they would in a checkout.

Two environment variables, both machine concerns rather than deploy flags:
``VIDEOFLOW_SOLUTIONS_CACHE`` moves the cache root, and ``VF_SOLUTION_REF``
overrides the git ref (``master`` to run the solutions of an unreleased core; a
branch is mutable, so the cache will not track it).
'''
from __future__ import annotations

import os
import re
import shutil
import subprocess
import sys
from dataclasses import dataclass
from typing import Optional, Tuple

import videoflow

from ..core.errors import ConfigError, ResourceUnavailable

SOLUTIONS_CACHE_ENV = 'VIDEOFLOW_SOLUTIONS_CACHE'
SOLUTION_REF_ENV = 'VF_SOLUTION_REF'
GITHUB_ORG = 'https://github.com/videoflow'

# One path segment each side of ``://``: no slashes and no ``..``, so a ref can
# never escape the cache root or the repository's solutions/ directory.
_REF_RE = re.compile(r'^(?P<repo>[A-Za-z0-9][A-Za-z0-9_.-]*)://'
                     r'(?P<name>[A-Za-z0-9][A-Za-z0-9_.-]*)'
                     r'(?::(?P<factory>[A-Za-z_]\w*))?$')


@dataclass(frozen = True)
class ResolvedSolution:
    graph_path : str      # <checkout>/solutions/<name>/<name>.py
    factory : Optional[str]
    build_context : str   # the checkout root: solution Dockerfiles COPY sibling packages


def default_cache_root() -> str:
    return os.environ.get(SOLUTIONS_CACHE_ENV,
                          os.path.join(os.path.expanduser('~'), '.videoflow', 'solutions'))


def parse_solution_ref(arg : str) -> Optional[Tuple[str, str, Optional[str]]]:
    '''``(repo, name, factory)`` for a ``<repo>://<name>[:factory]`` ref, or None for anything else (a path).'''
    match = _REF_RE.match(arg)
    if match is None or '..' in (match.group('repo'), match.group('name')):
        return None
    return match.group('repo'), match.group('name'), match.group('factory')


def solution_git_ref() -> str:
    '''The git ref solutions are fetched at: ``VF_SOLUTION_REF``, else the tag of the installed version.'''
    return os.environ.get(SOLUTION_REF_ENV) or f'v{videoflow.__version__}'


def resolve_solution_ref(arg : str, cache_root : Optional[str] = None) -> ResolvedSolution:
    '''
    Fetch (once) the repository a ``<repo>://<name>`` ref points at and locate the
    solution's graph module inside it.

    - Raises:
        - ``ConfigError``: not a solution ref, or the repository has no such solution.
        - ``ResourceUnavailable``: git is missing, or the clone failed (no such tag \
            for this version, no network, ...). Nothing is left behind in the cache.
    '''
    parsed = parse_solution_ref(arg)
    if parsed is None:
        raise ConfigError(f'{arg!r} is not a solution reference',
                          remedy = 'Expected <repo>://<name>, e.g. videoflow-contrib://human_tracking.')
    repo, name, factory = parsed
    ref = solution_git_ref()
    root = cache_root or default_cache_root()
    # '@' rather than ':' in the directory name: the graph dir is later passed
    # through parse_mounts, which splits mount specs on ':'.
    checkout = os.path.join(root, f'{repo}@{ref}')
    if not os.path.isdir(checkout):
        _clone(repo, ref, checkout)

    solutions_dir = os.path.join(checkout, 'solutions')
    graph_path = os.path.join(solutions_dir, name, f'{name}.py')
    if not os.path.isfile(graph_path):
        available = sorted(d for d in os.listdir(solutions_dir)
                           if os.path.isfile(os.path.join(solutions_dir, d, f'{d}.py'))) \
            if os.path.isdir(solutions_dir) else []
        raise ConfigError(f'{repo} {ref} has no solution {name!r}',
                          remedy = ('Available: ' + ', '.join(available)) if available
                          else f'{repo} {ref} ships no solutions/ directory.')
    print(f'Using {repo}://{name} from {os.path.dirname(graph_path)} '
          f'(config.yaml and outputs are kept there).', file = sys.stderr)
    return ResolvedSolution(graph_path = graph_path, factory = factory, build_context = checkout)


def _clone(repo : str, ref : str, checkout : str) -> None:
    url = f'{GITHUB_ORG}/{repo}'
    manual = f'git clone {url} && videoflow run-local {repo}/solutions/<name>/<name>.py'
    # Clone into a sibling temp dir and rename on success, so an interrupted clone
    # never leaves a directory the next run would take for a complete checkout.
    tmp = f'{checkout}.tmp-{os.getpid()}'
    os.makedirs(os.path.dirname(checkout), exist_ok = True)
    shutil.rmtree(tmp, ignore_errors = True)
    print(f'Fetching {url} at {ref} (one-time)...', file = sys.stderr)
    cmd = ['git', 'clone', '--depth', '1', '--branch', ref,
           '--config', 'advice.detachedHead=false', url, tmp]
    try:
        proc = subprocess.run(cmd, capture_output = True, text = True, check = False)
    except FileNotFoundError:
        raise ResourceUnavailable(
            f'git is not on PATH, so {repo}://... cannot be fetched.',
            remedy = f'Install git, or clone by hand and pass the path: {manual}') from None
    if proc.returncode != 0:
        shutil.rmtree(tmp, ignore_errors = True)
        detail = (proc.stderr or proc.stdout).strip().splitlines()
        reason = detail[-1] if detail else f'git exited with {proc.returncode}'
        if 'not found' in reason.lower() and os.environ.get(SOLUTION_REF_ENV):
            remedy = (f'{SOLUTION_REF_ENV}={ref} names a tag or branch {url} does not have. '
                      f'Set it to an existing one, or unset it to use the v{videoflow.__version__} tag.')
        elif 'not found' in reason.lower():
            remedy = (f'There is no {ref} tag in {url}: videoflow {videoflow.__version__} is a '
                      f'development or pre-release version. Clone by hand ({manual}), '
                      f'or set {SOLUTION_REF_ENV}=master to fetch the current branch instead.')
        else:
            remedy = f'Check the network and that {url} is reachable, or clone by hand: {manual}'
        raise ResourceUnavailable(f'could not fetch {repo} at {ref}: {reason}', remedy = remedy)
    os.rename(tmp, checkout)
