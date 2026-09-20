'''
``<repo>://<name>`` solution references: parsing, the one-time shallow clone at
the installed version's tag, cache reuse, and the errors for a missing tag,
missing git, or unknown solution.

Pure/unit: subprocess is monkeypatched — no git, no network.
'''
import os
import subprocess

import pytest

import videoflow
from videoflow.core.errors import ConfigError, ResourceUnavailable
from videoflow.deploy import solution_refs as refs


class _Proc:
    def __init__(self, returncode = 0, stdout = '', stderr = ''):
        self.returncode = returncode
        self.stdout = stdout
        self.stderr = stderr


@pytest.fixture
def cache(monkeypatch, tmp_path):
    monkeypatch.setenv(refs.SOLUTIONS_CACHE_ENV, str(tmp_path))
    monkeypatch.delenv(refs.SOLUTION_REF_ENV, raising = False)
    monkeypatch.setattr(videoflow, '__version__', '1.0.2')
    return tmp_path


def _stage(root, name):
    d = root / 'solutions' / name
    d.mkdir(parents = True)
    (d / f'{name}.py').write_text('def build_flow(): ...\n')


def _never_run(monkeypatch):
    def run(cmd, **kwargs):
        raise AssertionError(f'unexpected subprocess: {cmd}')
    monkeypatch.setattr(subprocess, 'run', run)


# -- parsing ----------------------------------------------------------------

@pytest.mark.parametrize('arg, expected', [
    ('videoflow-contrib://human_tracking', ('videoflow-contrib', 'human_tracking', None)),
    ('videoflow://toy_calculator:build_flow', ('videoflow', 'toy_calculator', 'build_flow')),
    ('solutions/x/x.py', None),
    ('x.py:build_flow', None),
    ('oci://ghcr.io/a/b', None),          # a slash in the name: not a solution ref
    ('videoflow://../x', None),
    ('a/b://x', None),
    ('://x', None),
])
def test_parse_ref_forms(arg, expected):
    assert refs.parse_solution_ref(arg) == expected


def test_resolve_rejects_non_refs(cache, monkeypatch):
    _never_run(monkeypatch)
    with pytest.raises(ConfigError):
        refs.resolve_solution_ref('solutions/x/x.py')


# -- cache ------------------------------------------------------------------

def test_cache_hit_never_clones(cache, monkeypatch, capsys):
    checkout = cache / 'videoflow-contrib@v1.0.2'
    _stage(checkout, 'human_tracking')
    _never_run(monkeypatch)
    r = refs.resolve_solution_ref('videoflow-contrib://human_tracking:build_flow')
    assert r.graph_path == str(checkout / 'solutions' / 'human_tracking' / 'human_tracking.py')
    assert r.factory == 'build_flow'
    assert r.build_context == str(checkout)
    assert 'Using videoflow-contrib://human_tracking from' in capsys.readouterr().err


def test_cache_miss_clones_shallow_at_tag(cache, monkeypatch):
    calls = []

    def run(cmd, **kwargs):
        calls.append(list(cmd))
        _stage(type(cache)(cmd[-1]), 'human_tracking')   # what a real clone would leave
        return _Proc()

    monkeypatch.setattr(subprocess, 'run', run)
    r = refs.resolve_solution_ref('videoflow-contrib://human_tracking')
    checkout = cache / 'videoflow-contrib@v1.0.2'
    assert len(calls) == 1
    tmp = calls[0][-1]
    assert calls[0] == ['git', 'clone', '--depth', '1', '--branch', 'v1.0.2',
                        '--config', 'advice.detachedHead=false',
                        'https://github.com/videoflow/videoflow-contrib', tmp]
    assert tmp.startswith(str(checkout) + '.tmp-')
    assert not os.path.exists(tmp)                    # renamed into place
    assert r.build_context == str(checkout)
    assert os.path.isfile(r.graph_path)


def test_ref_env_override(cache, monkeypatch):
    monkeypatch.setenv(refs.SOLUTION_REF_ENV, 'master')
    calls = []

    def run(cmd, **kwargs):
        calls.append(list(cmd))
        _stage(type(cache)(cmd[-1]), 'toy_calculator')
        return _Proc()

    monkeypatch.setattr(subprocess, 'run', run)
    r = refs.resolve_solution_ref('videoflow://toy_calculator')
    assert '--branch' in calls[0] and calls[0][calls[0].index('--branch') + 1] == 'master'
    assert r.build_context == str(cache / 'videoflow@master')


# -- errors -----------------------------------------------------------------

def test_missing_tag_is_resource_unavailable_with_manual_remedy(cache, monkeypatch):
    def run(cmd, **kwargs):
        return _Proc(returncode = 128,
                     stderr = 'Cloning into ...\nfatal: Remote branch v1.0.2 not found in upstream origin\n')

    monkeypatch.setattr(subprocess, 'run', run)
    with pytest.raises(ResourceUnavailable) as e:
        refs.resolve_solution_ref('videoflow-contrib://human_tracking')
    text = str(e.value)
    assert 'Remote branch v1.0.2 not found' in text
    assert 'git clone https://github.com/videoflow/videoflow-contrib' in text
    assert 'VF_SOLUTION_REF=master' in text
    assert not any(p.name.startswith('videoflow-contrib@') for p in cache.iterdir())


def test_bad_ref_override_is_blamed_on_the_override(cache, monkeypatch):
    monkeypatch.setenv(refs.SOLUTION_REF_ENV, 'v9.9.9')

    def run(cmd, **kwargs):
        return _Proc(returncode = 128, stderr = 'fatal: Remote branch v9.9.9 not found in upstream origin\n')

    monkeypatch.setattr(subprocess, 'run', run)
    with pytest.raises(ResourceUnavailable) as e:
        refs.resolve_solution_ref('videoflow://toy_calculator')
    assert 'VF_SOLUTION_REF=v9.9.9 names a tag or branch' in str(e.value)
    assert 'unset it to use the v1.0.2 tag' in str(e.value)


def test_missing_git_is_resource_unavailable(cache, monkeypatch):
    def run(cmd, **kwargs):
        raise FileNotFoundError('git')

    monkeypatch.setattr(subprocess, 'run', run)
    with pytest.raises(ResourceUnavailable, match = 'git is not on PATH'):
        refs.resolve_solution_ref('videoflow-contrib://human_tracking')


def test_unknown_solution_lists_available(cache, monkeypatch):
    checkout = cache / 'videoflow@v1.0.2'
    _stage(checkout, 'toy_calculator')
    _stage(checkout, 'toy_router')
    _never_run(monkeypatch)
    with pytest.raises(ConfigError) as e:
        refs.resolve_solution_ref('videoflow://nope')
    assert "no solution 'nope'" in str(e.value)
    assert 'toy_calculator, toy_router' in str(e.value)
