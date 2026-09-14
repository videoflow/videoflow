'''
Asset identity at open time (videoflow/runtime/assetcheck.py, RUN-033): a node's
``required_assets()`` are verified by content digest before the worker opens it;
a missing or different file is an explicit ``ResourceUnavailable`` whose remedy
tells a local-only hostPath from a portable asset delivered wrong.
'''
from __future__ import absolute_import, division, print_function

import hashlib

import pytest

from videoflow.core.errors import ResourceUnavailable
from videoflow.core.node import AssetRequirement, ProcessorNode
from videoflow.runtime.assetcheck import check_assets, verify_assets, verify_node_assets


def _write(path, data : bytes) -> str:
    path.write_bytes(data)
    return hashlib.sha256(data).hexdigest()


def test_verified_assets_pass_and_report(tmp_path):
    model = tmp_path / 'model.bin'
    digest = _write(model, b'weights')
    reports = verify_assets([AssetRequirement(str(model), digest), AssetRequirement(str(model), 'sha256:' + digest.upper())])
    assert [r.status for r in reports] == ['verified', 'verified']


def test_missing_local_only_asset_names_the_host_and_the_placement_fix(tmp_path):
    with pytest.raises(ResourceUnavailable) as excinfo:
        verify_assets([AssetRequirement(str(tmp_path / 'absent.bin'), 'ab' * 32, portable = False)], node_name = 'det')
    assert 'is missing' in str(excinfo.value) and 'local-only' in excinfo.value.remedy


def test_changed_portable_bytes_are_a_mismatch_not_a_pass(tmp_path):
    model = tmp_path / 'model.bin'
    _write(model, b'other bytes at the same path')
    declared = hashlib.sha256(b'weights').hexdigest()
    assert check_assets([AssetRequirement(str(model), declared)])[0].status == 'mismatch'
    with pytest.raises(ResourceUnavailable) as excinfo:
        verify_assets([AssetRequirement(str(model), declared)])
    assert 'not the declared' in str(excinfo.value) and 'distribution' in excinfo.value.remedy


def test_nodes_declare_nothing_by_default_and_may_declare_from_their_params(tmp_path):
    class Plain(ProcessorNode):
        def process(self, item):   # type: ignore[override]
            return item

    class Model(ProcessorNode):
        def __init__(self, model_path : str, model_sha256 : str, **kwargs):
            self._model_path = model_path
            self._model_sha256 = model_sha256
            super().__init__(**kwargs)

        def required_assets(self):
            return [AssetRequirement(self._model_path, self._model_sha256)]

        def process(self, item):   # type: ignore[override]
            return item

    assert verify_node_assets(Plain(name = 'p')) == []
    model = tmp_path / 'm.bin'
    digest = _write(model, b'm')
    assert verify_node_assets(Model(str(model), digest, name = 'm'))[0].status == 'verified'
    with pytest.raises(ResourceUnavailable):
        verify_node_assets(Model(str(model), '0' * 64, name = 'm'))
