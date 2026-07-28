'''
Host GPU inspection (videoflow/utils/system.py): ``visible_physical_gpus()``
intersects the system's devices with CUDA_VISIBLE_DEVICES (the local engine's
partition pool), and ``granted_gpus()`` renumbers that grant to valid CUDA
indices ``[0..n-1]`` — the component-facing side of the multi-GPU visibility
contract (RFC 0003).

Pure/unit: nvidia-smi is monkeypatched away — the probe tests stub
``subprocess.check_output`` to simulate a present, missing, unauthorized or
wedged binary.
'''
from __future__ import absolute_import, division, print_function

import logging
import subprocess

from videoflow.utils import system


def _fake_gpus(monkeypatch, n):
    monkeypatch.setattr(system, 'get_number_of_gpus', lambda: n)


def test_granted_gpus_defaults_to_all_system_devices(monkeypatch):
    _fake_gpus(monkeypatch, 3)
    monkeypatch.delenv('CUDA_VISIBLE_DEVICES', raising = False)
    assert system.granted_gpus() == [0, 1, 2]


def test_granted_gpus_are_cuda_indices_not_physical_ordinals(monkeypatch):
    _fake_gpus(monkeypatch, 4)
    monkeypatch.setenv('CUDA_VISIBLE_DEVICES', '2,0')
    assert system.granted_gpus() == [0, 1]


def test_granted_gpus_drops_junk_entries_with_a_warning(monkeypatch, caplog):
    _fake_gpus(monkeypatch, 2)
    monkeypatch.setenv('CUDA_VISIBLE_DEVICES', 'GPU-8a7b,1,7')
    with caplog.at_level(logging.WARNING, logger = 'videoflow.utils'):
        assert system.granted_gpus() == [0]
    assert 'GPU-8a7b' in caplog.text
    # '' is the standard hide-all idiom, not misconfiguration — no devices, no warning.
    caplog.clear()
    monkeypatch.setenv('CUDA_VISIBLE_DEVICES', '')
    with caplog.at_level(logging.WARNING, logger = 'videoflow.utils'):
        assert system.granted_gpus() == []
    assert not caplog.records


def test_no_nvidia_smi_means_no_gpus(monkeypatch):
    _fake_gpus(monkeypatch, 0)
    monkeypatch.delenv('CUDA_VISIBLE_DEVICES', raising = False)
    assert system.granted_gpus() == []


def test_visible_physical_gpus_defaults_to_all_system_devices(monkeypatch):
    _fake_gpus(monkeypatch, 3)
    monkeypatch.delenv('CUDA_VISIBLE_DEVICES', raising = False)
    assert system.visible_physical_gpus() == [0, 1, 2]


def test_visible_physical_gpus_keeps_physical_ordinals(monkeypatch):
    _fake_gpus(monkeypatch, 4)
    monkeypatch.setenv('CUDA_VISIBLE_DEVICES', '2,0')
    assert system.visible_physical_gpus() == [0, 2]


def test_visible_physical_gpus_warns_on_junk_but_not_out_of_range_entries(monkeypatch, caplog):
    _fake_gpus(monkeypatch, 2)
    monkeypatch.setenv('CUDA_VISIBLE_DEVICES', 'GPU-8a7b,1,7')
    with caplog.at_level(logging.WARNING, logger = 'videoflow.utils'):
        assert system.visible_physical_gpus() == [1]
    assert 'GPU-8a7b' in caplog.text
    # Out-of-range ordinals ('7' on a 2-GPU host) are dropped by the intersection and
    # a trailing comma's stray '' is harmless — both stay silent.
    caplog.clear()
    monkeypatch.setenv('CUDA_VISIBLE_DEVICES', '0,7,')
    with caplog.at_level(logging.WARNING, logger = 'videoflow.utils'):
        assert system.visible_physical_gpus() == [0]
    assert not caplog.records


def test_deprecated_alias_is_the_same_function(monkeypatch):
    _fake_gpus(monkeypatch, 1)
    monkeypatch.delenv('CUDA_VISIBLE_DEVICES', raising = False)
    assert system.get_gpus_available_to_process() == [0]
    assert system.get_gpus_available_to_process is system.granted_gpus


def _probe_raises(monkeypatch, exc):
    def fake_check_output(cmd, **kwargs):
        raise exc
    monkeypatch.setattr(system.subprocess, 'check_output', fake_check_output)


def test_gpu_count_passes_a_timeout_and_counts_physical_gpu_lines(monkeypatch):
    seen = {}

    def fake_check_output(cmd, **kwargs):
        seen.update(kwargs)
        return b'GPU 0: A100 (UUID: GPU-aaa)\nGPU 1: A100 (UUID: GPU-bbb)\n'

    monkeypatch.setattr(system.subprocess, 'check_output', fake_check_output)
    assert system.get_number_of_gpus() == 2
    assert seen['timeout'] == system.NVIDIA_SMI_TIMEOUT_SECONDS


def test_mig_instances_are_not_counted_as_gpus(monkeypatch):
    # nvidia-smi -L on a MIG host (R470+ driver): each instance prints an
    # indented 'MIG ... (UUID: MIG-...)' line; only the two physical cards count.
    def fake_check_output(cmd, **kwargs):
        return (b'GPU 0: NVIDIA A100-SXM4-40GB (UUID: GPU-5c8b8b28-51b2-4a55-a05c-81ac535f3f49)\n'
                b'  MIG 1g.5gb      Device  0: (UUID: MIG-26ad6681-e2a5-5f1e-b346-84af180d02f1)\n'
                b'  MIG 1g.5gb      Device  1: (UUID: MIG-57d90b34-9d15-5ba7-a0a4-4a4ef2c8f0d0)\n'
                b'GPU 1: NVIDIA A100-SXM4-40GB (UUID: GPU-9b4a4a2c-42a5-4d0d-9b2c-3a2f1e0d4c5b)\n')

    monkeypatch.setattr(system.subprocess, 'check_output', fake_check_output)
    assert system.get_number_of_gpus() == 2


def test_old_driver_mig_uuid_format_counts_one_gpu(monkeypatch):
    # R450-era MIG UUIDs were 'MIG-GPU-<parent>/<gi>/<ci>'. One parent line plus
    # one instance line is also exactly what a Kubernetes MIG-granted container
    # sees, so the count of 1 keeps granted_gpus() == [0] correct in that pod.
    def fake_check_output(cmd, **kwargs):
        return (b'GPU 0: A100-SXM4-40GB (UUID: GPU-e91a5e5a-4a2f-4a1c-8f0e-1b2c3d4e5f60)\n'
                b'  MIG 1g.5gb Device 0: (UUID: MIG-GPU-e91a5e5a-4a2f-4a1c-8f0e-1b2c3d4e5f60/1/0)\n')

    monkeypatch.setattr(system.subprocess, 'check_output', fake_check_output)
    assert system.get_number_of_gpus() == 1


def test_wedged_nvidia_smi_degrades_to_zero_gpus_with_a_warning(monkeypatch, caplog):
    _probe_raises(monkeypatch,
                  subprocess.TimeoutExpired(['nvidia-smi', '-L'], system.NVIDIA_SMI_TIMEOUT_SECONDS))
    with caplog.at_level(logging.WARNING, logger = 'videoflow.utils'):
        assert system.get_number_of_gpus() == 0
    assert 'assuming 0 GPUs' in caplog.text


def test_unreadable_nvidia_smi_degrades_to_zero_gpus_with_a_warning(monkeypatch, caplog):
    _probe_raises(monkeypatch, PermissionError(13, 'Permission denied'))
    with caplog.at_level(logging.WARNING, logger = 'videoflow.utils'):
        assert system.get_number_of_gpus() == 0
    assert 'assuming 0 GPUs' in caplog.text


def test_missing_nvidia_smi_is_zero_gpus_and_silent(monkeypatch, caplog):
    _probe_raises(monkeypatch, FileNotFoundError(2, 'No such file or directory'))
    with caplog.at_level(logging.WARNING, logger = 'videoflow.utils'):
        assert system.get_number_of_gpus() == 0
    assert not caplog.records
