import pytest

import videoflow.utils.system as system

def test_gpus_available_1(monkeypatch):
    monkeypatch.setenv('CUDA_VISIBLE_DEVICES', '')
    gpus = system.get_gpus_available_to_process()
    assert len(gpus) == 0

    monkeypatch.setenv('CUDA_VISIBLE_DEVICES', '0')
    gpus = system.get_gpus_available_to_process()
    assert len(gpus) == 0

def test_gpus_available_2(monkeypatch):
    def get_system_gpus_mock():
        return set([0])
    
    monkeypatch.setattr(system, 'get_system_gpus', get_system_gpus_mock)
    monkeypatch.setenv('CUDA_VISIBLE_DEVICES', '')
    gpus = system.get_gpus_available_to_process()
    assert len(gpus) == 0

    monkeypatch.setenv('CUDA_VISIBLE_DEVICES', '0')
    gpus = system.get_gpus_available_to_process()
    assert len(gpus) == 1
    assert 0 in gpus

def test_gpus_available_3(monkeypatch):
    def get_system_gpus_mock():
        return set([0, 1])
    
    monkeypatch.setattr(system, 'get_system_gpus', get_system_gpus_mock)
    monkeypatch.setenv('CUDA_VISIBLE_DEVICES', '1, 2')
    gpus = system.get_gpus_available_to_process()
    assert len(gpus) == 1
    assert 1 in gpus

    monkeypatch.setenv('CUDA_VISIBLE_DEVICES', '2, 3')
    gpus = system.get_gpus_available_to_process()
    assert len(gpus) == 0

    monkeypatch.setenv('CUDA_VISIBLE_DEVICES', 'asdfa, 1, 0, asdf')
    gpus = system.get_gpus_available_to_process()
    assert len(gpus) == 2


if __name__ == "__main__":
    pytest.main([__file__])
