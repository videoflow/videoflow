'''
``RuntimeStore`` implementations that need no server: an in-memory one for
models, and a file-directory one that survives a process restart on a single
host (the local engine's default), each with compare-and-swap semantics.
'''
from __future__ import absolute_import, division, print_function

import fcntl
import json
import os
import threading
from typing import Any

from ..capabilities import RuntimeCapabilities
from ..outcomes import known
from ..runtime import RuntimeStore


class MemoryRuntimeStore(RuntimeStore):
    def __init__(self) -> None:
        self._data : dict[str, tuple[bytes, int]] = {}
        self._logs : dict[str, list[bytes]] = {}
        self._lock = threading.Lock()

    def get(self, key : str) -> tuple[bytes | None, str | None]:
        with self._lock:
            entry = self._data.get(key)
            return (None, None) if entry is None else (entry[0], str(entry[1]))

    def cas(self, key : str, expected_version : str | None, value : bytes) -> bool:
        with self._lock:
            entry = self._data.get(key)
            current = None if entry is None else str(entry[1])
            if current != expected_version:
                return False
            self._data[key] = (value, (entry[1] + 1) if entry else 1)
            return True

    def append(self, log : str, record : bytes) -> int:
        with self._lock:
            entries = self._logs.setdefault(log, [])
            entries.append(record)
            return len(entries)

    def scan(self, prefix : str) -> list[tuple[str, bytes, str]]:
        with self._lock:
            return [(k, v[0], str(v[1])) for k, v in sorted(self._data.items()) if k.startswith(prefix)]

    def delete(self, key : str, expected_version : str | None) -> bool:
        with self._lock:
            entry = self._data.get(key)
            if entry is None:
                return expected_version is None
            if expected_version is not None and str(entry[1]) != expected_version:
                return False
            del self._data[key]
            return True

    def log_entries(self, log : str) -> list[bytes]:
        with self._lock:
            return list(self._logs.get(log, ()))

    def capabilities(self) -> RuntimeCapabilities:
        return RuntimeCapabilities('memory', durable = known(False), shared_across_processes = False,
                                   restart_safe_joins = False, elastic_state = False)

class FileRuntimeStore(RuntimeStore):
    '''
    One JSON file per key under ``root`` (``<key>.json`` with slashes encoded),
    versioned by a counter inside the file, with an ``fcntl`` lock per key for
    compare-and-swap across the processes of one host. Durable across process
    restarts; not shared across hosts.
    '''
    def __init__(self, root : str) -> None:
        self._root = root
        os.makedirs(root, exist_ok = True)

    def _path(self, key : str) -> str:
        return os.path.join(self._root, key.replace('/', '%2F') + '.json')

    def _read(self, path : str) -> tuple[bytes | None, int]:
        if not os.path.exists(path):
            return None, 0
        with open(path) as f:
            doc = json.load(f)
        return bytes.fromhex(doc['value']), int(doc['version'])

    def _write(self, path : str, value : bytes, version : int) -> None:
        tmp = path + '.tmp'
        with open(tmp, 'w') as f:
            json.dump({'value': value.hex(), 'version': version}, f)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)

    def _locked(self, key : str) -> Any:
        lock_path = self._path(key) + '.lock'
        handle = open(lock_path, 'w')
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        return handle

    def get(self, key : str) -> tuple[bytes | None, str | None]:
        handle = self._locked(key)
        try:
            value, version = self._read(self._path(key))
            return (None, None) if value is None else (value, str(version))
        finally:
            handle.close()

    def cas(self, key : str, expected_version : str | None, value : bytes) -> bool:
        handle = self._locked(key)
        try:
            current, version = self._read(self._path(key))
            have = None if current is None else str(version)
            if have != expected_version:
                return False
            self._write(self._path(key), value, version + 1)
            return True
        finally:
            handle.close()

    def append(self, log : str, record : bytes) -> int:
        handle = self._locked('log:' + log)
        try:
            path = os.path.join(self._root, log.replace('/', '%2F') + '.log')
            with open(path, 'ab') as f:
                f.write(record.hex().encode() + b'\n')
                f.flush()
                os.fsync(f.fileno())
            with open(path, 'rb') as f:
                return sum(1 for _ in f)
        finally:
            handle.close()

    def scan(self, prefix : str) -> list[tuple[str, bytes, str]]:
        rows = []
        for entry in sorted(os.listdir(self._root)):
            if not entry.endswith('.json'):
                continue
            key = entry[:-5].replace('%2F', '/')
            if not key.startswith(prefix):
                continue
            value, version = self._read(os.path.join(self._root, entry))
            if value is not None:
                rows.append((key, value, str(version)))
        return rows

    def delete(self, key : str, expected_version : str | None) -> bool:
        handle = self._locked(key)
        try:
            path = self._path(key)
            current, version = self._read(path)
            if current is None:
                return expected_version is None
            if expected_version is not None and str(version) != expected_version:
                return False
            os.remove(path)
            return True
        finally:
            handle.close()

    def log_entries(self, log : str) -> list[bytes]:
        path = os.path.join(self._root, log.replace('/', '%2F') + '.log')
        if not os.path.exists(path):
            return []
        with open(path, 'rb') as f:
            return [bytes.fromhex(line.strip().decode()) for line in f if line.strip()]

    def capabilities(self) -> RuntimeCapabilities:
        return RuntimeCapabilities('file', durable = known(True), shared_across_processes = True,
                                   restart_safe_joins = True, elastic_state = False)
