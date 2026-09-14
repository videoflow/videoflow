'''
An append-only observation log — the independent evidence a conformance oracle
reads instead of trusting a provider's own counters.

The design package asks every adapter and the runtime to record, for each
logical operation, the identities that let a failure be attributed: the operation
id, the source epoch and offset, the event or group id, the logical consumer, the
delivery attempt, the ownership generation, the payload digest or obligation, the
claim and pod UIDs, the device ids, and a monotonic timestamp. Image bytes are
never logged. This module is deliberately tiny: a list of frozen records, a lock,
and JSONL in/out, so it can be attached to a worker subprocess through a file
path and merged back in the test that spawned it.
'''
from __future__ import absolute_import, division, print_function

import json
import threading
import time
from dataclasses import dataclass, field
from typing import Any, Iterable, Iterator, Mapping

#: Field names every record may carry; adapters use these keys so logs from
#: different backends line up in one oracle.
FIELDS = ('op_id', 'source_epoch', 'source_offset', 'event_id', 'group_id', 'consumer',
          'attempt', 'generation', 'payload_digest', 'obligation_id', 'claim_uid',
          'pod_uid', 'device_ids', 'status', 'reason')

@dataclass(frozen = True)
class Event:
    '''One observation. ``kind`` names what happened (``'publish'``, ``'settle'``, ``'claim'`` …).'''
    kind : str
    monotonic : float
    wall : float
    fields : Mapping[str, Any] = field(default_factory = dict)

    def as_dict(self) -> dict[str, Any]:
        return {'kind': self.kind, 'monotonic': self.monotonic, 'wall': self.wall, **self.fields}

class ObservationLog:
    '''
    Thread-safe, append-only, replayable.

    - Arguments:
        - path: when given, every event is also appended as one JSON line, so a \
            subprocess can keep writing while its parent still reads.
        - clock: monotonic clock, injectable for deterministic tests.
    '''
    def __init__(self, path : str | None = None, clock : Any = time.monotonic) -> None:
        self._events : list[Event] = []
        self._lock = threading.Lock()
        self._path = path
        self._clock = clock

    def emit(self, kind : str, **fields : Any) -> Event:
        for key, value in fields.items():
            if isinstance(value, (bytes, bytearray, memoryview)):
                raise TypeError(f'observation field {key!r} carries raw bytes; log a digest instead')
        event = Event(kind, float(self._clock()), time.time(), dict(fields))
        with self._lock:
            self._events.append(event)
            if self._path:
                with open(self._path, 'a') as f:
                    f.write(json.dumps(event.as_dict(), sort_keys = True, default = str) + '\n')
        return event

    def events(self, kind : str | None = None, **match : Any) -> list[Event]:
        '''Events of ``kind`` (any kind when None) whose fields equal every ``match`` item.'''
        with self._lock:
            snapshot = list(self._events)
        return [e for e in snapshot
                if (kind is None or e.kind == kind)
                and all(e.fields.get(k) == v for k, v in match.items())]

    def __iter__(self) -> Iterator[Event]:
        return iter(self.events())

    def __len__(self) -> int:
        with self._lock:
            return len(self._events)

    def extend(self, events : Iterable[Event]) -> None:
        '''Merge events recorded elsewhere (a subprocess log), keeping monotonic order per source.'''
        with self._lock:
            self._events.extend(events)

    @staticmethod
    def load(path : str) -> list[Event]:
        events : list[Event] = []
        with open(path) as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                record = json.loads(line)
                events.append(Event(record.pop('kind'), float(record.pop('monotonic')),
                                    float(record.pop('wall')), record))
        return events
