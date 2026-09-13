'''
A stdlib client for the toxiproxy control API (``docker compose --profile toxiproxy``),
enough for the fault schedules the broker-level cases need: add a toxic to the
``nats`` or ``redis`` proxy (latency, a data cap, a stalled connection), disable
a proxy outright to sever it, and put everything back afterwards. No third-party
dependency: the test tree must import without the optional extras installed.

toxiproxy 2.9.0 API (https://github.com/Shopify/toxiproxy#http-api): proxies are
``GET/POST /proxies``, one proxy is ``GET/POST/DELETE /proxies/{name}``, toxics are
``GET/POST /proxies/{name}/toxics`` and ``DELETE /proxies/{name}/toxics/{toxic}``;
``POST /reset`` enables every proxy and removes every toxic.
'''
from __future__ import absolute_import, division, print_function

import json
import urllib.error
import urllib.request
from typing import Any, Dict, List, Optional


class Toxiproxy:
    '''One control endpoint. Every call raises ``urllib.error.HTTPError`` on a refused request.'''
    def __init__(self, url : str, timeout : float = 5.0) -> None:
        self._url = url.rstrip('/')
        self._timeout = timeout

    def _call(self, method : str, path : str, body : Optional[Dict[str, Any]] = None) -> Any:
        data = None if body is None else json.dumps(body).encode()
        req = urllib.request.Request(self._url + path, data = data, method = method,
                                     headers = {'Content-Type': 'application/json'} if data else {})
        with urllib.request.urlopen(req, timeout = self._timeout) as resp:
            raw = resp.read()
        return json.loads(raw) if raw else None

    def proxies(self) -> Dict[str, Any]:
        return self._call('GET', '/proxies')

    def ensure(self, name : str, listen : str, upstream : str) -> None:
        '''Create ``name`` when absent; an existing proxy with the same name is left as it is.'''
        if name in self.proxies():
            return
        self._call('POST', '/proxies', {'name': name, 'listen': listen, 'upstream': upstream, 'enabled': True})

    def enable(self, name : str, enabled : bool = True) -> None:
        '''A disabled proxy closes its listener and every live connection: the upstream is unreachable.'''
        self._call('POST', f'/proxies/{name}', {'enabled': enabled})

    def add_toxic(self, proxy : str, kind : str, attributes : Dict[str, Any], name : Optional[str] = None,
                  stream : str = 'downstream', toxicity : float = 1.0) -> str:
        '''
        Attach a toxic and return its name. Kinds the cases use: ``latency``
        (``latency``, ``jitter`` ms), ``timeout`` (``timeout`` ms; 0 stalls until
        removed), ``limit_data`` (``bytes``: close after that many), ``bandwidth``
        (``rate`` KB/s), ``slicer``. ``stream`` is ``upstream`` (client → server) or
        ``downstream`` (server → client).
        '''
        toxic_name = name or f'{kind}_{stream}'
        self._call('POST', f'/proxies/{proxy}/toxics', {
            'name': toxic_name, 'type': kind, 'stream': stream, 'toxicity': toxicity, 'attributes': attributes,
        })
        return toxic_name

    def remove_toxic(self, proxy : str, name : str) -> None:
        try:
            self._call('DELETE', f'/proxies/{proxy}/toxics/{name}')
        except urllib.error.HTTPError as e:
            if e.code != 404:
                raise

    def toxics(self, proxy : str) -> List[Dict[str, Any]]:
        return self._call('GET', f'/proxies/{proxy}/toxics')

    def reset(self) -> None:
        '''Enable every proxy and remove every toxic — the fixture's teardown.'''
        self._call('POST', '/reset')
