'''
Solution conventions for one-command deploys. A "solution" is a graph module
shipped with optional sibling files:

  - ``config.template.yaml`` — a valid config plus two extension blocks stripped
    on write: ``x-questions`` (inputs deploy asks for interactively when no
    config exists) and ``x-mounts`` (paths from the resolved config that must be
    hostPath-mounted into prep containers and worker pods).
  - ``prepare.py`` — an idempotent prep hook deploy runs inside the solution
    image before compiling (its outputs get baked into the compiled specs).

``x-questions`` entries: ``{key, prompt, type, default, choices, item_key,
item_value}``. ``key`` is a dotted path into the config (digit segments index
int-keyed maps). Types: ``str`` (default), ``int``, ``float``, ``choice`` (with
``choices``), ``path`` (one filesystem path, validated and absolutized), and
``paths`` (comma-separated paths expanded into a mapping via ``item_key``,
e.g. ``'cam{i}'``, and ``item_value``, e.g. ``{video: '{path}'}``).

``x-mounts`` entries are path templates: ``'{cameras.*.video}:ro'`` (dotted
lookup into the resolved config, ``*`` fans out), ``'{work_dir}'``,
``'~/.videoflow:/root/.videoflow'``. A single path resolves to a same-path
hostPath mount (host and container see the same absolute path — required
because the paths baked into node params at compile time must resolve
identically in the pods); a ``host:container`` pair maps them explicitly
(e.g. the operator's home caches onto the container root's).
'''
from __future__ import absolute_import, division, print_function

import os
from typing import List, Optional

import yaml

X_QUESTIONS = 'x-questions'
X_MOUNTS = 'x-mounts'
TEMPLATE_NAME = 'config.template.yaml'
CONFIG_NAME = 'config.yaml'

def find_template(graph_dir) -> Optional[str]:
    path = os.path.join(graph_dir, TEMPLATE_NAME)
    return path if os.path.isfile(path) else None

def load_template(template_path) -> dict:
    with open(template_path) as f:
        return yaml.safe_load(f)

def _seg(segment):
    return int(segment) if segment.isdigit() else segment

def _set_dotted(config, dotted, value) -> None:
    parts = [_seg(p) for p in dotted.split('.')]
    target = config
    for part in parts[:-1]:
        target = target.setdefault(part, {})
    target[parts[-1]] = value

def _get_dotted(config, parts) -> list:
    '''Resolves a dotted path (already split) against nested dicts; ``*`` fans out. Returns all matches.'''
    values = [config]
    for part in parts:
        next_values: list = []
        for value in values:
            if not isinstance(value, dict):
                continue
            if part == '*':
                next_values.extend(value.values())
            elif _seg(part) in value:
                next_values.append(value[_seg(part)])
            elif part in value:
                next_values.append(value[part])
        values = next_values
    return values

def _abs_existing(path, base_dir):
    resolved = os.path.abspath(os.path.join(base_dir, os.path.expanduser(str(path))))
    if not os.path.exists(resolved):
        raise ValueError(f'no such file: {resolved}')
    return resolved

def _coerce(question, answer, base_dir):
    qtype = question.get('type', 'str')
    if qtype == 'float':
        return float(answer)
    if qtype == 'int':
        return int(answer)
    if qtype == 'choice':
        choices = [str(c) for c in question['choices']]
        if str(answer) not in choices:
            raise ValueError(f'must be one of: {", ".join(choices)}')
        return answer
    if qtype == 'path':
        # A single filesystem path, validated and absolutized (scalar config key).
        return _abs_existing(answer, base_dir)
    if qtype == 'paths':
        paths = [p.strip() for p in str(answer).replace(',', ' ').split() if p.strip()]
        if not paths:
            raise ValueError('at least one path is required')
        resolved = [_abs_existing(p, base_dir) for p in paths]
        item_key = question.get('item_key', '{i}')
        item_value = question.get('item_value', '{path}')
        result = {}
        for i, p in enumerate(resolved):
            result[item_key.format(i = i)] = _fill_paths(item_value, p)
        return result
    return answer

def _fill_paths(template_value, path):
    if isinstance(template_value, dict):
        return {k: _fill_paths(v, path) for k, v in template_value.items()}
    if isinstance(template_value, str):
        return template_value.format(path = path)
    return template_value

def ask_questions(questions, base_dir, input_fn = input) -> dict:
    '''Prompts for each x-question on the terminal; returns {dotted_key: typed_value}.'''
    answers = {}
    for question in questions:
        default = question.get('default')
        suffix = f' [{default}]' if default is not None else ''
        if question.get('type') == 'choice':
            suffix = f' ({"/".join(str(c) for c in question["choices"])}){suffix}'
        while True:
            raw = input_fn(f'{question["prompt"]}{suffix}: ').strip()
            if not raw:
                if default is None:
                    print('A value is required.')
                    continue
                raw = str(default)
            try:
                answers[question['key']] = _coerce(question, raw, base_dir)
                break
            except ValueError as e:
                print(f'Invalid value: {e}')
    return answers

def render_config(template, answers) -> str:
    '''The final config YAML: template body with answers applied and x-* blocks stripped.'''
    config = {k: v for k, v in template.items() if k not in (X_QUESTIONS, X_MOUNTS)}
    for dotted, value in answers.items():
        _set_dotted(config, dotted, value)
    header = f'# Generated by videoflow deploy from {TEMPLATE_NAME} — see that file for docs.\n'
    return header + yaml.dump(config, default_flow_style = False, sort_keys = False)

def ensure_config(graph_dir, config_arg = None, interactive = True, input_fn = input) -> Optional[str]:
    '''
    The config file deploy should use: an explicit ``--config``, an existing
    ``config.yaml`` next to the graph, or — when the solution ships a template —
    one generated by asking the template's questions. None when the solution has
    no config convention at all.

    - Raises:
        - ``SystemExit`` listing every question when a config must be generated \
            but the session is non-interactive.
    '''
    if config_arg:
        if not os.path.isfile(config_arg):
            raise SystemExit(f'Config not found: {config_arg}')
        return os.path.abspath(config_arg)
    existing = os.path.join(graph_dir, CONFIG_NAME)
    if os.path.isfile(existing):
        return existing
    template_path = find_template(graph_dir)
    if template_path is None:
        return None
    template = load_template(template_path)
    questions = template.get(X_QUESTIONS, [])
    if not interactive:
        wanted = '\n'.join(f'  - {q["key"]}: {q["prompt"]}' for q in questions)
        raise SystemExit(f'No {CONFIG_NAME} next to the graph and this session is '
                         f'non-interactive. Create one (copy {TEMPLATE_NAME}) or pass '
                         f'--config. Inputs needed:\n{wanted}')
    print(f'No {CONFIG_NAME} found — generating one from {TEMPLATE_NAME}.')
    answers = ask_questions(questions, graph_dir, input_fn = input_fn)
    with open(existing, 'w') as f:
        f.write(render_config(template, answers))
    print(f'Wrote {existing}')
    return existing

def resolve_mounts(template, config, graph_dir) -> List[str]:
    '''
    Expands the template's ``x-mounts`` against the resolved config into
    single-path mount specs (``/abs/path[:ro]``) for ``manifests.parse_mounts``.
    Relative config values resolve against ``graph_dir`` (matching a prep/compile
    container whose workdir is the graph dir); ``~`` expands.
    '''
    specs = []
    for entry in template.get(X_MOUNTS, []) if template else []:
        entry = str(entry)
        read_only = entry.endswith(':ro')
        if read_only:
            entry = entry[:-3]
        if entry.startswith('{') and entry.endswith('}'):
            values = _get_dotted(config, entry[1:-1].split('.'))
        else:
            values = [entry]
        for value in values:
            value = str(value)
            if ':' in value:
                host, container = value.split(':', 1)
                host = os.path.abspath(os.path.join(graph_dir, os.path.expanduser(host)))
                spec = f'{host}:{container}'
            else:
                spec = os.path.abspath(os.path.join(graph_dir, os.path.expanduser(value)))
            specs.append(f'{spec}:ro' if read_only else spec)
    # De-duplicate, preserving order (e.g. several cameras in one directory).
    seen = set()
    unique = []
    for spec in specs:
        if spec not in seen:
            seen.add(spec)
            unique.append(spec)
    return unique
