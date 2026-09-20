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
e.g. ``'cam{i}'``, and ``item_value``, e.g. ``{video: '{path}'}``). That set is
extensible: see ``register_question_type``.

``x-mounts`` entries are path templates: ``'{cameras.*.video}:ro'`` (dotted
lookup into the resolved config, ``*`` fans out), ``'{work_dir}'``,
``'~/.videoflow:/root/.videoflow'``. A single path resolves to a same-path
hostPath mount (host and container see the same absolute path — required
because the paths baked into node params at compile time must resolve
identically in the pods); a ``host:container`` pair maps them explicitly
(e.g. the operator's home caches onto the container root's). A
``pvc:<claim>:<path>[:ro]`` entry mounts an existing PersistentVolumeClaim at
``<path>`` in the pods instead — the form for data that lives in the cluster
rather than on the operator's machine (a shared model cache, an RWX work
directory on a multi-node cluster); ``<path>`` takes the same ``{dotted}``
lookup and resolves relative to the solution directory like the others.
Host-side paths are resolved to their real location (symlinks followed), and
``~`` expands to the operator's home unless ``--mount-home`` re-roots it — the
way to put the caches a template maps onto ``/root/...`` inside a directory
every cluster node sees.

``x-gpu`` names the config values that decide whether the solution's GPU image
is the one to build: a list of the same ``{dotted}`` lookups (``'{device}'``,
``'{device.*}'``) or literals; the flow needs ``gpu.Dockerfile`` when any of
them resolves to ``gpu``. Without it, deploy falls back to whichever Dockerfile
exists, then to the compiled graph's device placement when the graph imports on
this machine, and finally to the CPU image (with a note).

The config file a deploy or local run resolved is exported to the compile step
as ``VF_SOLUTION_CONFIG`` (``CONFIG_ENV``), so a ``build_flow(cfg=None)`` that
reads it before its sibling ``config.yaml`` honours ``--config`` too.
'''
from __future__ import absolute_import, division, print_function

import os
import subprocess
import sys
from typing import Any, Callable, List, Optional

import yaml

X_QUESTIONS = 'x-questions'
X_MOUNTS = 'x-mounts'
X_GPU = 'x-gpu'
#: The template blocks stripped from a generated ``config.yaml``.
X_BLOCKS = (X_QUESTIONS, X_MOUNTS, X_GPU)
#: Marks an ``x-mounts`` entry (and a resolved mount spec) as a claim mount.
PVC_MOUNT_PREFIX = 'pvc:'
TEMPLATE_NAME = 'config.template.yaml'
CONFIG_NAME = 'config.yaml'
PREPARE_NAME = 'prepare.py'
#: Environment variable carrying the resolved config path to ``build_flow`` (see the module docstring).
CONFIG_ENV = 'VF_SOLUTION_CONFIG'

def find_template(graph_dir : str) -> Optional[str]:
    path = os.path.join(graph_dir, TEMPLATE_NAME)
    return path if os.path.isfile(path) else None

def load_template(template_path : str) -> dict:
    with open(template_path) as f:
        return yaml.safe_load(f)

def _seg(segment : str) -> int | str:
    return int(segment) if segment.isdigit() else segment

def _set_dotted(config : dict, dotted : str, value : Any) -> None:
    parts = [_seg(p) for p in dotted.split('.')]
    target = config
    for part in parts[:-1]:
        target = target.setdefault(part, {})
    target[parts[-1]] = value

def _get_dotted(config : dict, parts : List[str]) -> list:
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

def _abs_existing(path : object, base_dir : str) -> str:
    resolved = os.path.abspath(os.path.join(base_dir, os.path.expanduser(str(path))))
    if not os.path.exists(resolved):
        raise ValueError(f'no such file: {resolved}')
    return resolved

def expand_home(path : str, home : Optional[str] = None) -> str:
    '''
    ``~`` expansion for the host side of a mount: ``home`` (``--mount-home``)
    replaces the operator's home directory when given, so a template's
    ``'~/.videoflow:/root/.videoflow'`` can land inside a directory every cluster
    node sees. Other forms (``~user``, a path without ``~``) expand as usual.
    '''
    if home and (path == '~' or path.startswith('~/')):
        return home.rstrip('/') + path[1:]
    return os.path.expanduser(path)

# -- x-questions type coercion ---------------------------------------------
#
# One coercer per ``x-questions`` ``type``. A solution that needs a type videoflow
# does not ship (a bool, a secret read from the environment, a validated URL) can
# register one instead of patching this module.

_QUESTION_COERCERS : dict = {}

def register_question_type(qtype : str, coercer : Callable[[dict, str, str], Any]) -> None:
    '''
    Registers a coercer for an ``x-questions`` ``type``, called as
    ``coercer(question, answer, base_dir)`` where ``question`` is the raw
    x-questions entry (so a coercer can read its own extra keys, as ``choice``
    reads ``choices``), ``answer`` is the operator's raw string, and ``base_dir``
    is the solution directory that relative paths resolve against.

    A coercer signals a bad answer by raising ``ValueError``; the prompt loop
    catches it, shows the message, and re-asks.

    - Arguments:
        - qtype: the ``type`` string used in ``config.template.yaml``.
        - coercer: callable returning the coerced config value.
    '''
    _QUESTION_COERCERS[qtype] = coercer

def registered_question_types() -> list:
    '''The ``x-questions`` type names currently registered, sorted.'''
    return sorted(_QUESTION_COERCERS)

def _coerce_choice(question : dict, answer : str, base_dir : str) -> str:
    choices = [str(c) for c in question['choices']]
    if str(answer) not in choices:
        raise ValueError(f'must be one of: {", ".join(choices)}')
    return answer

def _coerce_path(question : dict, answer : str, base_dir : str) -> str:
    '''A single filesystem path, validated and absolutized (scalar config key).'''
    return _abs_existing(answer, base_dir)

def _coerce_paths(question : dict, answer : str, base_dir : str) -> dict:
    '''Several paths, expanded into a mapping via ``item_key``/``item_value``.'''
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

register_question_type('str', lambda question, answer, base_dir: answer)
register_question_type('int', lambda question, answer, base_dir: int(answer))
register_question_type('float', lambda question, answer, base_dir: float(answer))
register_question_type('choice', _coerce_choice)
register_question_type('path', _coerce_path)
register_question_type('paths', _coerce_paths)

def _coerce(question : dict, answer : str, base_dir : str) -> Any:
    '''
    Coerces one raw answer per its question ``type``.

    - Raises:
        - ValueError: the answer is invalid for its type, or the type is not \
            registered. Either way the prompt loop re-asks with the message.
    '''
    qtype = question.get('type', 'str')
    coercer = _QUESTION_COERCERS.get(qtype)
    if coercer is None:
        raise ValueError(
            f'unknown x-questions type {qtype!r}. Known types: '
            f'{", ".join(registered_question_types())}. Register another with '
            f'videoflow.deploy.solution.register_question_type.')
    return coercer(question, answer, base_dir)

def _fill_paths(template_value : Any, path : str) -> Any:
    if isinstance(template_value, dict):
        return {k: _fill_paths(v, path) for k, v in template_value.items()}
    if isinstance(template_value, str):
        return template_value.format(path = path)
    return template_value

def validate_question_types(questions : Optional[list]) -> None:
    '''
    Checks every question's ``type`` is registered, before any prompting starts.

    Done up front deliberately: the prompt loop treats ``ValueError`` as a bad
    *answer* and re-asks, so an unregistered type discovered mid-loop would
    re-prompt forever over something the operator cannot fix by typing.

    - Raises:
        - ValueError: a question names an unregistered type; the message names \
            the offending key, the known types, and how to add one.
    '''
    for question in questions or []:
        qtype = question.get('type', 'str')
        if qtype not in _QUESTION_COERCERS:
            raise ValueError(
                f'x-questions entry {question.get("key", "<no key>")!r} has unknown '
                f'type {qtype!r}. Known types: {", ".join(registered_question_types())}. '
                f'Register another with videoflow.deploy.solution.register_question_type.')

def ask_questions(questions : List[dict], base_dir : str,
                  input_fn : Callable[[str], str] = input) -> dict:
    '''
    Prompts for each x-question on the terminal; returns {dotted_key: typed_value}.

    - Raises:
        - ValueError: a question names an unregistered type (checked before \
            prompting — see ``validate_question_types``).
    '''
    validate_question_types(questions)
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

def render_config(template : dict, answers : dict) -> str:
    '''The final config YAML: template body with answers applied and x-* blocks stripped.'''
    config = {k: v for k, v in template.items() if k not in X_BLOCKS}
    for dotted, value in answers.items():
        _set_dotted(config, dotted, value)
    header = f'# Generated by videoflow deploy from {TEMPLATE_NAME} — see that file for docs.\n'
    return header + yaml.dump(config, default_flow_style = False, sort_keys = False)

def ensure_config(graph_dir : str, config_arg : Optional[str] = None, interactive : bool = True,
                  input_fn : Callable[[str], str] = input) -> Optional[str]:
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

def _lookup(entry : str, config : dict) -> list:
    '''A ``{dotted}`` template entry's config values (``*`` fans out); anything else is a literal.'''
    if entry.startswith('{') and entry.endswith('}'):
        return _get_dotted(config, entry[1:-1].split('.'))
    return [entry]

def resolve_gpu(template : Optional[dict], config : dict) -> Optional[bool]:
    '''
    What the template's ``x-gpu`` block says about the image to build: ``True``
    when any named config value (or literal) is ``gpu``, ``False`` when the block
    is present and none is, ``None`` when the template declares no ``x-gpu`` at
    all — the caller then falls back to the Dockerfiles that exist and the graph.

    - Raises:
        - ValueError: the block is not a list.
    '''
    if not template or X_GPU not in template:
        return None
    block = template.get(X_GPU)
    if not isinstance(block, list):
        raise ValueError(f'{X_GPU} must be a list of config lookups such as {{device}}, got: {block!r}')
    return any(str(value).strip().lower() == 'gpu'
               for entry in block for value in _lookup(str(entry), config))

def _host_path(value : str, graph_dir : str, home : Optional[str]) -> tuple[str, str]:
    '''
    ``(real, abs)`` for the host side of a mount: the absolute path as written
    (graph-dir relative, ``~`` expanded) and its real location with symlinks
    followed — what a hostPath on a cluster node and a ``--mount-pvc`` backing
    directory are compared against.
    '''
    abs_path = os.path.abspath(os.path.join(graph_dir, expand_home(value, home)))
    return os.path.realpath(abs_path), abs_path

def resolve_mounts(template : Optional[dict], config : dict, graph_dir : str,
                   home : Optional[str] = None) -> List[str]:
    '''
    Expands the template's ``x-mounts`` against the resolved config into mount
    specs: ``/abs/path[:ro]`` and ``/host:/container[:ro]`` for
    ``manifests.parse_mounts``, and ``pvc:<claim>:/abs/path[:ro]`` for
    ``manifests.parse_pvc_mounts`` (``split_mount_specs`` separates the two).
    Relative config values resolve against ``graph_dir`` (matching a prep/compile
    container whose workdir is the graph dir); ``~`` expands to ``home`` when
    given (``--mount-home``), else to the operator's home. Host sides are real
    paths (symlinks followed); a same-path entry whose real location differs from
    the path as written is emitted as ``real:written`` so the container still sees
    the path baked into the node params.

    - Raises:
        - ValueError: a ``pvc:`` entry names no claim or no path.
    '''
    specs = []
    for entry in template.get(X_MOUNTS, []) if template else []:
        raw = entry = str(entry)
        read_only = entry.endswith(':ro')
        if read_only:
            entry = entry[:-3]
        claim = None
        if entry.startswith(PVC_MOUNT_PREFIX):
            claim, _, entry = entry[len(PVC_MOUNT_PREFIX):].partition(':')
            if not claim or not entry:
                raise ValueError(f'x-mounts entry {raw!r} must be pvc:<claim>:<path>[:ro] — '
                                 f'the claim name and the mount path are both required.')
        for value in _lookup(entry, config):
            if value is None or str(value).strip() == '':
                continue                    # an unset input (a sample the prep hook provides): nothing to mount
            value = str(value)
            if claim is not None:
                path = os.path.abspath(os.path.join(graph_dir, os.path.expanduser(value)))
                spec = f'{PVC_MOUNT_PREFIX}{claim}:{path}'
            elif ':' in value:
                host, container = value.split(':', 1)
                real, _written = _host_path(host, graph_dir, home)
                spec = f'{real}:{container}'
            else:
                real, written = _host_path(value, graph_dir, home)
                spec = written if real == written else f'{real}:{written}'
            specs.append(f'{spec}:ro' if read_only else spec)
    # De-duplicate, preserving order (e.g. several cameras in one directory).
    seen = set()
    unique = []
    for spec in specs:
        if spec not in seen:
            seen.add(spec)
            unique.append(spec)
    return unique

def split_mount_specs(specs : List[str]) -> tuple[List[str], List[str]]:
    '''
    Partitions ``resolve_mounts`` output into ``(host_specs, claim_specs)``: the
    host-path specs as ``manifests.parse_mounts`` takes them, and the claim specs
    with their ``pvc:`` prefix stripped, as ``manifests.parse_pvc_mounts`` takes
    them. Order is preserved within each list.
    '''
    host_specs : List[str] = []
    claim_specs : List[str] = []
    for spec in specs:
        if spec.startswith(PVC_MOUNT_PREFIX):
            claim_specs.append(spec[len(PVC_MOUNT_PREFIX):])
        else:
            host_specs.append(spec)
    return host_specs, claim_specs

def find_prepare(graph_dir : str) -> Optional[str]:
    '''The solution's ``prepare.py`` hook, or None when it ships none.'''
    path = os.path.join(graph_dir, PREPARE_NAME)
    return path if os.path.isfile(path) else None

def prepare_command(config_path : Optional[str] = None, python_exe : str = 'python') -> List[str]:
    '''The argv for the prepare hook, run with the solution directory as cwd.'''
    return [python_exe, PREPARE_NAME] + (['--config', config_path] if config_path else [])

def run_prepare_local(graph_dir : str, config_path : Optional[str] = None) -> bool:
    '''
    Runs the solution's ``prepare.py`` on this host, with the solution directory as
    the working directory so its ``import common`` resolves. Used by ``run-local``
    (whose workers are local anyway) and as ``deploy``'s fallback when there is no
    image to run it in.

    - Returns:
        - False when the solution ships no hook (nothing was run).

    - Raises:
        - ``subprocess.CalledProcessError`` when the hook exits non-zero.
    '''
    if find_prepare(graph_dir) is None:
        return False
    # The hook's output goes to stderr, not stdout: deploy's stdout is machine-readable
    # (--dry-run streams the rendered manifests there), so prepare progress must not
    # interleave with it.
    subprocess.run(prepare_command(config_path, sys.executable), cwd = graph_dir,
                   stdout = sys.stderr, check = True)
    return True
