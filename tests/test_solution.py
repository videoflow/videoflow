'''
Solution conventions: config generation from config.template.yaml (question
typing incl. the paths fan-out, x-* stripping, non-interactive failure) and
x-mounts expansion against the resolved config.

Pure/unit: input() is stubbed — no terminal, no docker.
'''
import os

import pytest
import yaml

from videoflow.deploy import solution

TEMPLATE = '''
work_dir: ./out
flow_type: batch
cameras: {}
pitch:
  length: 100.0
  width: 64.0
team_names: {0: Reds, 1: Blues}
x-questions:
  - {key: cameras, prompt: 'Video files', type: paths,
     item_key: 'cam{i}', item_value: {video: '{path}'}}
  - {key: pitch.length, prompt: 'Pitch length', type: float, default: 105.0}
  - {key: flow_type, prompt: 'Flow type', type: choice, choices: [batch, realtime], default: batch}
  - {key: team_names.0, prompt: 'Team 0', type: str, default: Reds}
x-mounts:
  - '{cameras.*.video}:ro'
  - '{work_dir}'
  - '~/.videoflow:/root/.videoflow'
'''


def _solution_dir(tmp_path, videos = 2):
    (tmp_path / 'config.template.yaml').write_text(TEMPLATE)
    for i in range(videos):
        (tmp_path / f'cam{i}.mp4').write_bytes(b'x')
    return str(tmp_path)


def test_ensure_config_generates_from_answers(tmp_path):
    graph_dir = _solution_dir(tmp_path)
    answers = iter([f'{tmp_path}/cam0.mp4, {tmp_path}/cam1.mp4', '', 'realtime', 'Sharks'])
    path = solution.ensure_config(graph_dir, interactive = True,
                                  input_fn = lambda prompt: next(answers))
    assert path == os.path.join(graph_dir, 'config.yaml')
    config = yaml.safe_load(open(path))
    assert config['cameras'] == {'cam0': {'video': f'{tmp_path}/cam0.mp4'},
                                 'cam1': {'video': f'{tmp_path}/cam1.mp4'}}
    assert config['pitch']['length'] == 105.0        # empty answer → default
    assert config['pitch']['width'] == 64.0          # untouched template value
    assert config['flow_type'] == 'realtime'
    assert config['team_names'] == {0: 'Sharks', 1: 'Blues'}
    # The extension blocks never leak into the generated config.
    assert 'x-questions' not in config and 'x-mounts' not in config


def test_ensure_config_prefers_existing_and_explicit(tmp_path):
    graph_dir = _solution_dir(tmp_path)
    existing = tmp_path / 'config.yaml'
    existing.write_text('flow_type: batch\n')
    assert solution.ensure_config(graph_dir) == str(existing)
    explicit = tmp_path / 'other.yaml'
    explicit.write_text('flow_type: batch\n')
    assert solution.ensure_config(graph_dir, config_arg = str(explicit)) == str(explicit)
    with pytest.raises(SystemExit, match = 'not found'):
        solution.ensure_config(graph_dir, config_arg = str(tmp_path / 'missing.yaml'))


def test_ensure_config_non_interactive_lists_questions(tmp_path):
    graph_dir = _solution_dir(tmp_path)
    with pytest.raises(SystemExit) as exc:
        solution.ensure_config(graph_dir, interactive = False)
    message = str(exc.value)
    assert 'pitch.length' in message and 'Video files' in message and '--config' in message


def test_ensure_config_none_without_template(tmp_path):
    assert solution.ensure_config(str(tmp_path)) is None


def test_paths_question_validates_existence(tmp_path):
    graph_dir = _solution_dir(tmp_path)
    answers = iter(['/nope/missing.mp4', f'{tmp_path}/cam0.mp4'])
    path = solution.ensure_config(
        graph_dir, interactive = True,
        input_fn = lambda prompt: next(answers) if 'Video' in prompt else '')
    config = yaml.safe_load(open(path))
    # The bad path was re-asked; only the good one landed.
    assert list(config['cameras']) == ['cam0']


SCALAR_TEMPLATE = '''
input_video: null
fps: 30
x-questions:
  - {key: input_video, prompt: 'Video', type: path}
  - {key: fps, prompt: 'FPS', type: int, default: 30}
x-mounts:
  - '{input_video}:ro'
  - '~/.videoflow:/root/.videoflow'
'''


def test_path_and_int_question_types(tmp_path):
    (tmp_path / 'config.template.yaml').write_text(SCALAR_TEMPLATE)
    (tmp_path / 'clip.mp4').write_bytes(b'x')
    answers = iter(['clip.mp4', '25'])
    path = solution.ensure_config(str(tmp_path), interactive = True,
                                  input_fn = lambda prompt: next(answers))
    config = yaml.safe_load(open(path))
    # `path` yields a validated absolute string (not the mapping `paths` builds).
    assert config['input_video'] == str(tmp_path / 'clip.mp4')
    assert config['fps'] == 25 and isinstance(config['fps'], int)


def test_path_question_rejects_missing_file(tmp_path):
    (tmp_path / 'config.template.yaml').write_text(SCALAR_TEMPLATE)
    (tmp_path / 'clip.mp4').write_bytes(b'x')
    answers = iter(['/nope/missing.mp4', 'clip.mp4', ''])
    path = solution.ensure_config(str(tmp_path), interactive = True,
                                  input_fn = lambda prompt: next(answers))
    assert yaml.safe_load(open(path))['input_video'] == str(tmp_path / 'clip.mp4')


def test_resolve_mounts_scalar_path_and_host_container_pair(tmp_path):
    template = yaml.safe_load(SCALAR_TEMPLATE)
    config = {'input_video': '/data/clip.mp4'}
    mounts = solution.resolve_mounts(template, config, str(tmp_path))
    assert mounts == ['/data/clip.mp4:ro',
                      os.path.expanduser('~/.videoflow') + ':/root/.videoflow']


def test_resolve_mounts_fans_out_and_dedupes(tmp_path):
    template = yaml.safe_load(TEMPLATE)
    config = {
        'work_dir': './out',
        'cameras': {'cam0': {'video': '/data/a.mp4'}, 'cam1': {'video': '/data/b.mp4'}},
    }
    mounts = solution.resolve_mounts(template, config, str(tmp_path))
    assert mounts == ['/data/a.mp4:ro', '/data/b.mp4:ro',
                      str(tmp_path / 'out'),
                      os.path.expanduser('~/.videoflow') + ':/root/.videoflow']


# -- question-type registry ------------------------------------------------

def test_builtin_question_types_are_registered():
    assert set(solution.registered_question_types()) == {
        'str', 'int', 'float', 'choice', 'path', 'paths'}


def test_register_question_type_extends_the_prompt(monkeypatch):
    '''A solution needing a type videoflow does not ship registers one.'''
    monkeypatch.setattr(solution, '_QUESTION_COERCERS', dict(solution._QUESTION_COERCERS))
    solution.register_question_type(
        'bool', lambda question, answer, base_dir: str(answer).lower() in ('1', 'true', 'yes'))

    questions = [{'key': 'debug', 'prompt': 'Debug?', 'type': 'bool'}]
    answers = solution.ask_questions(questions, '.', input_fn = lambda _p: 'yes')
    assert answers == {'debug': True}


def test_coercer_receives_the_question_so_it_can_read_its_own_keys(monkeypatch):
    monkeypatch.setattr(solution, '_QUESTION_COERCERS', dict(solution._QUESTION_COERCERS))
    solution.register_question_type(
        'suffixed', lambda question, answer, base_dir: f'{answer}{question["suffix"]}')

    questions = [{'key': 'k', 'prompt': 'p', 'type': 'suffixed', 'suffix': '!'}]
    answers = solution.ask_questions(questions, '.', input_fn = lambda _p: 'hi')
    assert answers == {'k': 'hi!'}


def test_unknown_type_fails_before_prompting_not_in_a_loop():
    '''
    The prompt loop retries on ValueError, so an unknown type must be caught up
    front -- otherwise a template typo re-prompts forever over something the
    operator cannot fix by typing.
    '''
    questions = [{'key': 'k', 'prompt': 'p', 'type': 'flaot'}]

    def _never_called(_prompt):
        raise AssertionError('must not prompt for an unregistered type')

    with pytest.raises(ValueError) as excinfo:
        solution.ask_questions(questions, '.', input_fn = _never_called)
    msg = str(excinfo.value)
    assert 'flaot' in msg and 'register_question_type' in msg


def test_coercer_valueerror_still_reprompts(monkeypatch, capsys):
    '''A bad *answer* (as opposed to a bad type) must keep re-asking.'''
    answers_iter = iter(['nope', '3'])
    questions = [{'key': 'n', 'prompt': 'A number', 'type': 'int'}]
    result = solution.ask_questions(questions, '.', input_fn = lambda _p: next(answers_iter))
    assert result == {'n': 3}
    assert 'Invalid value' in capsys.readouterr().out
