'''
The error taxonomy every other module classifies against.

An exception hierarchy earns its keep only if something *branches* on it, so each
level here drives one specific decision:

===================  ======================================================
Level                Decides
===================  ======================================================
Top-level class      who is at fault, and therefore who sees it and where
``disposition``      what the messenger does with the in-flight message
``code``             what the metric is labelled with, and what the DLQ is
                     queryable by
``remedy``           what the CLI prints under the error
===================  ======================================================

The three branches of the tree map onto the three boundaries of the framework:

- ``VideoflowUserError`` — the graph or its configuration is invalid. Raised while
  the graph is being *built*, on the machine that builds it; the CLI turns it into
  exit code 2.
- ``VideoflowEnvironmentError`` — the world is not as required (broker down,
  cluster unreachable, no GPU capacity). Exit code 3.
- ``VideoflowRuntimeError`` — something failed while messages were flowing. Its
  three leaves are the **dispositions**, and they are the only part of this module
  the hot path reads.

The disposition is what fixes the framework's oldest blind spot: with a single
``except Exception`` there is no way to tell a bad *message* (retrying is waste)
from a temporarily unavailable *world* (retrying is exactly right) from a sick
*worker* (retrying is actively harmful — it shreds a healthy stream into the DLQ
one message at a time). See ``videoflow.core.policies.DeliveryPolicy`` for what
each disposition actually causes.

``remedy`` deserves a note: naming the fix rather than only the problem is the
strongest convention in this codebase's error strings. Making it a field rather
than prose means the CLI, the DLQ inspector and the Kubernetes termination log can
all render it the same way, and that it cannot be quietly forgotten.
'''
from __future__ import absolute_import, division, print_function

import json
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional, Type

#: What the messenger does with the input group a node just failed on. These are
#: the values ``VideoflowRuntimeError.disposition`` takes and the keys
#: ``DeliveryPolicy`` dispatches on.
POISON = 'poison'              # the DATA is bad     → dead-letter now, never retry
TRANSIENT = 'transient'        # the WORLD blipped   → retry with backoff
WORKER_FATAL = 'worker_fatal'  # THIS WORKER is sick → hand off, take the worker out
DISPOSITIONS = (POISON, TRANSIENT, WORKER_FATAL)

#: Disposition assumed for an exception nothing has classified. TRANSIENT
#: reproduces the framework's historical behaviour (retry, then dead-letter), so
#: adopting the taxonomy changes nothing until a node or a classifier opts in.
DEFAULT_DISPOSITION = TRANSIENT

#: Process exit statuses, by fault class. A uniform exit 1 is untriageable in CI;
#: these let a wrapper script tell "your flow is wrong" from "the cluster is
#: wrong" from "the flow ran and lost nodes" without parsing stderr.
EXIT_USER = 2
EXIT_ENVIRONMENT = 3
EXIT_FLOW_FAILED = 4
EXIT_FLOW_STALLED = 5
EXIT_INTERRUPTED = 130

class VideoflowError(Exception):
    '''
    Base of every error videoflow raises on purpose.

    - Arguments:
        - message: what went wrong.
        - remedy: what the reader should *do* about it. Kept out of ``message`` \
            so every renderer can present it consistently.
        - context: structured key/values (node, trace_id, replica, path). Never \
            interpolated into ``message`` either — they are emitted as log fields \
            and proto fields so they stay queryable.

    - Class attributes:
        - code: stable, greppable identifier (``VF_GRAPH_CYCLE``). The message may \
            be reworded freely; the code may not, because metrics and DLQ queries \
            key on it.
        - exit_code: the process exit status when this reaches the CLI.
    '''
    code : str = 'VF_UNKNOWN'
    exit_code : int = 1

    def __init__(self, message : str, remedy : Optional[str] = None,
                **context : Any) -> None:
        super(VideoflowError, self).__init__(message)
        self.message = message
        self.remedy = remedy
        self.context : Dict[str, Any] = context

    def __str__(self) -> str:
        # The remedy rides along in str() so the many places that render an
        # exception with %s (logs, subprocess wrappers, third-party handlers)
        # keep showing the fix, not just the symptom.
        return f'{self.message} {self.remedy}' if self.remedy else self.message

    def to_dict(self) -> Dict[str, Any]:
        '''
        JSON-safe form, used by the Kubernetes termination log and by tests. \
            Kept explicit (not ``asdict``) because this crosses a process \
            boundary and its shape is a contract.
        '''
        d : Dict[str, Any] = {'code': self.code, 'message': self.message}
        if self.remedy:
            d['remedy'] = self.remedy
        disposition = getattr(self, 'disposition', None)
        if disposition:
            d['disposition'] = disposition
        if self.context:
            # Context values come from node code, so they are not guaranteed
            # serializable; repr() the ones that are not rather than lose the field.
            d['context'] = {k: _jsonable(v) for k, v in self.context.items()}
        return d

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), sort_keys = True)

def error_to_dict(error : Any) -> Dict[str, Any]:
    '''
    Normalizes anything that describes a failure into the JSON-safe record the
    ABORT marker and the termination log carry: one of ours keeps its structure,
    a bare exception gets a code derived from its type, and an already-normalized
    dict passes through (which is how an ABORT is relayed hop to hop without the
    origin's detail being rewritten at every step).
    '''
    if isinstance(error, dict):
        return error
    if isinstance(error, VideoflowError):
        return error.to_dict()
    return {'code': f'VF_{type(error).__name__.upper()}', 'message': str(error)}

def _jsonable(value : Any) -> Any:
    try:
        json.dumps(value)
    except (TypeError, ValueError):
        return repr(value)
    return value

# -- build time: the graph or its configuration is invalid --------------------

class VideoflowUserError(VideoflowError):
    '''Something the author of the flow can fix by editing their code or config.'''
    code = 'VF_USER'
    exit_code = EXIT_USER

class GraphError(VideoflowUserError):
    '''
    The graph itself is invalid: a cycle, an unreachable consumer, duplicate node
    names, a replicated join without a partition key.

    - Arguments:
        - diagnostics: every problem found in one validation pass (see \
            ``videoflow.core.graph.validate``). Reporting them together is the \
            difference between one fix-rerun cycle and four.
    '''
    code = 'VF_GRAPH'

    def __init__(self, message : str, remedy : Optional[str] = None,
                diagnostics : Optional[List["Diagnostic"]] = None,
                **context : Any) -> None:
        super(GraphError, self).__init__(message, remedy, **context)
        self.diagnostics : List[Diagnostic] = list(diagnostics or [])

class NodeContractError(VideoflowUserError):
    '''A node violates the contract a worker relies on to rebuild and run it.'''
    code = 'VF_NODE_CONTRACT'

class ConfigError(VideoflowUserError):
    '''An invalid configuration value: flow type, join policy, mount spec, image ref.'''
    code = 'VF_CONFIG'

class CapabilityError(VideoflowUserError):
    '''The flow asks a component for something it declares it cannot do.'''
    code = 'VF_CAPABILITY'

# -- deploy time: the world is not as required --------------------------------

class VideoflowEnvironmentError(VideoflowError):
    '''The code is fine; the machine, cluster, broker or registry is not.'''
    code = 'VF_ENVIRONMENT'
    exit_code = EXIT_ENVIRONMENT

class BrokerUnavailable(VideoflowEnvironmentError):
    '''NATS could not be reached, or a stream/consumer could not be created.'''
    code = 'VF_BROKER_UNAVAILABLE'

class ClusterError(VideoflowEnvironmentError):
    '''kubectl is missing, an apply was rejected, or the cluster refused the work.'''
    code = 'VF_CLUSTER'

class ResourceUnavailable(VideoflowEnvironmentError):
    '''A resource the flow needs does not exist or cannot be obtained.'''
    code = 'VF_RESOURCE_UNAVAILABLE'

class FlowFailed(VideoflowEnvironmentError):
    '''The flow ran and one or more nodes failed. Distinct exit code so CI can tell it from a bad deploy.'''
    code = 'VF_FLOW_FAILED'
    exit_code = EXIT_FLOW_FAILED

class FlowStalled(VideoflowEnvironmentError):
    '''The flow can never finish: unschedulable pods, or a node that stopped making progress.'''
    code = 'VF_FLOW_STALLED'
    exit_code = EXIT_FLOW_STALLED

# -- run time: a message was in flight ----------------------------------------

class VideoflowRuntimeError(VideoflowError):
    '''
    A failure that happened while messages were flowing. The ``disposition`` is
    the only thing the hot path reads — see ``DeliveryPolicy.action_for``.
    '''
    code = 'VF_RUNTIME'
    exit_code = EXIT_FLOW_FAILED
    disposition : str = DEFAULT_DISPOSITION

class PoisonMessage(VideoflowRuntimeError):
    '''
    The message itself is bad. Retrying is waste: it will fail identically every
    time, so it is dead-lettered on the first failure.
    '''
    code = 'VF_POISON'
    disposition = POISON

class DecodeError(PoisonMessage):
    '''The envelope or payload could not be decoded off the wire.'''
    code = 'VF_POISON_DECODE'

class SchemaError(PoisonMessage):
    '''The payload decoded but is not what this node requires.'''
    code = 'VF_POISON_SCHEMA'

class TransientFailure(VideoflowRuntimeError):
    '''Something outside the worker was briefly unavailable. Retry with backoff.'''
    code = 'VF_TRANSIENT'
    disposition = TRANSIENT

class UpstreamUnavailable(TransientFailure):
    '''A service the node depends on (database, API, model server) is down or throttling.'''
    code = 'VF_UPSTREAM_UNAVAILABLE'

class WorkerFatal(VideoflowRuntimeError):
    '''
    This worker cannot process *any* message.

    Two things follow, and both matter. The in-flight message is handed back to
    the broker for a healthy replica rather than blamed — it is never
    dead-lettered, because the data is fine. And the worker then **stops**:
    raising this is the node asserting that nothing it is given will succeed, so
    continuing would only nak the rest of the stream one message at a time on
    the way to the same conclusion.
    '''
    code = 'VF_WORKER_FATAL'
    disposition = WORKER_FATAL

class DeviceError(WorkerFatal):
    '''The accelerator is unusable: out of memory, fell off the bus, wrong driver.'''
    code = 'VF_DEVICE'

class ResourceExhausted(WorkerFatal):
    '''The worker is out of a host resource — memory, disk, file descriptors.'''
    code = 'VF_RESOURCE_EXHAUSTED'

class WorkerUnhealthy(WorkerFatal):
    '''
    Raised by the task loop when the circuit breaker trips: this worker has failed
    ``threshold`` messages in a row, which is the signature of a sick worker rather
    than of bad data. It stops the run loop so the un-acked inputs go back to a
    healthy replica.
    '''
    code = 'VF_WORKER_UNHEALTHY'

class ProgressStalled(VideoflowRuntimeError):
    '''
    Raised when a node has acked nothing for the progress deadline while work was
    pending. Distinct from ``WorkerUnhealthy``: nothing raised, the node simply
    stopped making progress.
    '''
    code = 'VF_PROGRESS_STALLED'
    disposition = WORKER_FATAL
    exit_code = EXIT_FLOW_STALLED

class UpstreamAborted(VideoflowRuntimeError):
    '''
    An upstream node terminated abnormally and published an ABORT marker. This
    node stops too, carrying the originating error, instead of waiting forever for
    an end-of-stream that is never coming.
    '''
    code = 'VF_UPSTREAM_ABORTED'
    disposition = WORKER_FATAL
    exit_code = EXIT_FLOW_FAILED

# -- classification -----------------------------------------------------------

#: Third-party exception types mapped onto dispositions. A list, not a dict, so
#: lookup honours subclass relationships and later registrations win — a component
#: may deliberately override a built-in mapping.
_CLASSIFIERS : List[tuple] = []

def _seed_builtin_classifiers() -> None:
    '''
    The stdlib types whose disposition is unambiguous. Deliberately conservative:
    anything arguable is left to the default so the framework never silently
    decides that a user's exception means something it does not.
    '''
    register_error_classifier(UnicodeDecodeError, POISON)
    register_error_classifier(json.JSONDecodeError, POISON)
    register_error_classifier(ConnectionError, TRANSIENT)
    register_error_classifier(TimeoutError, TRANSIENT)
    register_error_classifier(MemoryError, WORKER_FATAL)

def register_error_classifier(exc_type : Type[BaseException], disposition : str) -> None:
    '''
    Maps a third-party exception type onto a disposition, so the retry ladder can
    reason about failures from libraries videoflow does not own — a component
    cannot subclass ``torch.cuda.OutOfMemoryError``, but it can register it::

        register_error_classifier(torch.cuda.OutOfMemoryError, WORKER_FATAL)

    Register on import of the package that raises the type; every flow using that
    component then inherits the correct behaviour.

    - Arguments:
        - exc_type: the exception class to classify. Subclasses match too.
        - disposition: one of ``DISPOSITIONS``.

    - Raises:
        - ValueError: if ``disposition`` is not a known one (the message names them).
    '''
    if disposition not in DISPOSITIONS:
        raise ValueError(
            f'Unknown disposition {disposition!r}. Known dispositions: '
            f'{", ".join(DISPOSITIONS)}. Import them from videoflow.core.errors.')
    if not (isinstance(exc_type, type) and issubclass(exc_type, BaseException)):
        raise ValueError(f'register_error_classifier expects an exception class, '
                        f'got {exc_type!r}.')
    # Appended, and matched from the end, so a later registration wins over an
    # earlier one for the same (or a more general) type.
    _CLASSIFIERS.append((exc_type, disposition))

def registered_error_classifiers() -> List[tuple]:
    '''The ``(exception type, disposition)`` pairs currently registered, in registration order.'''
    return list(_CLASSIFIERS)

def classify(exc : BaseException, default : str = DEFAULT_DISPOSITION) -> str:
    '''
    The disposition of ``exc``: its own if it is one of ours, else the most
    recently registered classifier that matches, else ``default``.

    - Arguments:
        - exc: the exception a node raised.
        - default: what an unclassified exception means. Per-node overridable \
            via ``ProcessorNode(on_error = ...)``.
    '''
    if isinstance(exc, VideoflowRuntimeError):
        return exc.disposition
    for exc_type, disposition in reversed(_CLASSIFIERS):
        if isinstance(exc, exc_type):
            return disposition
    return default

def as_runtime_error(exc : BaseException, default : str = DEFAULT_DISPOSITION,
                    **context : Any) -> VideoflowRuntimeError:
    '''
    Wraps whatever a node raised into a classified ``VideoflowRuntimeError``
    carrying the message's identity, so everything downstream of the node-call
    boundary sees a typed error with a disposition, a code and a context.

    One of ours is enriched in place rather than re-wrapped: re-wrapping would
    bury the original code, which is the thing metrics and the DLQ key on.

    - Arguments:
        - exc: the original exception.
        - default: disposition for an exception nothing classifies.
        - context: node, trace_id, seq, replica — whatever identifies the message.
    '''
    if isinstance(exc, VideoflowRuntimeError):
        exc.context.update(context)
        return exc
    disposition = classify(exc, default)
    cls = _DISPOSITION_CLASSES[disposition]
    wrapped = cls(f'{type(exc).__name__}: {exc}', **context)
    wrapped.__cause__ = exc
    return wrapped

#: The error class a bare exception is wrapped in, per disposition.
_DISPOSITION_CLASSES : Dict[str, Type[VideoflowRuntimeError]] = {
    POISON: PoisonMessage,
    TRANSIENT: TransientFailure,
    WORKER_FATAL: WorkerFatal,
}

# -- diagnostics (build-time validation) --------------------------------------

SEVERITY_ERROR = 'error'
SEVERITY_WARNING = 'warning'

@dataclass(frozen = True)
class Diagnostic:
    '''
    One problem found by ``videoflow.core.graph.validate``. Validation collects
    these rather than raising on the first one, so a flow with three mistakes
    reports three — a compiler's contract, not an interpreter's.

    - Attributes:
        - severity: ``error`` (the flow cannot run) or ``warning`` (it can, but \
            something is probably not what the author meant).
        - code: the ``VideoflowError.code`` this would be raised as.
        - node: the node name the problem belongs to, or None if it is graph-wide.
        - message: what is wrong.
        - remedy: what to do about it.
    '''
    severity : str
    code : str
    node : Optional[str]
    message : str
    remedy : Optional[str] = None

    def render(self) -> str:
        '''One line, as the CLI prints it.'''
        where = f' [{self.node}]' if self.node else ''
        remedy = f' {self.remedy}' if self.remedy else ''
        return f'{self.severity}: {self.code}{where}: {self.message}{remedy}'

def raise_for_diagnostics(diagnostics : List[Diagnostic]) -> None:
    '''
    Raises a single ``GraphError`` listing every error-severity diagnostic, or
    returns quietly if there are none. Warnings never raise.

    - Raises:
        - GraphError: carrying all error diagnostics in ``.diagnostics``.
    '''
    errors = [d for d in diagnostics if d.severity == SEVERITY_ERROR]
    if not errors:
        return
    if len(errors) == 1:
        raise GraphError(errors[0].message, remedy = errors[0].remedy,
                        diagnostics = diagnostics)
    listing = '\n'.join(f'  - {d.render()}' for d in errors)
    raise GraphError(
        f'{len(errors)} problems in the flow graph:\n{listing}',
        remedy = 'Fix all of the above; they were found in a single validation pass.',
        diagnostics = diagnostics)

# -- classifier registration for optional/foreign types -----------------------

def register_classifier_for(name : str, disposition : str,
                        resolver : Optional[Callable[[], Optional[type]]] = None) -> bool:
    '''
    Registers a classifier for a type that may not be importable, e.g. a CUDA
    error class that only exists when torch is installed. Returns whether the type
    resolved and was registered, so callers can register opportunistically without
    guarding every import themselves.

    - Arguments:
        - name: human name of the type, for the log line when it does not resolve.
        - disposition: one of ``DISPOSITIONS``.
        - resolver: returns the class, or None when the package is absent.
    '''
    if resolver is None:
        return False
    try:
        resolved = resolver()
    except Exception:                       # pragma: no cover - a broken optional dep
        return False
    if resolved is None:
        return False
    register_error_classifier(resolved, disposition)
    return True

_seed_builtin_classifiers()
