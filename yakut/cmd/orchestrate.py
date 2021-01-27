# Copyright (c) 2021 UAVCAN Consortium
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@uavcan.org>

from __future__ import annotations
import os
import sys
import time
import errno
import signal
import dataclasses
from typing import Dict, List, Any, Tuple, Union, Optional, Callable
from functools import lru_cache
from pathlib import Path
import logging
import click
import yakut
from yakut.yaml import YAMLLoader, YAMLDumper


_logger = logging.getLogger(__name__)

_NAME_SEP = "."
_ITEM_SEP = " "

_NOT_ENV = "="
"""Equals sign is the only character that cannot occur in an environment variable name in most OS."""

_POLL_INTERVAL = 0.1


class SchemaError(ValueError):
    pass


class EnvironmentVariableError(SchemaError):
    pass


@yakut.subcommand()
@click.argument("yaml", type=click.Path(exists=True, dir_okay=False, resolve_path=True, allow_dash=True, path_type=str))
def orchestrate(yaml: str) -> None:
    """
    Execute an orchestration file.
    """
    interrupted = False

    def on_signal(s: int, _: Any) -> None:
        nonlocal interrupted
        interrupted = True
        _logger.info("Orchestrator received signal %s %r, stopping", s, signal.strsignal(s))

    for sig in [signal.SIGABRT, signal.SIGINT, signal.SIGTERM]:
        signal.signal(sig, on_signal)
    if sys.platform.startswith("win"):
        signal.signal(signal.SIGBREAK, on_signal)

    res = exec_file("", Path(yaml).resolve(), {}, predicate=lambda: not interrupted)

    exit(-1 if interrupted and (res == 0) else res)


FlagDelegate = Callable[[], bool]


def exec_file(loc: str, yaml_file: Path, env: Dict[str, str], *, predicate: FlagDelegate) -> int:
    _log_execution_point(loc, str(yaml_file.resolve()))
    comp = _parse(_load_yaml(yaml_file), env)
    _logger.debug("Parsed file %s with env keys %s", yaml_file, list(env))
    return exec_composition(f"{loc}/{str(yaml_file)!r}", comp, predicate=predicate)


def exec_composition(loc: str, comp: Composition, *, predicate: FlagDelegate) -> int:
    res = exec_script(f"{loc}/?", comp.predicate, comp.env, kill_timeout=comp.kill_timeout, predicate=predicate)
    if res != 0:  # If any of the commands of the predicate fails, we simply skip the rest and report success.
        return 0

    res = exec_script(f"{loc}/$", comp.start, comp.env, kill_timeout=comp.kill_timeout, predicate=predicate)
    if res != 0:
        return res

    return exec_script(f"{loc}/.", comp.stop, comp.env, kill_timeout=comp.kill_timeout, predicate=predicate)


def exec_script(
    loc: str, scr: List[Statement], env: Dict[str, str], *, kill_timeout: float, predicate: FlagDelegate
) -> int:
    from concurrent.futures import Future, ThreadPoolExecutor, wait

    if not scr:
        return 0  # We have successfully done nothing. Hard to fail that.

    go_on = True

    def inner_predicate() -> bool:
        return go_on and predicate()

    def update_stop_condition(result: int) -> int:
        nonlocal go_on
        assert isinstance(result, int)
        go_on = go_on and (result == 0)
        return result

    def launch_shell(inner_loc: str, cmd: str) -> Future[int]:
        return executor.submit(
            lambda: update_stop_condition(
                exec_shell(inner_loc, cmd, env.copy(), kill_timeout=kill_timeout, predicate=inner_predicate)
            )
        )

    def launch_composition(inner_loc: str, comp: Composition) -> Future[int]:
        return executor.submit(
            lambda: update_stop_condition(exec_composition(inner_loc, comp, predicate=inner_predicate))
        )

    try:
        executor = ThreadPoolExecutor(max_workers=len(scr))
        pending: List[Future[int]] = []
        for index, stmt in enumerate(scr):
            stmt_loc = f"{loc}/{index}"
            if not inner_predicate():
                break
            if isinstance(stmt, ShellStatement):
                pending.append(launch_shell(stmt_loc, stmt.cmd))
            elif isinstance(stmt, CompositionStatement):
                pending.append(launch_composition(stmt_loc, stmt.comp))
            elif isinstance(stmt, JoinStatement):
                num_pending = sum(1 for x in pending if not x.done())
                _log_execution_point(stmt_loc, f"waiting for {num_pending} statements")
                wait(pending)
            else:
                assert False

        # Wait for all processes to complete and then aggregate the results.
        done, not_done = wait(pending)
        assert not not_done
        results = list(x.result() for x in done)
        if not results or (set(results) == {0}):
            return 0
        res = list(sorted(results, key=lambda x: -abs(x)))[0]
        assert res != 0
        return res
    finally:
        go_on = False  # Terminate processes in the event of an exception.


def exec_shell(loc: str, cmd: str, env: Dict[str, str], *, kill_timeout: float, predicate: FlagDelegate) -> int:
    _log_execution_point(loc, f"executing: {cmd}")
    longest_env_name = max(len(x) for x in env)
    for k, v in env.items():
        _log_execution_point(loc, "envvar: " + k.ljust(longest_env_name) + f" = {v!r}")

    ch = Child(cmd, env)
    try:
        ret: Optional[int] = None
        while predicate() and ret is None:
            ret = ch.poll(_POLL_INTERVAL)
        if ret is None:
            _log_execution_point(loc, "stopping")
            ch.initiate_stopping_sequence(kill_timeout * 0.5, kill_timeout)
        while ret is None:
            ret = ch.poll(_POLL_INTERVAL)
        _log_execution_point(loc, f"exit code: {ret}")
        return ret
    finally:
        ch.ensure_dead()  # Terminate process in the event of an exception.


def _log_execution_point(loc: str, text: str) -> None:
    _logger.info("@%s: %s", loc, text)


class Child:
    def __init__(self, cmd: str, env: Dict[str, str]) -> None:
        from subprocess import Popen, PIPE, DEVNULL

        e = os.environ.copy()
        e.update(env)
        self._proc = Popen(cmd, env=e, shell=True, text=True, stdout=PIPE, stderr=PIPE, stdin=DEVNULL, bufsize=1)
        self._return: Optional[int] = None
        self._signaling_schedule: List[Tuple[float, Callable[[], None]]] = []
        self._prefix_stdout = click.style(f"STDOUT{self.pid: 8d}: ", fg="green", bold=True)
        self._prefix_stderr = click.style(f"STDERR{self.pid: 8d}: ", fg="yellow", bold=True)

    @property
    def pid(self) -> int:
        return self._proc.pid

    def poll(self, timeout: float) -> Optional[int]:
        if self._signaling_schedule:
            deadline, handler = self._signaling_schedule[0]
            if time.monotonic() >= deadline:
                self._signaling_schedule.pop(0)
                handler()

        stdout, stderr = self._proc.communicate(timeout=timeout)
        for s in stdout.splitlines():
            _echo(self._prefix_stdout + s)
        for s in stderr.splitlines():
            _echo(self._prefix_stderr + s)

        if self._return is None and self._proc.returncode is not None:
            self._return = self._proc.returncode
            _logger.debug("%s: Exited with code %d", self, self._return)

        return self._return

    def initiate_stopping_sequence(self, escalate_after: float, give_up_after: float) -> None:
        if self._return is not None:
            return

        give_up_after = max(give_up_after, escalate_after)
        _logger.debug(
            "%s: Interrupting. Escalation timeout: %.1f, give-up timeout: %.1f", self, escalate_after, give_up_after
        )
        # FIXME: on Windows, killing the shell process does not terminate its children.
        # TODO: use psutil to manually kill off the children one by one.
        self._proc.send_signal(signal.SIGBREAK if sys.platform.startswith("win") else signal.SIGINT)

        def term() -> None:
            _logger.warning("%s: Interrupt signal had no effect, trying to terminate")
            self._proc.send_signal(signal.SIGTERM)

        def kill() -> None:
            _logger.error("%s: Termination signal had no effect, giving up by killing and abandoning the child", self)
            self._proc.send_signal(signal.SIGABRT if sys.platform.startswith("win") else signal.SIGKILL)
            self._return = -errno.ETIMEDOUT

        self._signaling_schedule = [
            (time.monotonic() + escalate_after, term),
            (time.monotonic() + give_up_after, kill),
        ]

    def ensure_dead(self) -> None:
        self._proc.kill()

    def __str__(self) -> str:
        return f"Child {self.pid:07d}"


@dataclasses.dataclass(frozen=True)
class Composition:
    predicate: List[Statement]
    start: List[Statement]
    stop: List[Statement]
    env: Dict[str, str]

    kill_timeout: float = 20.0


@dataclasses.dataclass(frozen=True)
class Statement:
    pass


@dataclasses.dataclass(frozen=True)
class ShellStatement(Statement):
    cmd: str


@dataclasses.dataclass(frozen=True)
class CompositionStatement(Statement):
    comp: Composition


@dataclasses.dataclass(frozen=True)
class JoinStatement(Statement):
    pass


def _load_yaml(yaml_file: Path) -> Dict[Any, Any]:
    with open(yaml_file.resolve(), "r", encoding="utf8") as f:
        return YAMLLoader().load(f.read())


def _parse(ast: Dict[Any, Any], env: Dict[str, str]) -> Composition:
    """
    Environment inheritance order (last entry takes precedence):

    - Parent process environment (i.e., the environment the orchestrator is invoked from).
    - Outer composition environment (e.g., root members of the orchestration file).
    - Extension orchestration files.
    - Local environment variables.
    """
    if not isinstance(ast, dict):
        raise SchemaError(f"The composition shall be a dict, not {type(ast).__name__}")

    extend = ast.pop("=import", None)
    if extend is not None:
        if not isinstance(extend, str):
            raise SchemaError(f"Invalid extend specifier: {extend!r}")
        ext = _parse(_load_yaml(Path(extend)), env.copy())
    else:
        ext = Composition(predicate=[], start=[], stop=[], env=env.copy())
    del env

    for name, value in _flatten_registers({k: v for k, v in ast.items() if isinstance(k, str) and _NOT_ENV not in k}):
        if _NAME_SEP in name:  # UAVCAN register.
            name, value = _canonicalize_register(name, value)
            name = name.upper().replace(_NAME_SEP, "_" * 2)
        ext.env[name] = str(value)

    out = Composition(
        predicate=_parse_script(ast.pop("=?", []), ext.env) + ext.predicate,
        start=_parse_script(ast.pop("=$", []), ext.env) + ext.start,
        stop=_parse_script(ast.pop("=.", []), ext.env) + ext.stop,
        env=ext.env,
    )
    unattended = [k for k in ast if _NOT_ENV in k]
    if unattended:
        raise SchemaError(f"Unknown directives: {unattended}")
    return out


def _parse_script(ast: Any, env: Dict[str, str]) -> List[Statement]:
    if isinstance(ast, list):
        return [_parse_statement(x, env) for x in ast]
    return [_parse_statement(ast, env)]


def _parse_statement(ast: Any, env: Dict[str, str]) -> Statement:
    if isinstance(ast, str):
        return ShellStatement(ast)
    if isinstance(ast, dict):
        return CompositionStatement(_parse(ast, env))
    if ast is None:
        return JoinStatement()
    raise SchemaError("Statement shall be either: string (command to run), dict (nested schema), null (join)")


@lru_cache(None)
def _get_register_value_option_names() -> List[str]:
    from uavcan.register import Value_1_0 as Value

    return [x for x in dir(Value) if not x.startswith("_")]


def _canonicalize_register(name: str, value: Any) -> Tuple[str, str]:
    """
    Ensures that the name has the correct type suffux and converts the value to string.

    >>> _canonicalize_register('foo.empty', ['this', 'is', 'ignored'])
    ('foo.empty', '')
    >>> _canonicalize_register('foo.string', 123)
    ('foo.string', '123')
    >>> _canonicalize_register('foo', 'hello')  # Auto-detect.
    ('foo.string', 'hello')
    >>> _canonicalize_register('foo', b'hello')
    ('foo.unstructured', '68656c6c6f')
    >>> _canonicalize_register('foo.unstructured', '68656c6c6f')  # Same, just different notation.
    ('foo.unstructured', '68656c6c6f')
    >>> _canonicalize_register('foo', [True, False, True])
    ('foo.bit', '1 0 1')
    >>> _canonicalize_register('foo', [60_000, 50_000])
    ('foo.natural16', '60000 50000')
    >>> _canonicalize_register('foo', 300_000)
    ('foo.natural32', '300000')
    >>> _canonicalize_register('foo', [2 ** 32, 0])
    ('foo.natural64', '4294967296 0')
    >>> _canonicalize_register('foo', -10_000)
    ('foo.integer16', '-10000')
    >>> _canonicalize_register('foo', [-10_000, 40_000])
    ('foo.integer32', '-10000 40000')
    >>> _canonicalize_register('foo', [-(2 ** 31), 2 ** 31])
    ('foo.integer64', '-2147483648 2147483648')
    >>> _canonicalize_register('foo', 1.0)
    ('foo.real64', '1.0')
    >>> _canonicalize_register('foo', [1, 'a'])  # doctest: +IGNORE_EXCEPTION_DETAIL
    Traceback (most recent call last):
    ...
    EnvironmentVariableError: ...
    """
    for val_opt_name in _get_register_value_option_names():
        suffix = _NAME_SEP + val_opt_name
        if name.endswith(suffix):
            if val_opt_name == "empty":
                return name, ""
            if val_opt_name == "string":
                return name, str(value)
            if val_opt_name == "unstructured":
                try:
                    if not isinstance(value, bytes):
                        value = bytes.fromhex(str(value))
                except ValueError:
                    raise EnvironmentVariableError(f"{name!r}: expected bytes or hex-encoded string") from None
                return name, value.hex()
            # All other values are vector values. Converge scalars to one-dimensional vectors:
            try:
                value = list(value)
            except TypeError:
                value = [value]
            if val_opt_name == "bit":
                return name, _ITEM_SEP.join(("1" if x else "0") for x in value)
            if val_opt_name.startswith("integer") or val_opt_name.startswith("natural"):
                return name, _ITEM_SEP.join(str(int(x)) for x in value)
            if val_opt_name.startswith("real"):
                return name, _ITEM_SEP.join(str(float(x)) for x in value)
            assert False, f"Internal error: unhandled value option: {val_opt_name}"

    def convert(ty: str) -> Tuple[str, str]:
        assert ty in _get_register_value_option_names()
        return _canonicalize_register(name + _NAME_SEP + ty, value)

    # Type not specified. Perform auto-detection.
    if isinstance(value, str):
        return convert("string")
    if isinstance(value, bytes):
        return convert("unstructured")
    # All other values are vector values. Converge scalars to one-dimensional vectors:
    try:
        value = list(value)
    except TypeError:
        value = [value]
    if all(isinstance(x, bool) for x in value):
        return convert("bit")
    if all(isinstance(x, int) for x in value):
        # fmt: off
        if all(0          <= x < (2 ** 16) for x in value): return convert("natural16")
        if all(0          <= x < (2 ** 32) for x in value): return convert("natural32")
        if all(0          <= x < (2 ** 64) for x in value): return convert("natural64")
        if all(-(2 ** 15) <= x < (2 ** 15) for x in value): return convert("integer16")
        if all(-(2 ** 31) <= x < (2 ** 31) for x in value): return convert("integer32")
        if all(-(2 ** 63) <= x < (2 ** 63) for x in value): return convert("integer64")
        # fmt: on
    if all(isinstance(x, (int, float)) for x in value):
        return convert("real64")

    raise EnvironmentVariableError(f"Cannot infer the type of {name!r}")


def _flatten_registers(spec: Dict[str, Any], prefix: str = "") -> Dict[str, Any]:
    """
    >>> _flatten_registers({"FOO": "BAR", "a": {"b": 123, "c": [456, 789]}})  # doctest: +NORMALIZE_WHITESPACE
    {'FOO': 'BAR',
     'a.b': 123,
     'a.c': [456, 789]}
    """
    out: Dict[str, Any] = {}
    for k, v in spec.items():
        name = _NAME_SEP.join((prefix, k)) if prefix else k
        if isinstance(v, dict):
            out.update(_flatten_registers(v, name))
        else:
            out[name] = v
    return out


def _echo(text: str, **styles: Union[str, bool]) -> None:
    if styles:
        click.secho(text, err=True, **styles)
    else:
        click.echo(text, err=True)
