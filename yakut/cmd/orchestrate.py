# Copyright (c) 2021 UAVCAN Consortium
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@uavcan.org>

from __future__ import annotations
import os
import io
import sys
import time
import signal
import dataclasses
from concurrent.futures import Future, ThreadPoolExecutor, wait
from typing import Dict, List, Any, Tuple, Union, Optional, Callable
from functools import lru_cache, partial
from pathlib import Path
import logging
import click
import yakut
from yakut.yaml import YAMLLoader


_logger = logging.getLogger(__name__)

_NAME_SEP = "."
_ITEM_SEP = " "

_NOT_ENV = "="
"""Equals sign is the only character that cannot occur in an environment variable name in most OS."""

_POLL_INTERVAL = 0.05


class SchemaError(ValueError):
    pass


class EnvironmentVariableError(SchemaError):
    pass


@yakut.subcommand()
@click.argument("yaml_file", type=str)
def orchestrate(yaml_file: str) -> None:
    """
    Execute an orchestration file.
    """
    sig_num = 0

    def on_signal(s: int, _: Any) -> None:
        nonlocal sig_num
        sig_num = s
        _logger.info("Orchestrator received signal %s %r, stopping", s, signal.strsignal(s))

    for sig in [signal.SIGABRT, signal.SIGINT, signal.SIGTERM]:
        signal.signal(sig, on_signal)
    if sys.platform.startswith("win"):
        signal.signal(signal.SIGBREAK, on_signal)
    else:
        signal.signal(signal.SIGHUP, on_signal)

    res = exec_file("", yaml_file, {}, predicate=lambda: sig_num == 0)

    exit(res if res != 0 else sig_num)


FlagDelegate = Callable[[], bool]


def exec_file(loc: str, yaml_file: str, env: Dict[str, str], *, predicate: FlagDelegate) -> int:
    # TODO: locate YAML in YAKUT_PATH
    _log_execution(loc, yaml_file)
    comp = parse_composition(_load_yaml(Path(yaml_file)), env)
    _logger.debug("Parsed file %s with env keys %s", yaml_file, list(env))
    return exec_composition(f"{loc}/{str(yaml_file)!r}", comp, predicate=predicate)


def exec_composition(loc: str, comp: Composition, *, predicate: FlagDelegate) -> int:
    def do(inner_loc: str, scr: List[Statement]) -> int:
        started_at = time.monotonic()
        res = exec_script(inner_loc, scr, comp.env, kill_timeout=comp.kill_timeout, predicate=predicate)
        elapsed = time.monotonic() - started_at
        _log_execution(inner_loc, f"script exit status={res} elapsed={elapsed:.1f}")
        return res

    def finalize() -> int:
        return do(f"{loc}/.", comp.finalizer)

    res = do(f"{loc}/?", comp.predicate)
    if res != 0:  # If any of the commands of the predicate fails, we simply skip the rest and report success.
        return 0

    res = do(f"{loc}/$", comp.main)
    if res != 0:  # The return code of a composition is that of the first failed process.
        finalize()
        return res
    return finalize()


def exec_script(
    loc: str, scr: List[Statement], env: Dict[str, str], *, kill_timeout: float, predicate: FlagDelegate
) -> int:
    """
    :return: Exit code of the first statement to fail. Zero if all have succeeded.
    """
    if not scr:
        return 0  # We have successfully done nothing. Hard to fail that.

    first_failure_code: Optional[int] = None

    def inner_predicate() -> bool:
        return (first_failure_code is None) and predicate()

    def accept_result(result: int) -> None:
        nonlocal first_failure_code
        assert isinstance(result, int)
        if result != 0 and first_failure_code is None:
            first_failure_code = result  # Script ALWAYS returns the code of the FIRST FAILED statement.

    def launch_shell(inner_loc: str, cmd: str) -> Future[None]:
        return executor.submit(
            lambda: accept_result(
                exec_shell(inner_loc, cmd, env.copy(), kill_timeout=kill_timeout, predicate=inner_predicate)
            )
        )

    def launch_composition(inner_loc: str, comp: Composition) -> Future[None]:
        return executor.submit(lambda: accept_result(exec_composition(inner_loc, comp, predicate=inner_predicate)))

    executor = ThreadPoolExecutor(max_workers=len(scr))
    pending: List[Future[None]] = []
    try:
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
                _log_execution(stmt_loc, f"waiting on {num_pending} statements")
                wait(pending)
            else:
                assert False

        # Wait for all statements to complete and then aggregate the results.
        done, not_done = wait(pending)
        assert not not_done
        _ = list(x.result() for x in done)  # Collect results explicitly to propagate exceptions.
        if first_failure_code is not None:
            assert first_failure_code != 0
            return first_failure_code
        return 0
    except Exception:
        first_failure_code = 1
        raise


def exec_shell(loc: str, cmd: str, env: Dict[str, str], *, kill_timeout: float, predicate: FlagDelegate) -> int:
    started_at = time.monotonic()
    ch = Child(
        cmd,
        env,
        line_handler_stdout=partial(print),
        line_handler_stderr=partial(print, file=sys.stderr),
    )
    cli_prefix = f"[PID {ch.pid:07d}] "
    try:
        _log_execution(loc, f"shell pid={ch.pid:07d} env={env} cmd=\\\n{cmd}")
        click.echo(
            click.style(cli_prefix + f"Started with {len(env)} envvars:\n", fg="bright_cyan", bold=True)
            + click.style(cmd, fg="cyan"),
            err=True,
        )

        ret: Optional[int] = None
        while predicate() and ret is None:
            ret = ch.poll(_POLL_INTERVAL)
        if ret is None:
            _log_execution(loc, "stopping")
            ch.initiate_stopping_sequence(kill_timeout * 0.5, kill_timeout)
        while ret is None:
            ret = ch.poll(_POLL_INTERVAL)

        elapsed = time.monotonic() - started_at
        _log_execution(loc, f"exit pid={ch.pid:07d} status={ret} elapsed={elapsed:.1f}")
        if ret != 0:
            click.secho(cli_prefix + f"Nonzero exit status: {ret}", fg="bright_yellow", err=True)
        return ret
    finally:
        ch.ensure_dead()  # Terminate process in the event of an exception.


def _log_execution(loc: str, text: str) -> None:
    _logger.info("At %s: %s", loc, text)


class Child:
    _STREAM_BUFFER_SIZE = 1024 ** 2
    _STREAM_POLL_INTERVAL = 0.01

    def __init__(
        self,
        cmd: str,
        env: Dict[str, str],
        line_handler_stdout: Callable[[str], None],
        line_handler_stderr: Callable[[str], None],
    ) -> None:
        from subprocess import Popen, PIPE, DEVNULL

        e = os.environ.copy()
        e.update(env)
        self._proc = Popen(
            cmd,
            env=e,
            shell=True,
            text=True,
            stdout=PIPE,
            stderr=PIPE,
            stdin=DEVNULL,
            bufsize=Child._STREAM_BUFFER_SIZE,
        )
        self._return: Optional[int] = None
        self._executor = ThreadPoolExecutor(max_workers=10)
        self._futures = {
            self._executor.submit(self._forward_line_by_line, self._proc.stdout, line_handler_stdout),
            self._executor.submit(self._forward_line_by_line, self._proc.stderr, line_handler_stderr),
        }

    @property
    def pid(self) -> int:
        return self._proc.pid

    def poll(self, timeout: float) -> Optional[int]:
        # Propagate exceptions, if any, from the background tasks.
        done, self._futures = wait(self._futures, timeout=0)
        for f in done:
            f.result()

        if self._return is None:
            ret = self._proc.poll()
            if ret is None:
                time.sleep(timeout)
                ret = self._proc.poll()
            if ret is not None:
                self._return = ret
        return self._return

    def initiate_stopping_sequence(self, escalate_after: float, give_up_after: float) -> None:
        if self._return is not None or self._proc.poll() is not None:
            return
        _logger.debug(
            "%s: Interrupting. Escalation timeout: %.1f, give-up timeout: %.1f", self, escalate_after, give_up_after
        )
        # FIXME: on Windows, killing the shell process does not terminate its children.
        # TODO: use psutil to manually hunt down each child and kill them off one by one.
        self._proc.send_signal(signal.SIGBREAK if sys.platform.startswith("win") else signal.SIGINT)

        def do_stop() -> None:
            time.sleep(escalate_after)
            if self._proc.poll() is not None:
                return
            _logger.warning("%s: Interrupt signal had no effect, trying to terminate", self)
            self._proc.send_signal(signal.SIGTERM)
            time.sleep(give_up_after - escalate_after)
            if self._proc.poll() is not None:
                return
            _logger.error("%s: Termination signal had no effect, giving up by killing and abandoning the child", self)
            sig = signal.SIGABRT if sys.platform.startswith("win") else signal.SIGKILL
            self._proc.send_signal(sig)
            if self._return is None:
                self._return = -sig

        self._futures.add(self._executor.submit(do_stop))

    def ensure_dead(self) -> None:
        self._proc.kill()

    def _forward_line_by_line(self, stream: io.TextIOWrapper, line_handler: Callable[[str], None]) -> None:
        buf = io.StringIO()
        while True:
            ch = stream.read(1)
            if ch:
                if ch == "\n":
                    line_handler(buf.getvalue())
                    buf = io.StringIO()
                else:
                    buf.write(ch)
            elif self._return is not None or self._proc.poll() is not None:
                break
            else:
                time.sleep(Child._STREAM_POLL_INTERVAL)
        if buf.getvalue():
            line_handler(buf.getvalue())

    def __str__(self) -> str:
        return f"Child {self.pid:07d}"


@dataclasses.dataclass(frozen=True)
class Composition:
    predicate: List[Statement]
    main: List[Statement]
    finalizer: List[Statement]
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


def parse_composition(ast: Dict[Any, Any], env: Dict[str, str]) -> Composition:
    """
    Environment inheritance order (last entry takes precedence):

    - Parent process environment (i.e., the environment the orchestrator is invoked from).
    - Outer composition environment (e.g., root members of the orchestration file).
    - Extension orchestration files.
    - Local environment variables.
    """
    if not isinstance(ast, dict):
        raise SchemaError(f"The composition shall be a dict, not {type(ast).__name__}")

    imp = ast.pop("import=", None)
    if imp is not None:
        if not isinstance(imp, str):
            raise SchemaError(f"Invalid import specifier: {imp!r}")
        ext = parse_composition(_load_yaml(Path(imp)), env.copy())
    else:
        ext = Composition(predicate=[], main=[], finalizer=[], env=env.copy())
    del env

    for name, value in _flatten_registers(
        {k: v for k, v in ast.items() if isinstance(k, str) and _NOT_ENV not in k}
    ).items():
        if _NAME_SEP in name:  # UAVCAN register.
            name, value = _canonicalize_register(name, value)
            name = name.upper().replace(_NAME_SEP, "_" * 2)
        ext.env[name] = str(value)

    out = Composition(
        predicate=parse_script(ast.pop("?=", []), ext.env) + ext.predicate,
        main=parse_script(ast.pop("$=", []), ext.env) + ext.main,
        finalizer=parse_script(ast.pop(".=", []), ext.env) + ext.finalizer,
        env=ext.env,
    )
    unattended = [k for k in ast if _NOT_ENV in k]
    if unattended:
        raise SchemaError(f"Unknown directives: {unattended}")
    return out


def parse_script(ast: Any, env: Dict[str, str]) -> List[Statement]:
    if isinstance(ast, list):
        return [parse_statement(x, env) for x in ast]
    return [parse_statement(ast, env)]


def parse_statement(ast: Any, env: Dict[str, str]) -> Statement:
    if isinstance(ast, str):
        return ShellStatement(ast)
    if isinstance(ast, dict):
        return CompositionStatement(parse_composition(ast, env))
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
