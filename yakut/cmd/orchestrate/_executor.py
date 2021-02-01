# Copyright (c) 2021 UAVCAN Consortium
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@uavcan.org>

from __future__ import annotations
import sys
import time
import enum
import itertools
import dataclasses
from concurrent.futures import Future, ThreadPoolExecutor, wait
from typing import Dict, List, Optional, Callable, Any, Sequence
from pathlib import Path
import logging
from ._child import Child
from ._schema import Composition, load_composition, load_ast, SchemaError
from ._schema import Statement, ShellStatement, CompositionStatement, JoinStatement


FlagDelegate = Callable[[], bool]


class ErrorCode(enum.IntEnum):
    """
    POSIX systems can safely use exit codes in [0, 125]. We use the upper range for own errors.
    https://unix.stackexchange.com/questions/418784/what-is-the-min-and-max-values-of-exit-codes-in-linux
    """

    SCHEMA_ERROR = 125
    FILE_ERROR = 124


@dataclasses.dataclass(frozen=True)
class Context:
    lookup_paths: Sequence[Path]
    poll_interval: float = 0.05


def locate(ctx: Context, file: str) -> Optional[Path]:
    p = Path(file)
    if p.is_absolute():
        if p.exists():
            return p
    else:
        for p in ctx.lookup_paths:
            p = (p / file).resolve()
            if p.exists():
                return p
    return None


def exec_file(
    ctx: Context, file: str, inout_env: Dict[str, str], *, gate: FlagDelegate, stack: Optional[Stack] = None
) -> int:
    """
    This function never raises exceptions in response to invalid syntax or a programmable error.
    Instead, it uses exit codes to report failures, to unify behavior with invoked processes.
    An exception may only indicate a bug in the implementation or an internal contract violation.

    The env var dict is used both as the input and the output.
    The provided values are inherited by the executed composition.
    Afterwards, they are updated with the variables defined by the composition, which take precedence
    over the supplied variables.
    """
    stack = stack or Stack()
    stack.log_debug(f"Locating file: {file!r} in:", *map(str, ctx.lookup_paths))
    pth = locate(ctx, file)
    if not pth:
        stack.log_warning(f"Cannot locate file {file!r} in:", *map(str, ctx.lookup_paths))
        return int(ErrorCode.FILE_ERROR)

    stack.log_debug(f"Executing file {file!r} found at: {pth}")
    try:
        source_text = pth.read_text()
    except Exception as ex:  # pylint: disable=broad-except
        stack.log_warning(f"Cannot read file {pth}: {ex}")
        return int(ErrorCode.FILE_ERROR)

    try:
        comp = load_composition(load_ast(source_text), inout_env.copy())
    except SchemaError as ex:
        stack.log_warning(f"Cannot load file {pth}: {ex}")
        return int(ErrorCode.SCHEMA_ERROR)

    # Export the variables to the caller. Vars from the composition override the supplied vars.
    inout_env.update(comp.env)

    stack = stack.push(repr(file))
    stack.log_debug(f"Loaded composition:", str(comp))
    return exec_composition(ctx, comp, gate=gate, stack=stack)


def exec_composition(ctx: Context, comp: Composition, *, gate: FlagDelegate, stack: Stack) -> int:
    env = comp.env.copy()
    for ca in comp.calls:  # The "env" is updated in-place.
        res = exec_file(ctx, ca.file, env, gate=gate, stack=stack.push("call"))
        if res != 0:
            return res

    def scr(node: str, scr: Sequence[Statement], inner_gate: FlagDelegate = lambda: True) -> int:
        inner_stack = stack.push(node)
        started_at = time.monotonic()
        res = exec_script(ctx, scr, env.copy(), kill_timeout=comp.kill_timeout, gate=inner_gate, stack=inner_stack)
        elapsed = time.monotonic() - started_at
        inner_stack.log_debug(f"Script exit status {res} in {elapsed:.1f} sec")
        return res

    res = scr("?", comp.predicate, gate)
    if res != 0:  # If any of the commands of the predicate fail, we simply skip the rest and report success.
        return 0

    res = scr("$", comp.main, gate)
    if res != 0:  # The return code of a composition is that of the first failed process.
        scr(".", comp.finalizer)
        return res

    return scr(".", comp.finalizer)


def exec_script(
    ctx: Context,
    scr: Sequence[Statement],
    env: Dict[str, str],
    *,
    kill_timeout: float,
    gate: FlagDelegate,
    stack: Stack,
) -> int:
    """
    :return: Exit code of the first statement to fail. Zero if all have succeeded.
    """
    if not scr:
        return 0  # We have successfully done nothing. Hard to fail that.

    first_failure_code: Optional[int] = None

    def inner_gate() -> bool:
        return (first_failure_code is None) and gate()

    def accept_result(result: int) -> None:
        nonlocal first_failure_code
        assert isinstance(result, int)
        if result != 0 and first_failure_code is None:
            first_failure_code = result  # Script ALWAYS returns the code of the FIRST FAILED statement.

    def launch_shell(inner_stack: Stack, cmd: str) -> Future[None]:
        return executor.submit(
            lambda: accept_result(
                exec_shell(ctx, cmd, env.copy(), kill_timeout=kill_timeout, gate=inner_gate, stack=inner_stack)
            )
        )

    def launch_composition(inner_stack: Stack, comp: Composition) -> Future[None]:
        return executor.submit(lambda: accept_result(exec_composition(ctx, comp, gate=inner_gate, stack=inner_stack)))

    executor = ThreadPoolExecutor(max_workers=len(scr))
    pending: List[Future[None]] = []
    try:
        for index, stmt in enumerate(scr):
            stmt_stack = stack.push(index)
            if not inner_gate():
                break
            if isinstance(stmt, ShellStatement):
                pending.append(launch_shell(stmt_stack, stmt.cmd))
            elif isinstance(stmt, CompositionStatement):
                pending.append(launch_composition(stmt_stack, stmt.comp))
            elif isinstance(stmt, JoinStatement):
                num_pending = sum(1 for x in pending if not x.done())
                stmt_stack.log_debug(f"Waiting for {num_pending} pending statements to join")
                if pending:
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


def exec_shell(
    ctx: Context, cmd: str, env: Dict[str, str], *, kill_timeout: float, gate: FlagDelegate, stack: Stack
) -> int:
    started_at = time.monotonic()
    ch = Child(cmd, env, stdout=sys.stdout.buffer, stderr=sys.stderr.buffer)
    prefix = f"PID={ch.pid:08d} "
    try:
        longest_env = max(map(len, env.keys())) if env else 0
        stack.log_info(
            *itertools.chain(
                (f"{prefix}EXECUTING WITH ENVIRONMENT VARIABLES:",),
                ((k.ljust(longest_env) + " = " + repr(v)) for k, v in env.items()) if env else ["<no variables>"],
                cmd.splitlines(),
            ),
        )
        ret: Optional[int] = None
        while gate() and ret is None:
            ret = ch.poll(ctx.poll_interval)
        if ret is None:
            stack.log_warning(f"{prefix}Stopping (was started {time.monotonic() - started_at:.1f} sec ago)")
            ch.stop(kill_timeout * 0.75, kill_timeout)
        while ret is None:
            ret = ch.poll(ctx.poll_interval)

        elapsed = time.monotonic() - started_at
        stack.log_info(f"{prefix}Exit status {ret} in {elapsed:.1f} sec")
        return ret
    finally:
        ch.kill()


class Stack:
    def __init__(self, path: Optional[List[str]] = None, logger: Optional[logging.Logger] = None) -> None:
        from . import __name__ as nm

        self._path = path or []
        self._logger = logger or logging.getLogger(nm)

    def push(self, node: Any) -> Stack:
        if isinstance(node, Path):
            node = repr(str(node))
        else:
            node = str(node)
        assert isinstance(node, str)
        return Stack(self._path + [node], self._logger)

    def log(self, level: int, *lines: str) -> None:
        if self._logger.isEnabledFor(level):
            self._logger.log(level, "Call stack: %s\n%s", self, "\n".join(lines))

    def log_debug(self, *lines: str) -> None:
        return self.log(logging.DEBUG, *lines)

    def log_info(self, *lines: str) -> None:
        return self.log(logging.INFO, *lines)

    def log_warning(self, *lines: str) -> None:
        return self.log(logging.WARNING, *lines)

    def __str__(self) -> str:
        return " ".join(self._path)
