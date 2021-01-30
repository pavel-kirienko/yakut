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
from functools import partial
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
    ctx: Context, file: str, env: Dict[str, str], *, predicate: FlagDelegate, stack: Optional[Stack] = None
) -> int:
    """
    This function never raises exceptions in response to invalid syntax or a programmable error.
    Instead, it uses exit codes to report failures, to unify behavior with invoked processes.
    An exception may only indicate a bug in the implementation or an internal contract violation.
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
    except Exception as ex:
        stack.log_warning(f"Cannot read file {pth}: {ex}")
        return int(ErrorCode.FILE_ERROR)

    try:
        comp = load_composition(load_ast(source_text), env)
    except SchemaError as ex:
        stack.log_warning(f"Cannot load file {pth}: {ex}")
        return int(ErrorCode.SCHEMA_ERROR)

    stack = stack.push(pth)
    stack.log_debug(f"Loaded composition:", str(comp))
    return exec_composition(ctx, comp, predicate=predicate, stack=stack)


def exec_composition(ctx: Context, comp: Composition, *, predicate: FlagDelegate, stack: Stack) -> int:
    def scr(node: str, scr: Sequence[Statement], inner_predicate: FlagDelegate) -> int:
        inner_stack = stack.push(node)
        started_at = time.monotonic()
        res = exec_script(
            ctx, scr, comp.env.copy(), kill_timeout=comp.kill_timeout, predicate=inner_predicate, stack=inner_stack
        )
        elapsed = time.monotonic() - started_at
        inner_stack.log_debug(f"Script exit status {res} in {elapsed:.1f} sec")
        return res

    def finalize() -> int:
        return scr(".", comp.finalizer, lambda: True)  # Finalizers cannot be interrupted.

    res = scr("?", comp.predicate, predicate)
    if res != 0:  # If any of the commands of the predicate fails, we simply skip the rest and report success.
        return 0

    res = scr("$", comp.main, predicate)
    if res != 0:  # The return code of a composition is that of the first failed process.
        finalize()
        return res

    if comp.delegate is not None:
        res = exec_file(ctx, comp.delegate, comp.env.copy(), predicate=predicate, stack=stack.push("delegate"))
        if res != 0:
            finalize()
            return res

    return finalize()


def exec_script(
    ctx: Context,
    scr: Sequence[Statement],
    env: Dict[str, str],
    *,
    kill_timeout: float,
    predicate: FlagDelegate,
    stack: Stack,
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

    def launch_shell(inner_stack: Stack, cmd: str) -> Future[None]:
        return executor.submit(
            lambda: accept_result(
                exec_shell(
                    ctx, cmd, env.copy(), kill_timeout=kill_timeout, predicate=inner_predicate, stack=inner_stack
                )
            )
        )

    def launch_composition(inner_stack: Stack, comp: Composition) -> Future[None]:
        return executor.submit(
            lambda: accept_result(exec_composition(ctx, comp, predicate=inner_predicate, stack=inner_stack))
        )

    executor = ThreadPoolExecutor(max_workers=len(scr))
    pending: List[Future[None]] = []
    try:
        for index, stmt in enumerate(scr):
            stmt_stack = stack.push(index)
            if not inner_predicate():
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
    ctx: Context, cmd: str, env: Dict[str, str], *, kill_timeout: float, predicate: FlagDelegate, stack: Stack
) -> int:
    started_at = time.monotonic()
    ch = Child(
        cmd,
        env,
        line_handler_stdout=partial(print),
        line_handler_stderr=partial(print, file=sys.stderr),
    )
    prefix = f"PID={ch.pid:08d} "
    try:
        longest_env = max(map(len, env.keys()))
        stack.log_info(
            *itertools.chain(
                (f"{prefix}EXECUTING WITH ENVIRONMENT VARIABLES:",),
                ((k.ljust(longest_env) + " = " + repr(v)) for k, v in env.items()),
                cmd.splitlines(),
            ),
        )
        ret: Optional[int] = None
        while predicate() and ret is None:
            ret = ch.poll(ctx.poll_interval)
        if ret is None:
            stack.log_warning(f"{prefix}Stopping manually")
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
            self._logger.log(level, f"Call stack: {self}\n" + "\n".join(lines))

    def log_debug(self, *lines: str) -> None:
        return self.log(logging.DEBUG, *lines)

    def log_info(self, *lines: str) -> None:
        return self.log(logging.INFO, *lines)

    def log_warning(self, *lines: str) -> None:
        return self.log(logging.WARNING, *lines)

    def __str__(self) -> str:
        return " ".join(self._path)
