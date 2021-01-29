# Copyright (c) 2021 UAVCAN Consortium
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@uavcan.org>

from __future__ import annotations
import sys
import time
from concurrent.futures import Future, ThreadPoolExecutor, wait
from typing import Dict, List, Optional, Callable, Any
from functools import partial
from pathlib import Path
import logging
import click
from ._child import Child
from ._schema import Composition, load_composition, parse
from ._schema import Statement, ShellStatement, CompositionStatement, JoinStatement


FlagDelegate = Callable[[], bool]

_POLL_INTERVAL = 0.05

_logger = logging.getLogger(__name__)


def exec_file(file: str, env: Dict[str, str], *, predicate: FlagDelegate, stack: Optional[Stack] = None) -> int:
    # TODO: locate YAML in YAKUT_PATH
    pth = Path(file).resolve()
    stack = stack.push(repr(str(pth)))
    stack.trace(file)
    comp = load_composition(parse(pth), env)
    _logger.debug("Parsed file %s using external env keys %s", pth, list(env))
    return exec_composition(comp, predicate=predicate, stack=stack)


def exec_composition(comp: Composition, *, predicate: FlagDelegate, stack: Stack) -> int:
    def do(node: str, scr: List[Statement], inner_predicate: FlagDelegate) -> int:
        inner_stack = stack.push(node)
        started_at = time.monotonic()
        res = exec_script(scr, comp.env, kill_timeout=comp.kill_timeout, predicate=inner_predicate, stack=inner_stack)
        elapsed = time.monotonic() - started_at
        inner_stack.trace(f"script exit status={res} elapsed={elapsed:.1f}")
        return res

    def finalize() -> int:
        return do(".", comp.finalizer, lambda: True)  # Finalizers cannot be interrupted.

    res = do("?", comp.predicate, predicate)
    if res != 0:  # If any of the commands of the predicate fails, we simply skip the rest and report success.
        return 0

    res = do("$", comp.main, predicate)
    if res != 0:  # The return code of a composition is that of the first failed process.
        finalize()
        return res
    return finalize()


def exec_script(
    scr: List[Statement], env: Dict[str, str], *, kill_timeout: float, predicate: FlagDelegate, stack: Stack
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
                exec_shell(cmd, env.copy(), kill_timeout=kill_timeout, predicate=inner_predicate, stack=inner_stack)
            )
        )

    def launch_composition(inner_stack: Stack, comp: Composition) -> Future[None]:
        return executor.submit(
            lambda: accept_result(exec_composition(comp, predicate=inner_predicate, stack=inner_stack))
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
                stmt_stack.trace(f"waiting on {num_pending} statements")
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


def exec_shell(cmd: str, env: Dict[str, str], *, kill_timeout: float, predicate: FlagDelegate, stack: Stack) -> int:
    started_at = time.monotonic()
    ch = Child(
        cmd,
        env,
        line_handler_stdout=partial(print),
        line_handler_stderr=partial(print, file=sys.stderr),
    )
    prefix = f"{ch.pid:08d}: "
    try:
        stack.trace(f"shell pid={ch.pid:08d} env={env} cmd=\\\n{cmd}")
        longest_env = max(map(len, env.keys()))
        echo(
            "\n".join(
                [
                    style_env("\n".join(f"{prefix}{k.ljust(longest_env)} = {v!r}" for k, v in env.items())),
                    style_cmd("\n".join(f"{prefix}{ln}" for ln in cmd.splitlines())),
                ]
            ),
            err=True,
        )
        ret: Optional[int] = None
        while predicate() and ret is None:
            ret = ch.poll(_POLL_INTERVAL)
        if ret is None:
            stack.trace("stopping")
            echo(style_warn(f"{prefix}[stopping]"))
            ch.initiate_stopping_sequence(kill_timeout * 0.75, kill_timeout)
        while ret is None:
            ret = ch.poll(_POLL_INTERVAL)

        elapsed = time.monotonic() - started_at
        stack.trace(f"exit pid={ch.pid:08d} status={ret} elapsed={elapsed:.1f}")
        if ret != 0:
            echo(style_warn(f"{prefix}[exit status {ret}]"))
        return ret
    finally:
        ch.ensure_dead()  # Terminate process in the event of an exception.


class Stack:
    def __init__(self, path: Optional[List[str]] = None) -> None:
        self._path = path if path else []

    def push(self, node: Any) -> Stack:
        return Stack(self._path + [str(node)])

    def trace(self, text: str) -> None:
        _logger.info("At %s: %s", str(self), text)

    def __str__(self) -> str:
        return "/" + "/".join(self._path)


echo = partial(click.echo, err=True)
style_warn = partial(click.style, fg="yellow")
style_env = partial(click.style, fg="cyan")
style_cmd = partial(click.style, fg="magenta")
