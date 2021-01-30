# Copyright (c) 2021 UAVCAN Consortium
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@uavcan.org>

from __future__ import annotations
import sys
import signal
import logging
from typing import Any
import click
import yakut

from ._schema import Composition as Composition
from ._schema import load_ast as load_ast
from ._schema import load_composition as load_composition

from ._executor import Context as Context
from ._executor import Stack as Stack
from ._executor import ErrorCode as ErrorCode
from ._executor import exec_file as exec_file
from ._executor import exec_composition as exec_composition


_logger = logging.getLogger(__name__)


@yakut.subcommand()
@click.argument("orchestration_file", type=str)
@yakut.pass_purser
def orchestrate(purser: yakut.Purser, orchestration_file: str) -> None:
    """
    Execute an orchestration file.
    """
    sig_num = 0

    def on_signal(s: int, _: Any) -> None:
        nonlocal sig_num
        sig_num = s
        _logger.info("Orchestrator received signal %s %r, stopping...", s, signal.strsignal(s))

    signal.signal(signal.SIGINT, on_signal)
    signal.signal(signal.SIGTERM, on_signal)
    if sys.platform.startswith("win"):
        signal.signal(signal.SIGBREAK, on_signal)
    else:
        signal.signal(signal.SIGHUP, on_signal)

    ctx = Context(lookup_paths=purser.paths)
    res = exec_file(ctx, orchestration_file, {}, predicate=lambda: sig_num == 0)

    exit(res if res != 0 else -sig_num)
