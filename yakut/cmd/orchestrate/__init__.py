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

from ._schema import SchemaError as SchemaError
from ._schema import Composition as Composition
from ._schema import load_composition as load_composition
from ._schema import parse as parse

from ._executor import Stack as Stack
from ._executor import exec_file as exec_file
from ._executor import exec_composition as exec_composition


SCHEMA_ERROR_CODE = 125
"""
125 is the maximum exit code that can be safely used in a POSIX system. It is used for reporting invalid spec files.
"""


_logger = logging.getLogger(__name__)


@yakut.subcommand()
@click.argument("orchestration_file", type=str)
def orchestrate(orchestration_file: str) -> None:
    """
    Execute an orchestration file.

    The finalization section is intentionally shielded from all signals.
    """
    sig_num = 0

    def on_signal(s: int, _: Any) -> None:
        nonlocal sig_num
        sig_num = s
        _logger.info("Orchestrator received signal %s %r, stopping", s, signal.strsignal(s))

    signal.signal(signal.SIGINT, on_signal)
    signal.signal(signal.SIGTERM, on_signal)
    if sys.platform.startswith("win"):
        signal.signal(signal.SIGBREAK, on_signal)
    else:
        signal.signal(signal.SIGHUP, on_signal)

    try:
        res = exec_file("", orchestration_file, {}, predicate=lambda: sig_num == 0)
    except SchemaError as ex:
        res = SCHEMA_ERROR_CODE
        _logger.exception(f"Orchestration file schema error: {ex}")

    exit(res if res != 0 else -sig_num)
