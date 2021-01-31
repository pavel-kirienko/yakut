# Copyright (c) 2021 UAVCAN Consortium
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@uavcan.org>

import sys
import time
from pathlib import Path
import pytest
from yakut.cmd.orchestrate import exec_composition, load_composition, Stack, Context, ErrorCode, exec_file, load_ast
from .... import ROOT_DIR


if sys.platform.startswith("win"):  # pragma: no cover
    pytest.skip("These are GNU/Linux-only tests", allow_module_level=True)


def _std_reset() -> None:
    if sys.stdout.seekable():
        sys.stdout.seek(0)
        sys.stdout.truncate(0)
    if sys.stderr.seekable():
        sys.stderr.seek(0)
        sys.stderr.truncate(0)


def _std_flush() -> None:
    sys.stdout.flush()
    sys.stderr.flush()


def _unittest_a(stdout_file: Path, stderr_file: Path) -> None:
    ast = load_ast((Path(__file__).parent / "a.orc.yaml").read_text())
    comp = load_composition(ast, {"C": "DEF", "D": "this variable will be unset"})
    print(comp)
    ctx = Context(lookup_paths=[])

    # Regular test, runs until completion.
    _std_reset()
    started_at = time.monotonic()
    assert 100 == exec_composition(ctx, comp, predicate=_true, stack=Stack())
    elapsed = time.monotonic() - started_at
    assert 10 <= elapsed <= 15, "Parallel execution is not handled correctly."
    _std_flush()
    assert stdout_file.read_text().splitlines() == [
        "100 abc DEF",
        "finalizer",
        "a.d.e: 1 2 3",
    ]
    assert "text value\n" in stderr_file.read_text()

    # Interrupted five seconds in.
    _std_reset()
    started_at = time.monotonic()
    assert 0 != exec_composition(ctx, comp, predicate=lambda: time.monotonic() - started_at < 5.0, stack=Stack())
    elapsed = time.monotonic() - started_at
    assert 5 <= elapsed <= 9, "Interruption is not handled correctly."
    _std_flush()
    assert stdout_file.read_text().splitlines() == [
        "100 abc DEF",
    ]
    assert "text value\n" in stderr_file.read_text()

    # Refers to a non-existent file.
    comp = load_composition(ast, {"CRASH": "1"})
    print(comp)
    assert ErrorCode.FILE_ERROR == exec_composition(ctx, comp, predicate=_true, stack=Stack())


def _unittest_b(stdout_file: Path) -> None:
    ctx = Context(lookup_paths=[ROOT_DIR, Path(__file__).parent])
    _std_reset()
    assert 0 == exec_file(ctx, "b.orc.yaml", env={"PROCEED_B": "1"}, predicate=_true)
    _std_flush()
    assert stdout_file.read_text().splitlines() == [
        "main b",
        "123",
        "456",
        "finalizer b",
        "finalizer b 1",
    ]

    _std_reset()
    assert 0 == exec_file(ctx, str((Path(__file__).parent / "b.orc.yaml").absolute()), env={}, predicate=_true)
    _std_flush()
    assert stdout_file.read_text().splitlines() == [
        "finalizer b",
    ]

    _std_reset()
    assert 0 == exec_file(ctx, "b.orc.yaml", env={"PLEASE_FAIL": "1"}, predicate=_true)
    _std_flush()
    assert stdout_file.read_text().splitlines() == [
        "finalizer b",
    ]

    _std_reset()
    assert 42 == exec_file(ctx, "b.orc.yaml", env={"PROCEED_B": "1", "PLEASE_FAIL": "1"}, predicate=_true)
    _std_flush()
    assert stdout_file.read_text().splitlines() == [
        "main b",
        "123",
        "456",
        "finalizer b",
        "finalizer b 1",
    ]

    ctx = Context(lookup_paths=[])
    assert ErrorCode.FILE_ERROR == exec_file(ctx, "b.orc.yaml", env={"PROCEED_B": "1"}, predicate=_true)
    ctx = Context(lookup_paths=[Path(__file__).parent])
    assert ErrorCode.FILE_ERROR == exec_file(ctx, "b.orc.yaml", env={"PROCEED_B": "1"}, predicate=_true)
    ctx = Context(lookup_paths=[])
    assert ErrorCode.FILE_ERROR == exec_file(ctx, "b.orc.yaml", env={}, predicate=_true)


def _true() -> bool:
    return True
