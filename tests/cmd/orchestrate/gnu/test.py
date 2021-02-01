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
    _ = stdout_file, stderr_file

    ast = load_ast((Path(__file__).parent / "a.orc.yaml").read_text())
    comp = load_composition(ast, {"C": "DEF", "D": "this variable will be unset"})
    print(comp)
    ctx = Context(lookup_paths=[])

    # Regular test, runs until completion.
    _std_reset()
    started_at = time.monotonic()
    assert 100 == exec_composition(ctx, comp, gate=_true, stack=Stack())
    elapsed = time.monotonic() - started_at
    assert 10 <= elapsed <= 15, "Parallel execution is not handled correctly."
    _std_flush()
    sys.stdout.seek(0)
    assert sys.stdout.read().splitlines() == [
        "100 abc DEF",
        "finalizer",
        "a.d.e: 1 2 3",
    ]
    sys.stderr.seek(0)
    assert "text value\n" in sys.stderr.read()

    # Interrupted five seconds in.
    _std_reset()
    started_at = time.monotonic()
    assert 0 != exec_composition(ctx, comp, gate=lambda: time.monotonic() - started_at < 5.0, stack=Stack())
    elapsed = time.monotonic() - started_at
    assert 5 <= elapsed <= 9, "Interruption is not handled correctly."
    _std_flush()
    sys.stdout.seek(0)
    assert sys.stdout.read().splitlines() == [
        "100 abc DEF",
    ]
    sys.stderr.seek(0)
    assert "text value\n" in sys.stderr.read()

    # Refers to a non-existent file.
    comp = load_composition(ast, {"CRASH": "1"})
    print(comp)
    assert ErrorCode.FILE_ERROR == exec_composition(ctx, comp, gate=_true, stack=Stack())


def _unittest_b(stdout_file: Path) -> None:
    _ = stdout_file

    ctx = Context(lookup_paths=[ROOT_DIR, Path(__file__).parent])
    _std_reset()
    env = {"PROCEED_B": "1"}
    assert 0 == exec_file(ctx, "b.orc.yaml", env, gate=_true)
    _std_flush()
    sys.stdout.seek(0)
    assert sys.stdout.read().splitlines() == [
        "main b",
        "123",
        "456",
        "finalizer b",
        "finalizer b 1",
    ]
    assert env == {
        "PROCEED_B": "1",
        "FOO": "123",
        "BAR": "123",
    }

    _std_reset()
    env = {}
    assert 0 == exec_file(ctx, str((Path(__file__).parent / "b.orc.yaml").absolute()), env, gate=_true)
    _std_flush()
    sys.stdout.seek(0)
    assert sys.stdout.read().splitlines() == [
        "finalizer b",
    ]
    assert env == {
        "FOO": "123",
        "BAR": "123",
    }

    _std_reset()
    env = {"PLEASE_FAIL": "1"}
    assert 0 == exec_file(ctx, "b.orc.yaml", env, gate=_true)
    _std_flush()
    sys.stdout.seek(0)
    assert sys.stdout.read().splitlines() == [
        "finalizer b",
    ]
    assert env == {
        "PLEASE_FAIL": "1",
        "FOO": "123",
        "BAR": "123",
    }

    _std_reset()
    env = {"PROCEED_B": "1", "PLEASE_FAIL": "1"}
    assert 42 == exec_file(ctx, "b.orc.yaml", env, gate=_true)
    _std_flush()
    sys.stdout.seek(0)
    assert sys.stdout.read().splitlines() == [
        "main b",
        "123",
        "456",
        "finalizer b",
        "finalizer b 1",
    ]
    assert env == {
        "PROCEED_B": "1",
        "PLEASE_FAIL": "1",
        "FOO": "123",
        "BAR": "123",
    }

    ctx = Context(lookup_paths=[])
    assert ErrorCode.FILE_ERROR == exec_file(ctx, "b.orc.yaml", {"PROCEED_B": "1"}, gate=_true)
    ctx = Context(lookup_paths=[Path(__file__).parent])
    assert ErrorCode.FILE_ERROR == exec_file(ctx, "b.orc.yaml", {"PROCEED_B": "1"}, gate=_true)
    ctx = Context(lookup_paths=[])
    assert ErrorCode.FILE_ERROR == exec_file(ctx, "b.orc.yaml", {}, gate=_true)


def _true() -> bool:
    return True
