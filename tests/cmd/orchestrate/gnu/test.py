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


def _unittest_a(capsys: pytest.CaptureFixture[str]) -> None:
    ast = load_ast((Path(__file__).parent / "a.orc.yaml").read_text())
    comp = load_composition(ast, {"C": "DEF", "D": "this variable will be unset"})
    print(comp)
    ctx = Context(lookup_paths=[])

    # Regular test, runs until completion.
    _ = capsys.readouterr()  # Drop the capture buffer.
    started_at = time.monotonic()
    assert 100 == exec_composition(ctx, comp, predicate=_true, stack=Stack())
    elapsed = time.monotonic() - started_at
    assert 10 <= elapsed <= 15, "Parallel execution is not handled correctly."
    cap = capsys.readouterr()
    assert cap.out.splitlines() == [
        "100 abc DEF",
        "finalizer",
        "a.d.e: 1 2 3",
    ]
    assert "text value\n" in cap.err

    # Interrupted five seconds in.
    _ = capsys.readouterr()  # Drop the capture buffer.
    started_at = time.monotonic()
    assert 0 != exec_composition(ctx, comp, predicate=lambda: time.monotonic() - started_at < 5.0, stack=Stack())
    elapsed = time.monotonic() - started_at
    assert 5 <= elapsed <= 9, "Interruption is not handled correctly."
    cap = capsys.readouterr()
    assert cap.out.splitlines() == [
        "100 abc DEF",
    ]
    assert "text value\n" in cap.err

    # Refers to an non-existent file.
    comp = load_composition(ast, {"CRASH": "1"})
    print(comp)
    assert ErrorCode.FILE_ERROR == exec_composition(ctx, comp, predicate=_true, stack=Stack())


def _unittest_b(capsys: pytest.CaptureFixture[str]) -> None:
    ctx = Context(lookup_paths=[ROOT_DIR, Path(__file__).parent])
    _ = capsys.readouterr()
    assert 0 == exec_file(ctx, "b.orc.yaml", env={"PROCEED_B": "1"}, predicate=_true)
    cap = capsys.readouterr()
    assert cap.out.splitlines() == [
        "main b",
        "123",
        "456",
        "finalizer b",
        "finalizer b 1",
    ]

    _ = capsys.readouterr()
    assert 0 == exec_file(ctx, str((Path(__file__).parent / "b.orc.yaml").absolute()), env={}, predicate=_true)
    cap = capsys.readouterr()
    assert cap.out.splitlines() == [
        "finalizer b",
    ]

    _ = capsys.readouterr()
    assert 0 == exec_file(ctx, "b.orc.yaml", env={"PLEASE_FAIL": "1"}, predicate=_true)
    cap = capsys.readouterr()
    assert cap.out.splitlines() == [
        "finalizer b",
    ]

    _ = capsys.readouterr()
    assert 42 == exec_file(ctx, "b.orc.yaml", env={"PROCEED_B": "1", "PLEASE_FAIL": "1"}, predicate=_true)
    cap = capsys.readouterr()
    assert cap.out.splitlines() == [
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
