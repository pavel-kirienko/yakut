# Copyright (c) 2021 UAVCAN Consortium
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@uavcan.org>

import time
import signal
import pytest


# language=YAML
_TEST_A = """
?=: echo $A $B $C
$=:
- '>&2 echo $A__B__C__STRING'
- sleep 10
- sleep 8
-
- echo finalizer
- $=:
  - 'echo "a.d.e: $A__D__E__NATURAL8"'
  - ?=: []
.=: exit 123
A: 123
B: abc
a:
  b:
    c: text value
  d.e.natural8: [1, 2, 3]
"""


def _unittest_execute_composition_a(capsys: pytest.CaptureFixture) -> None:
    from yakut.cmd.orchestrate import exec_composition, load_composition, Stack
    from yakut.yaml import YAMLLoader

    ast = YAMLLoader().load(_TEST_A)
    comp = load_composition(ast, {"C": "DEF"})
    print(comp)
    _ = capsys.readouterr()

    # Regular test, runs until completion.
    started_at = time.monotonic()
    assert 123 == exec_composition(comp, predicate=lambda: True, stack=Stack())
    elapsed = time.monotonic() - started_at
    assert 10 <= elapsed <= 15, "Parallel execution is not handled correctly."
    cap = capsys.readouterr()
    assert cap.out.splitlines() == [
        "123 abc DEF",
        "finalizer",
        "a.d.e: 1 2 3",
    ]
    assert "\ntext value\n" in cap.err

    # Interrupted five seconds in.
    started_at = time.monotonic()
    print("started_at:", started_at)
    assert 123 == exec_composition(comp, predicate=lambda: time.monotonic() - started_at < 5.0, stack=Stack())
    elapsed = time.monotonic() - started_at
    assert 5 <= elapsed <= 9, "Interruption is not handled correctly."
    cap = capsys.readouterr()
    assert cap.out.splitlines() == [
        "123 abc DEF",
        "finalizer",
    ]
    assert "\ntext value\n" in cap.err
