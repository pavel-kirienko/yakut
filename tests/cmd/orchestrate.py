# Copyright (c) 2021 UAVCAN Consortium
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@uavcan.org>

import sys
import time
import signal
import pytest
import logging
from typing import List


# language=YAML
_TEST_A = """
?=: echo $A $B $C
$=: '>&2 echo $A__B__C__STRING'
.=:
- sleep 10          # This...
- echo finalizer    # ...this...
- sleep 8           # ...and this are executed concurrently.
-                   # Join statement -- do not proceed until above are done.
- $=:               # Nested composition.
  - 'echo "a.d.e: $A__D__E__NATURAL8"'
  - ?=: []          # Empty nested composition, nothing to do.
-
- exit 123
A: 123
B: abc
a:
  b:
    c: text value
  d.e.natural8: [1, 2, 3]
"""


def _unittest_execute_composition_a(capsys: pytest.CaptureFixture) -> None:
    from yakut.cmd.orchestrate import exec_composition, parse_composition
    from yakut.yaml import YAMLLoader

    ast = YAMLLoader().load(_TEST_A)
    comp = parse_composition(ast, {"C": "DEF"})
    print(comp)
    _ = capsys.readouterr()

    # Regular test, runs until completion.
    started_at = time.monotonic()
    assert 123 == exec_composition("", comp, predicate=lambda: True)
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
    assert -signal.SIGINT == exec_composition("", comp, predicate=lambda: time.monotonic() - started_at < 5.0)
    elapsed = time.monotonic() - started_at
    assert 5 <= elapsed <= 9, "Interruption is not handled correctly."
    cap = capsys.readouterr()
    assert cap.out.splitlines() == [
        "123 abc DEF",
        "finalizer",
    ]
    assert "\ntext value\n" in cap.err


def _unittest_child(caplog: pytest.LogCaptureFixture) -> None:
    from yakut.cmd.orchestrate import Child

    ln_out: List[str] = []
    ln_err: List[str] = []

    if sys.platform.startswith("win"):
        py = (
            "import time, signal as s; "
            + "s.signal(s.SIGBREAK, lambda *_: None); "
            + "s.signal(s.SIGTERM, lambda *_: None); "
            + "time.sleep(10)"
        )
    else:
        py = (
            "import time, signal as s; "
            + "s.signal(s.SIGINT, lambda *_: None); "
            + "s.signal(s.SIGTERM, lambda *_: None); "
            + "time.sleep(10)"
        )

    with caplog.at_level(logging.CRITICAL):
        c = Child(f"python -c '{py}'", {}, line_handler_stdout=ln_out.append, line_handler_stderr=ln_err.append)
        assert c.poll(0.1) is None
        c.initiate_stopping_sequence(1.0, 2.0)
        assert c.poll(1.0) is None
        res = c.poll(5.0)
        assert res is not None
        assert res < 0  # Killed
    assert not ln_out
    assert not ln_err

    c = Child(
        f"python -c 'import time; time.sleep(10)'",
        {},
        line_handler_stdout=ln_out.append,
        line_handler_stderr=ln_err.append,
    )
    assert c.poll(0.1) is None
    c.ensure_dead()
    res = c.poll(1.0)
    assert res is not None
    assert res < 0  # Killed
    assert not ln_out
    assert not ln_err

    c = Child(
        """python -c 'import sys; print("ABC", file=sys.stderr); print("DEF"); print("GHI", end="")'""",
        {},
        line_handler_stdout=ln_out.append,
        line_handler_stderr=ln_err.append,
    )
    assert 0 == c.poll(1.0)
    assert ln_out == ["DEF", "GHI"]
    assert ln_err == ["ABC"]
