# Copyright (c) 2021 UAVCAN Consortium
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@uavcan.org>

import sys
import pytest
import logging
from typing import List


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
