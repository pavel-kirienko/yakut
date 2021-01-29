# Copyright (c) 2021 UAVCAN Consortium
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@uavcan.org>

from __future__ import annotations
import os
import io
import sys
import time
import signal
from concurrent.futures import ThreadPoolExecutor, wait
from subprocess import Popen, PIPE, DEVNULL
from typing import Dict, Optional, Callable, List, Tuple
import logging


_logger = logging.getLogger(__name__)


class Child:
    _STREAM_BUFFER_SIZE = 1024 ** 2
    _STREAM_POLL_INTERVAL = 0.02
    _STREAM_FINALIZATION_DELAY = 1.0

    def __init__(
        self,
        cmd: str,
        env: Dict[str, str],
        line_handler_stdout: Callable[[str], None],
        line_handler_stderr: Callable[[str], None],
    ) -> None:
        self._return: Optional[int] = None
        self._signaling_schedule: List[Tuple[float, Callable[[], None]]] = []
        self._executor = ThreadPoolExecutor(max_workers=999)
        e = os.environ.copy()
        e.update(env)
        self._proc = Popen(
            cmd,
            env=e,
            shell=True,
            text=True,
            stdout=PIPE,
            stderr=PIPE,
            stdin=DEVNULL,
            bufsize=Child._STREAM_BUFFER_SIZE,
        )
        self._futures = {
            self._executor.submit(self._forward_line_by_line, self._proc.stdout, line_handler_stdout),
            self._executor.submit(self._forward_line_by_line, self._proc.stderr, line_handler_stderr),
        }

    @property
    def pid(self) -> int:
        return self._proc.pid

    def poll(self, timeout: float) -> Optional[int]:
        if self._futures:
            done, self._futures = wait(self._futures, timeout=0)
            for f in done:  # Propagate exceptions, if any, from the background tasks.
                f.result()

        if self._return is None:
            if self._signaling_schedule:
                deadline, handler = self._signaling_schedule[0]
                if time.monotonic() >= deadline:
                    self._signaling_schedule.pop(0)
                    handler()

            ret = self._proc.poll()
            if ret is None:
                time.sleep(timeout)
                ret = self._proc.poll()
            if ret is not None:
                self._return = ret

        return self._return

    def initiate_stopping_sequence(self, escalate_after: float, give_up_after: float) -> None:
        if self._return is not None or self._proc.poll() is not None:
            return
        give_up_after = max(give_up_after, escalate_after)
        _logger.debug(
            "%s: Interrupting. Escalation timeout: %.1f, give-up timeout: %.1f", self, escalate_after, give_up_after
        )
        # FIXME: on Windows, killing the shell process does not terminate its children.
        # TODO: use psutil to manually hunt down each child and kill them off one by one.
        self._proc.send_signal(signal.SIGBREAK if sys.platform.startswith("win") else signal.SIGINT)

        def terminate() -> None:
            _logger.warning("%s: Interrupt signal had no effect, trying to terminate", self)
            self._proc.send_signal(signal.SIGTERM)

        def kill() -> None:
            sig = signal.SIGABRT if sys.platform.startswith("win") else signal.SIGKILL
            if self._return is None:
                self._return = -sig
            _logger.error("%s: Termination signal had no effect, giving up by killing and abandoning the child", self)
            self._proc.send_signal(sig)

        now = time.monotonic()
        self._signaling_schedule = [
            (now + escalate_after, terminate),
            (now + give_up_after, kill),
        ]

    def ensure_dead(self) -> None:
        self._proc.kill()

    def _forward_line_by_line(self, stream: io.TextIOWrapper, line_handler: Callable[[str], None]) -> None:
        buf = io.StringIO()

        def once() -> None:
            nonlocal buf
            time.sleep(Child._STREAM_POLL_INTERVAL)
            for ch in stream.read():
                if ch == "\n":
                    line_handler(buf.getvalue())
                    buf = io.StringIO()
                else:
                    buf.write(ch)

        while self._return is None and self._proc.poll() is None:
            once()
        # Hang around for a short while to make sure that any delayed data is also forwarded.
        for _ in range(int(Child._STREAM_FINALIZATION_DELAY / Child._STREAM_POLL_INTERVAL)):
            once()
        # Flush unterminated line.
        if buf.getvalue():
            line_handler(buf.getvalue())

    def __str__(self) -> str:
        return f"Child {self.pid:08d}"


def _unittest_child(caplog: object) -> None:
    import pytest

    assert isinstance(caplog, pytest.LogCaptureFixture)

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
        for _ in range(50):
            res = c.poll(0.1)
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
