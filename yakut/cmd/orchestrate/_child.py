# Copyright (c) 2021 UAVCAN Consortium
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@uavcan.org>

from __future__ import annotations
import os
import sys
import time
import signal
from subprocess import Popen, DEVNULL
from typing import Dict, Optional, Callable, List, Tuple, BinaryIO
import logging


_logger = logging.getLogger(__name__)


if sys.platform.startswith("win"):  # pragma: no cover
    SIGNAL_INTERRUPT = signal.SIGBREAK
    SIGNAL_TERMINATE = signal.SIGTERM
    SIGNAL_KILL = signal.SIGABRT
else:
    SIGNAL_INTERRUPT = signal.SIGINT
    SIGNAL_TERMINATE = signal.SIGTERM
    SIGNAL_KILL = signal.SIGKILL


class Child:
    """
    Starts a child shell process and provides convenient non-blocking controls:

    - :meth:`poll` for querying the status.
    - :meth:`stop` for stopping with automatic escalation of the signal strength.
    - The constructor accepts file objects where the child process output is sent to.

    When running on Windows, it may be desirable to use PowerShell as the system shell.
    For that, export the COMSPEC environment variable.
    """

    def __init__(self, cmd: str, env: Dict[str, str], *, stdout: BinaryIO, stderr: BinaryIO) -> None:
        """
        :param cmd: Shell command to execute. Execution starts immediately.
        :param env: Additional environment variables.
        :param stdout: Stdout from the child process is redirected there.
        :param stderr: Ditto, but for stderr.
        """
        self._return: Optional[int] = None
        self._signaling_schedule: List[Tuple[float, Callable[[], None]]] = []
        e = os.environ.copy()
        e.update(env)
        self._proc = Popen(cmd, env=e, shell=True, stdout=stdout, stderr=stderr, stdin=DEVNULL, bufsize=1)

    @property
    def pid(self) -> int:
        """
        The process-ID of the child. This value retains validity even after the child is terminated.
        """
        return self._proc.pid

    def poll(self, timeout: float) -> Optional[int]:
        """
        :param timeout: Block for this many seconds, at most.
        :return: None if still running, exit code if finished (idempotent).
        """
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

    def stop(self, escalate_after: float, give_up_after: float) -> None:
        """
        Send a SIGINT/SIGBREAK to the process and schedule to check if it's dead later.

        :param escalate_after: If the process is still alive this many seconds after the initial termination signal,
            send a SIGTERM.

        :param give_up_after: Ditto, but instead of SIGTERM send SIGKILL (on Windows use SIGABRT instead)
            and disown the child immediately without waiting around. This is logged as error.
        """
        if self._return is not None or self._proc.poll() is not None:
            return
        give_up_after = max(give_up_after, escalate_after)
        _logger.debug(
            "%s: Stopping using signal %r. Escalation timeout: %.1f, give-up timeout: %.1f",
            self,
            signal.strsignal(SIGNAL_INTERRUPT),
            escalate_after,
            give_up_after,
        )
        # FIXME: on Windows, killing the shell process does not terminate its children.
        # TODO: use psutil to manually hunt down each child and kill them off one by one.
        self._proc.send_signal(SIGNAL_INTERRUPT)

        def terminate() -> None:
            _logger.warning("%s: The child is still alive. Escalating to %r", self, signal.strsignal(SIGNAL_TERMINATE))
            self._proc.send_signal(SIGNAL_TERMINATE)

        def kill() -> None:
            _logger.error(
                "%s: The child is still alive. Escalating to %r and detaching. No further attempts will be made!",
                self,
                signal.strsignal(SIGNAL_KILL),
            )
            self.kill()

        now = time.monotonic()
        self._signaling_schedule = [
            (now + escalate_after, terminate),
            (now + give_up_after, kill),
        ]

    def kill(self) -> None:
        """
        This is intended for abnormal termination of the owner of this instance.
        Simply kills the child and ceases all related activities.
        """
        if self._return is None:
            self._return = -SIGNAL_KILL
        self._proc.send_signal(SIGNAL_KILL)

    def __str__(self) -> str:
        return f"Child {self.pid:08d}"


def _unittest_child(caplog: object) -> None:
    import pytest
    from pathlib import Path

    assert isinstance(caplog, pytest.LogCaptureFixture)

    io_out = Path("stdout")
    io_err = Path("stderr")

    if sys.platform.startswith("win"):  # pragma: no cover
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
        c = Child(f"python -c '{py}'", {}, stdout=io_out.open("wb"), stderr=io_err.open("wb"))
        assert c.poll(0.1) is None
        c.stop(1.0, 2.0)
        assert c.poll(1.0) is None
        for _ in range(50):
            res = c.poll(0.1)
        assert res is not None
        assert res < 0  # Killed
        c.stop(1.0, 2.0)  # No effect because already dead.
    assert not io_out.read_text()
    assert not io_err.read_text()

    c = Child(f"python -c 'import time; time.sleep(10)'", {}, stdout=io_out.open("wb"), stderr=io_err.open("wb"))
    assert c.poll(0.1) is None
    c.kill()
    res = c.poll(1.0)
    assert res is not None
    assert res < 0  # Killed
    assert not io_out.read_text()
    assert not io_err.read_text()

    c = Child(
        """python -c 'import sys; print("ABC", file=sys.stderr); print("DEF"); print("GHI", end="")'""",
        {},
        stdout=io_out.open("wb"),
        stderr=io_err.open("wb"),
    )
    assert 0 == c.poll(1.0)
    time.sleep(2.0)
    assert io_out.read_text().splitlines() == ["DEF", "GHI"]
    assert io_err.read_text().splitlines() == ["ABC"]
