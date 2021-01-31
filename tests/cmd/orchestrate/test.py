# Copyright (c) 2021 UAVCAN Consortium
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@uavcan.org>

from __future__ import annotations
import time
import json
from pathlib import Path
import pytest
from tests.subprocess import Subprocess, execute_cli


def _unittest_x() -> None:
    # Run to completion.
    proc = Subprocess.cli("-v", "orc", str((Path(__file__).parent / "x.orc.yaml").absolute()))
    exit_code, stdout, _stderr = proc.wait(timeout=10)
    assert exit_code == 88
    assert stdout.splitlines() == [
        "Hello world!",
        "bar",
        "finalizer",
    ]

    # Premature termination.
    proc = Subprocess.cli("-v", f"--path={Path(__file__).parent}", "orc", "x.orc.yaml")
    time.sleep(1.0)
    exit_code, stdout, _stderr = proc.wait(timeout=10, interrupt=True)
    assert exit_code not in (0, 88)
    assert stdout.splitlines() == [
        "Hello world!",
        "finalizer",
    ]


def _unittest_pub_sub() -> None:
    from yakut.paths import DEFAULT_PUBLIC_REGULATED_DATA_TYPES_ARCHIVE_URI

    _, stdout, _ = execute_cli(
        "orc",
        str((Path(__file__).parent / "pub_sub.orc.yaml").absolute()),
        environment_variables={"DSDL_SRC": DEFAULT_PUBLIC_REGULATED_DATA_TYPES_ARCHIVE_URI},
        timeout=120.0,
    )
    objects = list(map(json.loads, stdout.splitlines()))
    assert len(objects) == 2
    for ob in objects:
        assert ob == {
            "33": {
                "radian": pytest.approx(2.31),
            },
        }
