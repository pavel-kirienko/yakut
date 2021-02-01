# Copyright (c) 2020 UAVCAN Consortium
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@uavcan.org>

# pylint: disable=unused-import
import sys
import tempfile
from typing import Iterator
from pathlib import Path
import pytest
from .dsdl import compiled_dsdl as compiled_dsdl
from .transport import transport_factory as transport_factory


@pytest.fixture()
def stdout_file() -> Iterator[Path]:
    """
    Replaces :attr:`sys.stdout` with a regular binary file on disk. The value of the fixture is the path to the file.
    The original stream is restored afterwards automatically.
    This is intended as an alternative to the standard PyTest caplog fixture where a real fd is required
    (the in-memory stub provided by PyTest is unusable if the tested code expects the streams to have real descriptors).
    """
    s = sys.stdout
    p = Path(tempfile.mktemp("stdout")).resolve()
    sys.stdout = p.open("a+")
    yield p
    sys.stdout = s


@pytest.fixture()
def stderr_file() -> Iterator[Path]:
    """
    Like :func:`stdout_file` but for stderr.
    """
    s = sys.stderr
    p = Path(tempfile.mktemp("stderr")).resolve()
    sys.stderr = p.open("a+")
    yield p
    sys.stderr = s
