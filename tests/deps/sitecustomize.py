# Copyright (c) 2019 UAVCAN Consortium
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@uavcan.org>

import os
import sys
import pathlib

ROOT_DIR = pathlib.Path(__file__).resolve().parent.parent.parent
SETUP_CFG = ROOT_DIR / "setup.cfg"
assert SETUP_CFG.is_file()


def detect_debugger() -> bool:
    if sys.gettrace() is not None:
        return True
    if (os.path.sep + "pydev") in sys.argv[0]:
        return True
    return False


def setup_coverage() -> None:
    try:
        import coverage  # The module may be missing during early stage setup, no need to abort everything.
    except ImportError as ex:
        pass
    else:
        # Coverage configuration; see https://coverage.readthedocs.io/en/coverage-4.2/subprocess.html
        os.environ["COVERAGE_PROCESS_START"] = str(SETUP_CFG)
        coverage.process_startup()


if not detect_debugger():
    setup_coverage()
