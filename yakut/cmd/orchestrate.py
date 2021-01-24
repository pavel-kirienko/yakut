# Copyright (c) 2021 UAVCAN Consortium
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@uavcan.org>

from __future__ import annotations
import os
import dataclasses
from concurrent.futures import Future
from typing import Dict, List, Any, Tuple, Union, Optional
from functools import lru_cache
from pathlib import Path
import logging
import click
import yakut
from yakut.yaml import YAMLLoader, YAMLDumper


_logger = logging.getLogger(__name__)

_NAME_SEP = "."
"""
From the UAVCAN Specification.
"""

_NOT_ENV = "="
"""
Equals sign is the only character that cannot occur in an environment variable name in most OS.
"""


class SchemaError(ValueError):
    pass


class EnvironmentVariableError(SchemaError):
    pass


@yakut.subcommand()
@click.argument("yaml", type=click.Path(exists=True, dir_okay=False, resolve_path=True, allow_dash=True, path_type=str))
def orchestrate(yaml: str) -> None:
    """
    Execute an orchestration file.
    """
    _logger.debug("Orchestrating %r", yaml)
    exit(_execute_file(Path(yaml).resolve(), {}))


def _execute_file(yaml_file: Path, env: Dict[str, str]) -> Future:
    frac = _parse(_load_yaml(yaml_file), env)
    if _logger.isEnabledFor(logging.DEBUG):
        _logger.debug("Parsed file %s with env keys %s:\n%s", yaml_file, list(env), YAMLDumper().dumps(frac))
    return _execute_fractal(frac)


def _execute_fractal(sch: Fractal) -> Future:
    pass


def _execute_shell(expression: str, env: Dict[str, str]) -> Future:
    pass


@dataclasses.dataclass(frozen=True)
class Fractal:
    opt: Script
    seq: Script
    par: Script
    end: Script
    env: Dict[str, str]


Script = List[Union[str, Fractal]]


def _load_yaml(yaml_file: Path) -> Dict[Any, Any]:
    with open(yaml_file.resolve(), "r", encoding="utf8") as f:
        return YAMLLoader().load(f.read())


def _parse(ast: Dict[Any, Any], env: Dict[str, str]) -> Fractal:
    """
    Environment inheritance order (last entry takes precedence):

    - Parent process environment (i.e., the environment the orchestrator is invoked from).
    - Outer fractal environment (e.g., root members of the orchestration file).
    - Extension orchestration files.
    - Local environment variables.
    """
    if not isinstance(ast, dict):
        raise SchemaError(f"The outer schema object shall be a dict, not {type(ast).__name__}")

    extend = ast.pop("=extend", None)
    if extend is not None:
        if not isinstance(extend, str):
            raise SchemaError(f"Invalid extend specifier: {extend!r}")
        ext = _parse(_load_yaml(Path(extend)), env.copy())
    else:
        ext = Fractal(opt=[], seq=[], par=[], end=[], env=env.copy())
    del env

    for name, value in _flatten_registers({k: v for k, v in ast.items() if isinstance(k, str) and _NOT_ENV not in k}):
        if _NAME_SEP in name:  # UAVCAN register.
            name, value = _canonicalize_register(name, value)
            name = name.upper().replace(_NAME_SEP, "_" * 2)
        ext.env[name] = str(value)

    out = Fractal(
        opt=_parse_script(ast.pop("=optional", []), ext.env) + ext.opt,
        seq=_parse_script(ast.pop("=sequential", []), ext.env) + ext.seq,
        par=_parse_script(ast.pop("=concurrent", []), ext.env) + ext.par,
        end=_parse_script(ast.pop("=stop", []), ext.env) + ext.end,
        env=ext.env,
    )
    unattended = [k for k in ast if _NOT_ENV in k]
    if unattended:
        raise SchemaError(f"Unknown directives: {unattended}")
    return out


def _parse_script(ast: Any, env: Dict[str, str]) -> Script:
    if isinstance(ast, list):
        return [_parse_script_item(x, env) for x in ast]
    return [_parse_script_item(ast, env)]


def _parse_script_item(ast: Any, env: Dict[str, str]) -> Union[str, Fractal]:
    if isinstance(ast, str):
        return ast
    if isinstance(ast, dict):
        return _parse(ast, env)
    raise SchemaError("Script item shall be either: string (command to run), dict (nested schema)")


@lru_cache(None)
def _get_register_value_option_names() -> List[str]:
    from uavcan.register import Value_1_0 as Value

    return [x for x in dir(Value) if not x.startswith("_")]


def _canonicalize_register(name: str, value: Any) -> Tuple[str, str]:
    """
    Ensures that the name has the correct type suffux and converts the value to string.

    >>> import sys, pytest
    >>> if sys.platform.startswith('win'): pytest.skip('Skipped because it depends on os.pathsep')
    >>> _canonicalize_register('foo.empty', ['this', 'is', 'ignored'])
    ('foo.empty', '')
    >>> _canonicalize_register('foo.string', 123)
    ('foo.string', '123')
    >>> _canonicalize_register('foo', 'hello')  # Auto-detect.
    ('foo.string', 'hello')
    >>> _canonicalize_register('foo', b'hello')
    ('foo.unstructured', '68656c6c6f')
    >>> _canonicalize_register('foo.unstructured', '68656c6c6f')  # Same, just different notation.
    ('foo.unstructured', '68656c6c6f')
    >>> _canonicalize_register('foo', [True, False, True])
    ('foo.bit', '1:0:1')
    >>> _canonicalize_register('foo', [60_000, 50_000])
    ('foo.natural16', '60000:50000')
    >>> _canonicalize_register('foo', 300_000)
    ('foo.natural32', '300000')
    >>> _canonicalize_register('foo', [2 ** 32, 0])
    ('foo.natural64', '4294967296:0')
    >>> _canonicalize_register('foo', -10_000)
    ('foo.integer16', '-10000')
    >>> _canonicalize_register('foo', [-10_000, 40_000])
    ('foo.integer32', '-10000:40000')
    >>> _canonicalize_register('foo', [-(2 ** 31), 2 ** 31])
    ('foo.integer64', '-2147483648:2147483648')
    >>> _canonicalize_register('foo', 1.0)
    ('foo.real64', '1.0')
    >>> _canonicalize_register('foo', [1, 'a'])  # doctest: +IGNORE_EXCEPTION_DETAIL
    Traceback (most recent call last):
    ...
    EnvironmentVariableError: ...
    """
    for val_opt_name in _get_register_value_option_names():
        suffix = _NAME_SEP + val_opt_name
        if name.endswith(suffix):
            if val_opt_name == "empty":
                return name, ""
            if val_opt_name == "string":
                return name, str(value)
            if val_opt_name == "unstructured":
                try:
                    if not isinstance(value, bytes):
                        value = bytes.fromhex(str(value))
                except ValueError:
                    raise EnvironmentVariableError(f"{name!r}: expected bytes or hex-encoded string") from None
                return name, value.hex()
            # All other values are vector values. Converge scalars to one-dimensional vectors:
            try:
                value = list(value)
            except TypeError:
                value = [value]
            if val_opt_name == "bit":
                return name, os.pathsep.join(("1" if x else "0") for x in value)
            if val_opt_name.startswith("integer") or val_opt_name.startswith("natural"):
                return name, os.pathsep.join(str(int(x)) for x in value)
            if val_opt_name.startswith("real"):
                return name, os.pathsep.join(str(float(x)) for x in value)
            assert False, f"Internal error: unhandled value option: {val_opt_name}"

    def convert(ty: str) -> Tuple[str, str]:
        assert ty in _get_register_value_option_names()
        return _canonicalize_register(name + _NAME_SEP + ty, value)

    # Type not specified. Perform auto-detection.
    if isinstance(value, str):
        return convert("string")
    if isinstance(value, bytes):
        return convert("unstructured")
    # All other values are vector values. Converge scalars to one-dimensional vectors:
    try:
        value = list(value)
    except TypeError:
        value = [value]
    if all(isinstance(x, bool) for x in value):
        return convert("bit")
    if all(isinstance(x, int) for x in value):
        # fmt: off
        if all(0          <= x < (2 ** 16) for x in value): return convert("natural16")
        if all(0          <= x < (2 ** 32) for x in value): return convert("natural32")
        if all(0          <= x < (2 ** 64) for x in value): return convert("natural64")
        if all(-(2 ** 15) <= x < (2 ** 15) for x in value): return convert("integer16")
        if all(-(2 ** 31) <= x < (2 ** 31) for x in value): return convert("integer32")
        if all(-(2 ** 63) <= x < (2 ** 63) for x in value): return convert("integer64")
        # fmt: on
    if all(isinstance(x, (int, float)) for x in value):
        return convert("real64")

    raise EnvironmentVariableError(f"Cannot infer the type of {name!r}")


def _flatten_registers(spec: Dict[str, Any], prefix: str = "") -> Dict[str, Any]:
    """
    >>> _flatten_registers({"FOO": "BAR", "a": {"b": 123, "c": [456, 789]}})  # doctest: +NORMALIZE_WHITESPACE
    {'FOO': 'BAR',
     'a.b': 123,
     'a.c': [456, 789]}
    """
    out: Dict[str, Any] = {}
    for k, v in spec.items():
        name = _NAME_SEP.join((prefix, k)) if prefix else k
        if isinstance(v, dict):
            out.update(_flatten_registers(v, name))
        else:
            out[name] = v
    return out
