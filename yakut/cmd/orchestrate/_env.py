# Copyright (c) 2021 UAVCAN Consortium
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@uavcan.org>

from __future__ import annotations
from functools import lru_cache
from typing import Dict, List, Tuple, Any


NAME_SEP = "."
ITEM_SEP = " "


class EnvironmentVariableError(ValueError):
    pass


@lru_cache(None)
def _get_register_value_option_names() -> List[str]:
    from uavcan.register import Value_1_0 as Value

    return [x for x in dir(Value) if not x.startswith("_")]


def canonicalize_register(name: str, value: Any) -> Tuple[str, str]:
    """
    Ensures that the name has the correct type suffux and converts the value to string.

    >>> canonicalize_register('foo.empty', ['this', 'is', 'ignored'])
    ('foo.empty', '')
    >>> canonicalize_register('foo.string', 123)
    ('foo.string', '123')
    >>> canonicalize_register('foo', 'hello')  # Auto-detect.
    ('foo.string', 'hello')
    >>> canonicalize_register('foo', b'hello')
    ('foo.unstructured', '68656c6c6f')
    >>> canonicalize_register('foo.unstructured', '68656c6c6f')  # Same, just different notation.
    ('foo.unstructured', '68656c6c6f')
    >>> canonicalize_register('foo', [True, False, True])
    ('foo.bit', '1 0 1')
    >>> canonicalize_register('foo', [60_000, 50_000])
    ('foo.natural16', '60000 50000')
    >>> canonicalize_register('foo', 300_000)
    ('foo.natural32', '300000')
    >>> canonicalize_register('foo', [2 ** 32, 0])
    ('foo.natural64', '4294967296 0')
    >>> canonicalize_register('foo', -10_000)
    ('foo.integer16', '-10000')
    >>> canonicalize_register('foo', [-10_000, 40_000])
    ('foo.integer32', '-10000 40000')
    >>> canonicalize_register('foo', [-(2 ** 31), 2 ** 31])
    ('foo.integer64', '-2147483648 2147483648')
    >>> canonicalize_register('foo', 1.0)
    ('foo.real64', '1.0')
    >>> canonicalize_register('foo', [1, 'a'])  # doctest: +IGNORE_EXCEPTION_DETAIL
    Traceback (most recent call last):
    ...
    EnvironmentVariableError: ...
    """
    for val_opt_name in _get_register_value_option_names():
        suffix = NAME_SEP + val_opt_name
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
                return name, ITEM_SEP.join(("1" if x else "0") for x in value)
            if val_opt_name.startswith("integer") or val_opt_name.startswith("natural"):
                return name, ITEM_SEP.join(str(int(x)) for x in value)
            if val_opt_name.startswith("real"):
                return name, ITEM_SEP.join(str(float(x)) for x in value)
            assert False, f"Internal error: unhandled value option: {val_opt_name}"

    def convert(ty: str) -> Tuple[str, str]:
        assert ty in _get_register_value_option_names()
        return canonicalize_register(name + NAME_SEP + ty, value)

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


def flatten_registers(spec: Dict[str, Any], prefix: str = "") -> Dict[str, Any]:
    """
    >>> flatten_registers({"FOO": "BAR", "a": {"b": 123, "c": [456, 789]}})  # doctest: +NORMALIZE_WHITESPACE
    {'FOO': 'BAR',
     'a.b': 123,
     'a.c': [456, 789]}
    """
    out: Dict[str, Any] = {}
    for k, v in spec.items():
        name = NAME_SEP.join((prefix, k)) if prefix else k
        if isinstance(v, dict):
            out.update(flatten_registers(v, name))
        else:
            out[name] = v
    return out
