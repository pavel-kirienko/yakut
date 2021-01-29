# Copyright (c) 2021 UAVCAN Consortium
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@uavcan.org>

from __future__ import annotations
import dataclasses
from typing import Dict, List, Any
from pathlib import Path
from yakut.yaml import YAMLLoader
from ._env import canonicalize_register, flatten_registers, NAME_SEP, EnvironmentVariableError


NOT_ENV = "="
"""
Equals sign is the only character that cannot occur in an environment variable name in most OS.
"""


class SchemaError(ValueError):
    pass


@dataclasses.dataclass(frozen=True)
class Composition:
    predicate: List[Statement]
    main: List[Statement]
    finalizer: List[Statement]

    env: Dict[str, str]

    kill_timeout: float = 20.0


@dataclasses.dataclass(frozen=True)
class Statement:
    pass


@dataclasses.dataclass(frozen=True)
class ShellStatement(Statement):
    cmd: str


@dataclasses.dataclass(frozen=True)
class CompositionStatement(Statement):
    comp: Composition


@dataclasses.dataclass(frozen=True)
class JoinStatement(Statement):
    pass


def parse(file: Path) -> Any:
    return YAMLLoader().load(Path(file).read_text())


def load_composition(ast: Any, env: Dict[str, str]) -> Composition:
    """
    Environment inheritance order (last entry takes precedence):

    - Parent process environment (i.e., the environment the orchestrator is invoked from).
    - Outer composition environment (e.g., root members of the orchestration file).
    - Extension orchestration files.
    - Local environment variables.
    """
    if not isinstance(ast, dict):
        raise SchemaError(f"The composition shall be a dict, not {type(ast).__name__}")

    imp = ast.pop("import=", None)
    if imp is not None:
        if not isinstance(imp, str):
            raise SchemaError(f"Invalid import specifier: {imp!r}")
        ext = load_composition(parse(Path(imp)), env.copy())
    else:
        ext = Composition(predicate=[], main=[], finalizer=[], env=env.copy())
    del env

    try:
        for name, value in flatten_registers(
            {k: v for k, v in ast.items() if isinstance(k, str) and NOT_ENV not in k}
        ).items():
            if NAME_SEP in name:  # UAVCAN register.
                name, value = canonicalize_register(name, value)
                name = name.upper().replace(NAME_SEP, "_" * 2)
            ext.env[name] = str(value)
    except EnvironmentVariableError as ex:
        raise SchemaError(f"Environment variable error: {ex}") from EnvironmentVariableError

    out = Composition(
        predicate=load_script(ast.pop("?=", []), ext.env) + ext.predicate,
        main=load_script(ast.pop("$=", []), ext.env) + ext.main,
        finalizer=load_script(ast.pop(".=", []), ext.env) + ext.finalizer,
        env=ext.env,
    )
    unattended = [k for k in ast if NOT_ENV in k]
    if unattended:
        raise SchemaError(f"Unknown directives: {unattended}")
    return out


def load_script(ast: Any, env: Dict[str, str]) -> List[Statement]:
    if isinstance(ast, list):
        return [load_statement(x, env) for x in ast]
    return [load_statement(ast, env)]


def load_statement(ast: Any, env: Dict[str, str]) -> Statement:
    if isinstance(ast, str):
        return ShellStatement(ast)
    if isinstance(ast, dict):
        return CompositionStatement(load_composition(ast, env))
    if ast is None:
        return JoinStatement()
    raise SchemaError("Statement shall be either: string (command to run), dict (nested schema), null (join)")
