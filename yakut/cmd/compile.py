# Copyright (c) 2019 UAVCAN Consortium
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@uavcan.org>

import http
import shutil
import typing
import logging
import zipfile
import tempfile
from pathlib import Path
import pyuavcan
import click
import yakut
from yakut.paths import DEFAULT_PUBLIC_REGULATED_DATA_TYPES_ARCHIVE_URI


_NAME = "compile"

_logger = logging.getLogger(__name__)


def make_usage_suggestion(root_namespace_name: typing.Optional[str]) -> str:
    """
    When a command is unable to find a compiled DSDL package, this helper can be used to construct a
    human-friendly suggestion on how to resolve the problem.
    """
    root_namespace_name = root_namespace_name or "<namespace>"
    root_namespace_name = root_namespace_name.split(".")[0]  # Transform: `uavcan.node` --> `uavcan`
    return f"Run `yakut {_NAME} <path>/{root_namespace_name}` to compile DSDL namespace {root_namespace_name!r}"


@yakut.subcommand(
    name=_NAME,
    help=f"""
Compile DSDL namespaces for use by Yakut.
This needs to be done before using any data types with pub/sub/call and other commands.

The command accepts a list of sources where each element is either a local path
or an URI pointing to the source DSDL root namespace(s).

If a source is a local path, it must point to a local DSDL root namespace directory or to a local archive containing
DSDL root namespace directories at the top level.
If the value is an URI, it must point to an archive containing DSDL root namespace directories at the top level
(this is convenient for generating packages from namespaces hosted in public repositories, e.g., on GitHub).

See also: top-level option `--path` and related environment variable `YAKUT_PATH`.

Example path: ~/uavcan/public_regulated_data_types/uavcan/

Example URI: {DEFAULT_PUBLIC_REGULATED_DATA_TYPES_ARCHIVE_URI}

Example command that compiles the root namespace `~/namespace` which depends on the public regulated types:

\b
    yakut {_NAME}  ~/namespace  --lookup {DEFAULT_PUBLIC_REGULATED_DATA_TYPES_ARCHIVE_URI}
""",
)
@click.argument("source", nargs=-1, required=True, type=str)
@click.option(
    "--lookup",
    "-L",
    multiple=True,
    type=str,
    metavar="SOURCE",
    help=f"""
This is like the sources except that the specified DSDL root namespace(s) will be used only for looking up dependencies.
Both local directories and URIs are accepted.
If a DSDL root namespace is specified as an input, it is automatically added to the look-up list as well.
""",
)
@click.option(
    "--output",
    "-O",
    type=str,
    help=f"""
Path to the directory where the compilation outputs will be stored.
If not specified, defaults to the current working directory.
Existing packages will be overwritten entirely.
""",
)
@click.option(
    "--allow-unregulated-fixed-port-id",
    is_flag=True,
    help="""
Instruct the DSDL front-end to accept unregulated data types with fixed port identifiers.
Make sure you understand the implications before using this option.
If not sure, ask for advice at https://forum.uavcan.org.
""",
)
def compile_(
    source: typing.Tuple[str, ...],
    lookup: typing.Tuple[str, ...],
    output: typing.Union[str, Path, None],
    allow_unregulated_fixed_port_id: bool,
) -> None:
    output = Path(output or Path.cwd()).resolve()
    _logger.info("Destination: %r", str(output))

    src_dirs: typing.List[Path] = []
    for location in source:
        src_dirs += _fetch_root_namespace_dirs(location)
    _logger.info("Source namespace dirs: %r", list(map(str, src_dirs)))

    lookup_dirs: typing.List[Path] = []
    for location in lookup:
        lookup_dirs += _fetch_root_namespace_dirs(location)
    _logger.info("Lookup namespace dirs: %r", list(map(str, lookup_dirs)))

    gpi_list = _generate_dsdl_packages(
        source_root_namespace_dirs=src_dirs,
        lookup_root_namespace_dirs=lookup_dirs,
        generated_packages_dir=output,
        allow_unregulated_fixed_port_id=allow_unregulated_fixed_port_id,
    )
    for gpi in gpi_list:
        _logger.info("Generated package %r with %d data types at %r", gpi.name, len(gpi.models), str(gpi.path))


def _fetch_root_namespace_dirs(location: str) -> typing.List[Path]:
    if "://" in location:
        dirs = _fetch_archive_dirs(location)
        _logger.info(
            "Resource %r contains the following root namespace directories: %r", location, list(map(str, dirs))
        )
        return dirs
    return [Path(location)]


def _fetch_archive_dirs(archive_uri: str) -> typing.List[Path]:
    """
    Downloads an archive from the specified URI, unpacks it into a temporary directory, and returns the list of
    directories in the root of the unpacked archive.
    """
    import requests  # Takes over 100 ms to import! Having it in the file scope is a performance disaster.

    # TODO: autodetect the type of the archive
    arch_dir = tempfile.mkdtemp(prefix="pyuavcan-cli-dsdl")
    arch_file = str(Path(arch_dir) / "dsdl.zip")

    _logger.info("Downloading the archive from %r into %r...", archive_uri, arch_file)
    response = requests.get(archive_uri)
    if response.status_code != http.HTTPStatus.OK:
        raise RuntimeError(f"Could not download the archive; HTTP error {response.status_code}")
    with open(arch_file, "wb") as f:
        f.write(response.content)

    _logger.info("Extracting the archive into %r...", arch_dir)
    with zipfile.ZipFile(arch_file) as zf:
        zf.extractall(arch_dir)

    (inner,) = [d for d in Path(arch_dir).iterdir() if d.is_dir()]  # Strip the outer layer, we don't need it

    assert isinstance(inner, Path)
    return [d for d in inner.iterdir() if d.is_dir()]


def _generate_dsdl_packages(
    source_root_namespace_dirs: typing.Iterable[Path],
    lookup_root_namespace_dirs: typing.Iterable[Path],
    generated_packages_dir: Path,
    allow_unregulated_fixed_port_id: bool,
) -> typing.Sequence[pyuavcan.dsdl.GeneratedPackageInfo]:
    lookup_root_namespace_dirs = frozenset(list(lookup_root_namespace_dirs) + list(source_root_namespace_dirs))
    generated_packages_dir.mkdir(parents=True, exist_ok=True)

    out: typing.List[pyuavcan.dsdl.GeneratedPackageInfo] = []
    for ns in source_root_namespace_dirs:
        if ns.name.startswith("."):
            _logger.debug("Skipping hidden directory %r", ns)
            continue
        dest_dir = generated_packages_dir / ns.name
        _logger.info(
            "Generating DSDL package %r from root namespace %r with lookup dirs: %r",
            str(dest_dir),
            str(ns),
            list(map(str, lookup_root_namespace_dirs)),
        )
        shutil.rmtree(dest_dir, ignore_errors=True)
        gpi = pyuavcan.dsdl.generate_package(
            root_namespace_directory=ns,
            lookup_directories=list(lookup_root_namespace_dirs),
            output_directory=generated_packages_dir,
            allow_unregulated_fixed_port_id=allow_unregulated_fixed_port_id,
        )
        if gpi is not None:
            out.append(gpi)
    return out
