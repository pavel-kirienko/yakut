# Copyright (c) 2019 UAVCAN Consortium
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@uavcan.org>

import os
import sys
import time
import typing
import dataclasses
import pytest
from .subprocess import Subprocess


@dataclasses.dataclass(frozen=True)
class TransportConfig:
    expression: str
    can_transmit: bool


TransportFactory = typing.Callable[[typing.Optional[int]], TransportConfig]
"""
This factory constructs arguments for the CLI instructing it to use a particular transport configuration.
The factory takes one argument - the node-ID - which can be None (anonymous).
"""


def _generate() -> typing.Iterator[typing.Callable[[], typing.Iterator[TransportFactory]]]:
    """
    Sensible transport configurations supported by the CLI to test against.
    Don't forget to extend when adding support for new transports.
    """
    if sys.platform == "linux":  # pragma: no branch

        def sudo(cmd: str, ensure_success: bool = True) -> None:
            c = f"sudo {cmd}"
            r = os.system(c)
            if ensure_success and 0 != r:  # pragma: no cover
                raise RuntimeError(f"Command {c!r} failed with exit code {r}")

        sudo("modprobe can")
        sudo("modprobe can_raw")
        sudo("modprobe vcan")
        for idx in range(3):
            iface = f"vcan{idx}"
            sudo(f"ip link add dev {iface} type vcan", ensure_success=False)
            sudo(f"ip link set     {iface} mtu 72")
            sudo(f"ip link set up  {iface}")

        def vcan() -> typing.Iterator[TransportFactory]:
            yield lambda nid: TransportConfig(
                expression=f"CAN(can.media.socketcan.SocketCANMedia('vcan0',64),local_node_id={nid})",
                can_transmit=True,
            )

        def vcan_tmr() -> typing.Iterator[TransportFactory]:
            # In anonymous mode, transfers with >8/32 bytes may fail for some transports. This is intentional.
            yield lambda nid: TransportConfig(
                expression=(
                    ",".join(
                        f"CAN(can.media.socketcan.SocketCANMedia('vcan{idx}',{mtu}),local_node_id={nid})"
                        for idx, mtu in enumerate([8, 32, 64])
                    )
                ),
                can_transmit=True,
            )

        yield vcan
        yield vcan_tmr

    serial_broker_port = 50905
    serial_endpoint = f"socket://localhost:{serial_broker_port}"

    def launch_serial_broker() -> Subprocess:
        return Subprocess("ncat", "--broker", "--listen", "--verbose", f"--source-port={serial_broker_port}")

    def serial_tunneled_via_tcp() -> typing.Iterator[TransportFactory]:
        broker = launch_serial_broker()
        yield lambda nid: TransportConfig(
            expression=f"Serial('{serial_endpoint}',local_node_id={nid})",
            can_transmit=True,
        )
        time.sleep(1.0)  # Ensure all clients have disconnected to avoid warnings in the test logs.
        broker.wait(5.0, interrupt=True)

    def udp_loopback() -> typing.Iterator[TransportFactory]:
        yield lambda nid: (
            TransportConfig(expression=f"UDP('127.0.0.{nid}')", can_transmit=True)
            if nid is not None
            else TransportConfig(expression="UDP('127.0.0.1',anonymous=True)", can_transmit=False)
        )

    def heterogeneous_udp_serial() -> typing.Iterator[TransportFactory]:
        broker = launch_serial_broker()
        yield lambda nid: TransportConfig(
            expression=(
                ",".join(
                    [
                        f"Serial('{serial_endpoint}',local_node_id={nid})",
                        (f"UDP('127.0.0.{nid}')" if nid is not None else "UDP('127.0.0.1',anonymous=True)"),
                    ]
                )
            ),
            can_transmit=nid is not None,
        )
        time.sleep(1.0)  # Ensure all clients have disconnected to avoid warnings in the test logs.
        broker.wait(5.0, interrupt=True)

    yield serial_tunneled_via_tcp
    yield udp_loopback
    yield heterogeneous_udp_serial


@pytest.fixture(params=_generate())
def transport_factory(request: typing.Any) -> typing.Iterable[TransportFactory]:
    yield from request.param()
