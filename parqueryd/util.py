from __future__ import annotations

import binascii
import os
import random
import time
from typing import Any

import netifaces
import zmq


def get_my_ip() -> str:
    """Get the IP address of the first ethernet interface."""
    eth_interfaces = sorted(
        [ifname for ifname in netifaces.interfaces() if (ifname.startswith("eth") or ifname.startswith("en"))]
    )
    ifname = "lo" if len(eth_interfaces) < 1 else eth_interfaces[-1]
    for x in netifaces.ifaddresses(ifname)[netifaces.AF_INET]:
        # Return first addr found
        return x["addr"]
    raise RuntimeError("No IP address found")


def bind_to_random_port(
    socket: zmq.Socket, addr: str, min_port: int = 49152, max_port: int = 65536, max_tries: int = 100
) -> bytes:
    """Bind socket to a random port and set identity before binding."""
    for _i in range(max_tries):
        try:
            port = random.randrange(min_port, max_port)
            socket.identity = bytes((f"{addr}:{port}").encode())
            socket.bind(bytes((f"tcp://*:{port}").encode()))
        except zmq.ZMQError as exception:
            en = exception.errno
            if en == zmq.EADDRINUSE:
                continue
            else:
                raise
        else:
            return socket.identity
    raise zmq.ZMQBindError("Could not bind socket to random port.")


def tree_checksum(path: str) -> str:
    """Calculate checksum of all filenames in a directory tree."""
    allfilenames: set[str] = set()
    for root, _dirs, filenames in os.walk(path):
        for filename in filenames:
            allfilenames.add(os.path.join(root, filename))
    buf = "".join(sorted(allfilenames))
    return hex(binascii.crc32(buf.encode()) & 0xFFFFFFFF)


###################################################################################################
# Various Utility methods for user-friendly info display


def show_workers(info_data: dict[str, Any], only_busy: bool = False) -> None:
    """For the given info_data dict, show a human-friendly overview of the current workers."""
    nodes: dict[str, list[dict[str, Any]]] = {}
    for w in info_data.get("workers", {}).values():
        nodes.setdefault(w["node"], []).append(w)
    for k, n in nodes.items():
        print(k)
        for nn in n:
            if only_busy and not nn.get("busy"):
                continue
            print("   ", time.ctime(nn["last_seen"]), nn.get("busy"))
