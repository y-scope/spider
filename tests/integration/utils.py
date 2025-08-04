"""Utilities for the network port."""

import socket

IPv4Addr = tuple[str, int]
IPv6Addr = tuple[str, int, int, int]
AddrType = IPv4Addr | IPv6Addr

def _get_free_tcp_port() -> int:
    """Returns a free TCP port."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        addr: AddrType = s.getsockname()
        return addr[1]


g_scheduler_port = _get_free_tcp_port()
