"""Utilities for the network port."""

import socket


def _get_free_tcp_port() -> int:
    """:return: A free TCP port number."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        port: int = s.getsockname()[1]
        return port


g_scheduler_port = _get_free_tcp_port()
