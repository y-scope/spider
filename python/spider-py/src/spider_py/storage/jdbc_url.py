"""JDBC URL module."""

import urllib.parse
from dataclasses import dataclass


@dataclass
class JdbcParameters:
    """JDBC url parameters."""

    protocol: str
    host: str
    database: str
    port: int | None = None
    user: str | None = None
    password: str | None = None


_JdbcPrefix = "jdbc:"


def parse_jdbc_url(url: str) -> JdbcParameters:
    """
    Parses a JDBC URL.
    :param url:
    :return: The parsed JDBC parameters.
    :raises ValueError: If the JDBC URL is invalid.
    """
    protocol_prefix = ""
    if url.startswith(_JdbcPrefix):
        url = url.removeprefix(_JdbcPrefix)
        protocol_prefix = _JdbcPrefix
    parsed = urllib.parse.urlparse(url)

    msg = "Invalid JDBC URL: {}. Missing {}."
    if not parsed.scheme:
        raise ValueError(msg.format(url, "protocol"))
    if not parsed.hostname:
        raise ValueError(msg.format(url, "host"))
    if not parsed.path or not parsed.path.lstrip("/"):
        raise ValueError(msg.format(url, "database"))

    database = parsed.path.lstrip("/")
    query_params = urllib.parse.parse_qs(parsed.query)
    user = query_params.get("user", [None])[0]
    password = query_params.get("password", [None])[0]

    return JdbcParameters(
        protocol=protocol_prefix + parsed.scheme,
        host=parsed.hostname,
        port=parsed.port,
        database=database,
        user=user,
        password=password,
    )
