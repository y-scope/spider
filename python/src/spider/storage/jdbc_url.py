"""JDBC URL module."""

import re
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


pattern = re.compile(
    r"^(?P<protocol>[a-zA-Z][a-zA-Z0-9+.-]*(:[a-zA-Z0-9+.-]*)?)://"
    r"(?P<host>([a-zA-Z0-9.-]+|\d{1,3}(?:\.\d{1,3}){3}))"
    r"(?::(?P<port>\d+))?"
    r"/(?P<database>[a-zA-Z0-9_\-]+)"
    r"(?:\?(?P<query>[a-zA-Z0-9_\-=&]+))?"
)


def parse_jdbc_url(url: str) -> JdbcParameters:
    """
    Parses a JDBC URL.
    :param url:
    :return:
    :raises ValueError: If the JDBC URL is invalid.
    """
    match = pattern.match(url)
    if not match:
        msg = f"Invalid JDBC URL: {url}"
        raise ValueError(msg)

    groups = match.groupdict()
    query = groups.get("query") or ""
    query_params = dict(param.split("=", 1) for param in query.split("&") if "=" in param)

    protocol = groups.get("protocol")
    if not protocol:
        msg = f"Protocol is required in JDBC URL: {url}"
        raise ValueError(msg)
    host = groups.get("host")
    if not host:
        msg = f"Host is required in JDBC URL: {url}"
        raise ValueError(msg)
    database = groups.get("database")
    if not database:
        msg = f"Database is required in JDBC URL: {url}"
        raise ValueError(msg)

    return JdbcParameters(
        protocol=protocol,
        host=host,
        port=int(groups["port"]) if groups.get("port") else None,
        database=database,
        user=query_params.get("user"),
        password=query_params.get("password"),
    )
