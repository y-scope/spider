"""Tests for the jdbc url module."""

import pytest

from spider.storage.jdbc_url import parse_jdbc_url


class TestJdbcUrl:
    """Tests for the jdbc url module."""

    def test_full_jdbc_url(self) -> None:
        """Tests parsing a full JDBC URL with all fields."""
        url = "jdbc::mariadb://localhost:3306/dbname?user=root&password=secret"
        params = parse_jdbc_url(url)
        assert params.protocol == "jdbc::mariadb"
        assert params.host == "localhost"
        assert params.port == 3306
        assert params.database == "dbname"
        assert params.user == "root"
        assert params.password == "secret"  # noqa: S105

    def test_jdbc_url_simple(self) -> None:
        """Tests parsing a simple JDBC URL without port, user and password."""
        url = "jdbc::postgresql://localhost/dbname"
        params = parse_jdbc_url(url)
        assert params.protocol == "jdbc::postgresql"
        assert params.host == "localhost"
        assert params.port is None
        assert params.database == "dbname"
        assert params.user is None
        assert params.password is None

    def test_invalid_jdbc_url(self) -> None:
        """Tests parsing an invalid JDBC URL."""
        url = "invalid_jdbc_url"
        with pytest.raises(ValueError, match=f"Invalid JDBC URL: {url}"):
            parse_jdbc_url(url)
