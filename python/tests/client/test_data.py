"""Tests for client data module."""

import pytest
from test_driver import driver  # noqa: F401

from spider import Data, Driver


class TestData:
    """Test data class for client data module."""

    @pytest.mark.storage
    def test_data(self, driver: Driver) -> None:  # noqa: F811
        """Tests data creation"""
        data = Data(b"test_data")
        data.hard_locality = True
        data.add_locality("localhost")

        assert data.value == b"test_data"
        assert data.hard_locality is True
        assert data.get_localities() == ["localhost"]

        driver.create_data(data)
