"""Data module for Spider."""

from dataclasses import dataclass
from uuid import UUID

DataId = UUID


@dataclass
class Data:
    """Represents a data object."""

    id: DataId
    value: bytes
