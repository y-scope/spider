"""Data module for Spider."""

from dataclasses import dataclass, field
from uuid import UUID

DataId = UUID


@dataclass
class DataLocality:
    """Represents the locality of a data object."""

    address: str


@dataclass
class Data:
    """Represents a data object."""

    id: DataId
    value: bytes
    localities: list[DataLocality] = field(default_factory=list)
    hard_locality: bool = False
    persisted: bool = False
