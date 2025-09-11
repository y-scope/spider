"""Data module for Spider."""

from dataclasses import dataclass, field
from uuid import UUID

DataId = UUID


DataAddr = str


@dataclass
class DataLocality:
    """Represents the locality of a data object."""

    address: str


@dataclass
class Data:
    """Represents a data object."""

    id: DataId
    value: bytes
    localities: list[DataAddr] = field(default_factory=list)
    hard_locality: bool = False
    persisted: bool = False
