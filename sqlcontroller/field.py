"""Sql Field class"""
from typing import Iterable, NamedTuple


class Field(NamedTuple):
    """Store table field data"""

    name: str
    type: str
    constraints: Iterable[str]
