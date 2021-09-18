"""SQL table class"""

import sqlite3
from abc import ABC, abstractmethod
from typing import Any, Collection, Iterable, TYPE_CHECKING, Optional, Sequence
from sqlcontroller.querybuilder import SqliteQueryBuilder
from sqlcontroller.field import Field

if TYPE_CHECKING:
    from sqlcontroller.controller import AbstractSqlController, SqliteController


class DbTable(ABC):
    """Provide base for database table classes"""

    controller: "AbstractSqlController"
    name: str

    def _execute(
        self, query: str, values: Optional[Collection] = None
    ) -> sqlite3.Cursor:
        """Forward execution to controller"""
        return self.controller.execute(query, self.name, values)

    def _executemany(
        self, query: str, valuelists: Optional[Collection] = None
    ) -> sqlite3.Cursor:
        """Forward execution to controller"""
        return self.controller.executemany(query, self.name, valuelists)

    @abstractmethod
    def add_row(
        self, values: Collection, fields: Optional[Iterable[Field]] = None
    ) -> None:
        """Add new row to a table"""

    @abstractmethod
    def add_rows(
        self, valuelists: Sequence[Collection], fields: Optional[Iterable[Field]] = None
    ) -> None:
        """Add new row to a table"""

    @abstractmethod
    def count_rows(self, clause: str = "") -> int:
        """Count rows"""

    @abstractmethod
    def get_row(
        self, fields: Iterable[str] = None, clause: str = "", as_dict: bool = False
    ) -> Any:
        """Get first matching row from a table"""

    @abstractmethod
    def _get_rows_cursor(
        self, fields: Iterable[str] = None, clause: str = ""
    ) -> sqlite3.Cursor:
        """Get cursor for a select query"""

    @abstractmethod
    def get_rows(
        self, fields: Iterable[str] = None, clause: str = "", as_dicts: bool = False
    ) -> list[Any]:
        """Get all matching rows from a table"""

    @abstractmethod
    def get_rows_generator(
        self, fields: Iterable[str] = None, clause: str = "", as_dicts: bool = False
    ) -> list[Any]:
        """Get generator for all matching rows from a table"""

    @abstractmethod
    def update_rows(self, values: dict, clause: str) -> None:
        """Modify a table's row's values"""

    @abstractmethod
    def delete_rows(self, clause: str) -> None:
        """Remove matching rows from a table"""

    @abstractmethod
    def delete_all_rows(self) -> None:
        """Remove all rows from a table"""


IterFieldOpt = Optional[Iterable[Field]]


class SqliteTable(DbTable):
    """Define methods to operate on database table"""

    controller: "SqliteController"

    def __init__(self, name: str, controller: "SqliteController") -> None:
        self.name = name
        self.controller = controller

    def add_row(self, values: Collection, fields: IterFieldOpt = None) -> None:
        query = SqliteQueryBuilder.build_insert_query(values, fields)
        self._execute(query, values)

    def add_rows(
        self, valuelists: Sequence[Collection], fields: IterFieldOpt = None
    ) -> None:
        query = SqliteQueryBuilder.build_insert_query(valuelists[0], fields)
        self._executemany(query, valuelists)

    def count_rows(self, clause: str = "") -> int:
        query = f"select count(*) from {{table}} {clause}"
        self._execute(query)
        count = self.controller.fetchone()[0]
        return count

    def get_row(
        self, fields: Iterable[str] = None, clause: str = "", as_dict: bool = False
    ) -> Any:
        fields = iterable_to_fields(fields)
        query = f"select {fields} from {{table}} {clause}"
        self._execute(query)

        row = self.controller.fetchone()
        return sqliterow_to_dict(row) if as_dict else sqliterow_to_tuple(row)

    def _get_rows_cursor(
        self, fields: Iterable[str] = None, clause: str = ""
    ) -> sqlite3.Cursor:
        fields = iterable_to_fields(fields)
        query = f"select {fields} from {{table}} {clause}"
        return self._execute(query)

    def get_rows(
        self, fields: Iterable[str] = None, clause: str = "", as_dicts: bool = False
    ) -> list[Any]:
        cursor = self._get_rows_cursor(fields, clause)
        rows = cursor.fetchall()

        converter = sqliterows_to_dicts if as_dicts else sqliterows_to_tuples
        return converter(rows)

    def get_rows_generator(
        self, fields: Iterable[str] = None, clause: str = "", as_dicts: bool = False
    ) -> list[Any]:
        cursor = self._get_rows_cursor(fields, clause)

        converter = sqliterows_to_dicts if as_dicts else sqliterows_to_tuples
        return converter(cursor)

    def update_rows(self, values: dict, clause: str = None) -> None:
        values_str = ",".join([f"{k} = {v}" for k, v in values.items()])

        clause = clause if clause else str()
        query = f"update {{table}} set {values_str} {clause}"
        self._execute(query)

    def delete_rows(self, clause: str) -> None:
        query = f"delete from {{table}} {clause}"
        self._execute(query)

    def delete_all_rows(self) -> None:
        query = "delete from {table}"
        self._execute(query)


def iterable_to_fields(fields: Optional[Iterable[Any]]) -> str:
    """Convert iterable to query fields string"""
    fields = f"{', '.join(i for i in fields)}" if fields else "*"
    return fields


def sqliterow_to_dict(row):
    """Convert sqlite3.Row instance to dict"""
    return dict(zip(row.keys(), tuple(row)))


def sqliterow_to_tuple(row):
    """Convert sqlite3.Row instance to tuple"""
    return tuple(row)


def sqliterows_to_dicts(rows):
    """Convert sqlite3.Row instances to dicts"""
    for row in rows:
        yield sqliterow_to_dict(row)


def sqliterows_to_tuples(rows):
    """Convert sqlite3.Row instances to tuples"""
    for row in rows:
        yield sqliterow_to_tuple(row)
