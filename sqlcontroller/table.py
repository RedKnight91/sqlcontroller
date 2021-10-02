"""SQL table class"""

import sqlite3
from abc import ABC, abstractmethod
from typing import Any, Collection, Generator, Iterable, TYPE_CHECKING, Optional, Sequence, Union
from sqlcontroller.querybuilder import SqliteQueryBuilder
from sqlcontroller.field import Field
from sqlcontroller.row_conversion import DictOrTuple, convert_sqliterow, convert_sqliterows

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
    def query(self, query: str, as_dict: bool = False) -> list[Any]:
        """Return the result of a direct query"""

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
    ) -> DictOrTuple:
        """Get first matching row from a table"""

    @abstractmethod
    def _get_rows_cursor(
        self, fields: Iterable[str] = None, clause: str = ""
    ) -> sqlite3.Cursor:
        """Get cursor for a select query"""

    @abstractmethod
    def get_rows(
        self, fields: Iterable[str] = None, clause: str = "", as_dicts: bool = False
    ) -> Generator[DictOrTuple, None, None]:
        """Get all matching rows from a table"""

    @abstractmethod
    def get_rows_generator(
        self, fields: Iterable[str] = None, clause: str = "", as_dicts: bool = False
    ) -> Generator[DictOrTuple, None, None]:
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

    def query(self, query: str, as_dict: bool = False) -> list[Any]:
        cursor = self._execute(query)
        rows = convert_sqliterows(cursor.fetchall(), as_dict)
        return rows

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

    def _get_rows_cursor(
        self, fields: Iterable[str] = None, clause: str = ""
    ) -> sqlite3.Cursor:
        fields = iterable_to_fields(fields)
        query = f"select {fields} from {{table}} {clause}"
        return self._execute(query)

    def get_row(
        self, fields: Iterable[str] = None, clause: str = "", as_dict: bool = False
    ) -> DictOrTuple:
        cursor = self._get_rows_cursor(fields, clause)
        row = convert_sqliterow(cursor.fetchone(), as_dict)

        return row

    def get_rows(
        self, fields: Iterable[str] = None, clause: str = "", as_dicts: bool = False
    ) -> Generator[DictOrTuple, None, None]:
        cursor = self._get_rows_cursor(fields, clause)
        rows = convert_sqliterows(cursor.fetchall(), as_dicts)

        return rows

    def get_rows_generator(
        self, fields: Iterable[str] = None, clause: str = "", as_dicts: bool = False
    ) -> Generator[DictOrTuple, None, None]:
        cursor = self._get_rows_cursor(fields, clause)
        rows = convert_sqliterows(cursor, as_dicts)

        return rows

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