"""SQL table class"""

from abc import ABC, abstractmethod
from typing import Any, Collection, Iterable, TYPE_CHECKING, Optional, Sequence
from sqlcontroller.sqlquerybuilder import SqliteQueryBuilder

if TYPE_CHECKING:
    from sqlcontroller.sqlcontroller import AbstractSqlController, SqliteController


class DbTable(ABC):
    """Provide base for database table classes"""

    controller: "AbstractSqlController"
    name: str

    def _execute(self, query: str, values: Optional[Collection] = None) -> Any:
        """Forward execution to controller"""
        self.controller.execute(query, self.name, values)

    def _executemany(self, query: str, valuelists: Optional[Collection] = None) -> Any:
        """Forward execution to controller"""
        self.controller.executemany(query, self.name, valuelists)

    @abstractmethod
    def add_row(self, values: Collection, fields: Optional[Iterable] = None) -> None:
        """Add new row to a table"""

    @abstractmethod
    def add_rows(
        self, valuelists: Sequence[Collection], fields: Optional[Iterable] = None
    ) -> None:
        """Add new row to a table"""

    @abstractmethod
    def get_row(self, clause: str) -> None:
        """Get first matching row from a table"""

    @abstractmethod
    def get_rows(self, clause: str) -> list:
        """Get all matching rows from a table"""

    @abstractmethod
    def get_all_rows(self) -> list:
        """Get all rows from a table"""

    @abstractmethod
    def update_rows(self, values: dict, clause: str) -> None:
        """Modify a table's row's values"""

    @abstractmethod
    def delete_rows(self, clause: str) -> None:
        """Remove matching rows from a table"""

    @abstractmethod
    def delete_all_rows(self) -> None:
        """Remove all rows from a table"""


class SqliteTable(DbTable):
    """Define methods to operate on database table"""

    controller: "SqliteController"

    def __init__(self, name: str, controller: "SqliteController") -> None:
        self.name = name
        self.controller = controller

    def add_row(self, values: Collection, fields: Optional[Iterable] = None) -> None:
        """Add row to table"""
        query = SqliteQueryBuilder.build_insert_query(values, fields)
        self._execute(query, values)

    def add_rows(
        self, valuelists: Sequence[Collection], fields: Optional[Iterable] = None
    ) -> None:
        """Add multiple rows to table"""
        query = SqliteQueryBuilder.build_insert_query(valuelists[0], fields)
        self._executemany(query, valuelists)

    def get_row(self, clause: str) -> Any:
        """Get first matching row from a table"""
        query = f"select * from {{table}} {clause}"
        self._execute(query)

        return self.controller.fetchone()

    def get_rows(self, clause: str) -> list:
        """Get all matching rows from a table"""
        query = f"select * from {{table}} {clause}"
        self._execute(query)

        return self.controller.fetchall()

    def get_all_rows(self) -> list:
        """Get all rows from a table"""
        query = "select * from {table}"
        self._execute(query)

        return self.controller.fetchall()

    def update_rows(self, values: dict, clause: str = None) -> None:
        """Update row values in a table"""

        values_str = ",".join([f"{k} = {v}" for k, v in values.items()])

        clause = clause if clause else str()
        query = f"update {{table}} set {values_str} {clause}"
        self._execute(query)

    def delete_rows(self, clause: str) -> None:
        """Remove matching rows from a table"""
        query = f"delete from {{table}} {clause}"
        self._execute(query)

    def delete_all_rows(self) -> None:
        """Remove all matching rows from a table"""
        query = "delete from {table}"
        self._execute(query)
