"""Facilitate handling a SQL database"""

import sqlite3
from abc import ABC, abstractmethod
from typing import Any, Collection, Iterable
from sqlcontroller.sqlvalidator import AbstractValidator, SqliteValidator
from sqlcontroller.sqlquerybuilder import SqliteQueryBuilder


class AbstractSqlController(ABC):  # pragma: no cover
    """Abstract controller"""

    database: str
    connection: sqlite3.Connection
    cursor: sqlite3.Cursor
    validator: AbstractValidator

    @abstractmethod
    def __enter__(self) -> "AbstractSqlController":
        """Enter controller context"""

    @abstractmethod
    def __exit__(self, *_) -> None:
        """Exit controller context"""

    @abstractmethod
    def connect_db(self) -> sqlite3.Connection:
        """Connect to a database"""

    @abstractmethod
    def disconnect_db(self) -> None:
        """Disconnect from a database"""

    @abstractmethod
    def save_db(self) -> None:
        """Save changes to a database"""

    @abstractmethod
    def get_cursor(self) -> sqlite3.Cursor:
        """Get database cursor"""

    @abstractmethod
    def create_table(self, name: str, fields: dict) -> None:
        """Add new table to a database"""

    @abstractmethod
    def delete_table(self, name: str) -> None:
        """Remove a table from a database"""

    @abstractmethod
    def add_row(self, table: str, values: Iterable, fields: Collection) -> None:
        """Add new row to a table"""

    @abstractmethod
    def add_rows(
        self, table: str, valuelists: Iterable[Iterable], fields: Collection
    ) -> None:
        """Add new row to a table"""

    @abstractmethod
    def get_row(self, table: str, clause: str) -> None:
        """Get first matching row from a table"""

    @abstractmethod
    def get_rows(self, table: str, clause: str) -> list:
        """Get all matching rows from a table"""

    @abstractmethod
    def get_all_rows(self, table: str) -> list:
        """Get all rows from a table"""

    @abstractmethod
    def update_rows(self, table: str, values: dict, clause: str) -> None:
        """Modify a table's row's values"""

    @abstractmethod
    def delete_rows(self, table: str, clause: str) -> None:
        """Remove matching rows from a table"""

    @abstractmethod
    def delete_all_rows(self, table: str) -> None:
        """Remove all rows from a table"""


class _BaseSqlController(AbstractSqlController):
    """Provide generic functionality for an SQL controller"""

    connection: sqlite3.Connection
    cursor: sqlite3.Cursor

    def __init__(self, database):
        self.database = database

    def __enter__(self) -> "_BaseSqlController":
        self.connection = self.connect_db()
        self.cursor = self.get_cursor()
        return self

    def __exit__(self, *_) -> None:
        self.save_db()
        self.disconnect_db()

    def _execute(self, query: str, table: str = None, values: Iterable = []) -> Any:
        query = query.format(table=table)
        return self.cursor.execute(query, values)

    def _executemany(
        self, query: str, table: str = None, valuelists: Iterable = []
    ) -> Any:
        query = query.format(table=table)
        return self.cursor.executemany(query, valuelists)


class SqliteController(_BaseSqlController):
    """Provide methods for database, table and row handling"""

    validator: SqliteValidator = SqliteValidator()

    def connect_db(self) -> sqlite3.Connection:
        """Connect to a database (create if non-existent)"""
        self.connection = sqlite3.connect(self.database)
        return self.connection

    def disconnect_db(self) -> None:
        """Clear database connection"""
        self.connection.close()
        del self.connection
        del self.cursor

    def save_db(self) -> None:
        """Save changes to a database"""
        self.connection.commit()

    def get_cursor(self) -> sqlite3.Cursor:
        """Get database cursor"""
        self.cursor = self.connection.cursor()
        return self.cursor

    def has_table(self, name: str) -> bool:
        """Check if table exists"""
        try:
            self._execute("select * from {table} limit 1", name)
            return True
        except sqlite3.OperationalError:
            return False

    def create_table(self, name: str, fields: dict) -> None:
        """Create a new table
        fields: {name: (type, constraint, constraint, ...), name: (...), ...}"""

        self.validator.validate_table_name(name)
        query = SqliteQueryBuilder.build_table_create_query(self.validator, fields)
        self._execute(query, name)

    def delete_table(self, name: str) -> None:
        """Delete table"""
        query = "drop table {table};"
        self._execute(query, name)

    @staticmethod
    def build_query_clauses(
        where: str, order: str = "", limit: int = 0, offset: int = 0
    ) -> str:
        """Build a query's clauses string"""
        return SqliteQueryBuilder.build_query_clauses(where, order, limit, offset)

    def add_row(self, table: str, values: Iterable, fields: Collection) -> None:
        """Add row to table"""
        query = SqliteQueryBuilder.build_insert_query(fields)
        self._execute(query, table, values)

    def add_rows(
        self, table: str, valuelists: Iterable[Iterable], fields: Collection
    ) -> None:
        """Add multiple rows to table"""
        query = SqliteQueryBuilder.build_insert_query(fields)
        self._executemany(query, table, valuelists)

    def get_row(self, table: str, clause: str) -> Any:
        """Get first matching row from a table"""
        query = f"select * from {{table}} {clause}"
        self._execute(query, table)

        # TODO check fetchone value
        return self.cursor.fetchone()

    def get_rows(self, table: str, clause: str) -> Any:
        """Get all matching rows from a table"""
        query = f"select * from {{table}} {clause}"
        self._execute(query, table)

        # TODO check fetchall value
        return self.cursor.fetchall()

    def get_all_rows(self, table: str) -> list:
        """Get all rows from a table"""
        query = "select * from {table}"
        self._execute(query, table)
        return self.cursor.fetchall()

    def update_rows(self, table: str, values: dict, clause: str = None) -> None:
        """Update row values in a table"""

        values_str = ",".join([f"{k} = {v}" for k, v in values.items()])

        clause = clause if clause else str()
        query = f"update {table} set {values_str} {clause}"
        self._execute(query, table)

    def delete_rows(self, table: str, clause: str) -> None:
        """Remove matching rows from a table"""
        query = f"delete from {{table}} {clause}"
        self._execute(query, table)

    def delete_all_rows(self, table: str) -> None:
        """Remove all matching rows from a table"""
        query = "delete from {table}"
        self._execute(query, table)
