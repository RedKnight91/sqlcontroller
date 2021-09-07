"""Facilitate handling a SQL database"""

import sqlite3
from abc import ABC, abstractmethod
from sqlvalidator import AbstractValidator, SqlValidator


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
    def create_table(self, name: str, columns: dict) -> None:
        """Add new table to a database"""

    @abstractmethod
    def delete_table(self, name: str) -> None:
        """Remove a table from a database"""

    @abstractmethod
    def add_row(self, table: str, values: list, columns: list = []) -> None:
        """Add new row to a table"""

    @abstractmethod
    def get_row(self, table: str, where_clause: str) -> None:
        """Get first matching row from a table"""

    @abstractmethod
    def get_rows(self, table: str, where_clause: str) -> list:
        """Get all matching rows from a table"""

    @abstractmethod
    def get_all_rows(self, table: str) -> list:
        """Get all rows from a table"""

    @abstractmethod
    def update_rows(
        self, table: str, values: dict, where_clause: str, order, limit=-1, offset=0
    ) -> None:
        """Modify a table's row's values"""

    @abstractmethod
    def delete_rows(self, table: str, where_clause: str) -> None:
        """Remove matching rows from a table"""

    @abstractmethod
    def delete_all_rows(self, table: str) -> None:
        """Remove all rows from a table"""


class _BaseSqlController(AbstractSqlController):
    """Provide generic functionality for an SQL controller"""

    def __init__(self, database):
        self.database = database
        self.connection = self.cursor = None
        self.validator = SqlValidator()

    def __enter__(self) -> "_BaseSqlController":
        self.connection = self.connect_db()
        self.cursor = self.get_cursor()
        return self

    def __exit__(self, *_) -> None:
        self.save_db()
        self.disconnect_db()

    def _execute(self, query: str, table: str = None, values: list = []) -> None:
        self.validator.validate_alphanum(table, True, False)
        query = query.format(table=table)
        return self.cursor.execute(query, values)

    def _executemany(
        self, query: str, table: str = None, valuelists: list = []
    ) -> None:
        self.validator.validate_alphanum(table, True, False)
        query = query.format(table=table)
        return self.cursor.executemany(query, valuelists)

    @staticmethod
    def _build_add_query(values: list, columns: list = []) -> str:
        column_str = "" if not columns else f"({','.join(columns)})"
        qmarks = ",".join(["?"] * len(values))

        query = f"insert into {{table}}{column_str} values ({qmarks})"
        return query


class SqlController(_BaseSqlController):
    """Provide methods for database, table and row handling"""

    def connect_db(self) -> sqlite3.Connection:
        """Connect to a database (create if non-existent)"""
        self.connection = sqlite3.connect(self.database)
        return self.connection

    def disconnect_db(self) -> None:
        """Clear database connection"""
        self.cursor = None
        self.connection.close()
        self.connection = None

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
            self._execute("select * from {table}", name)
            return True
        except sqlite3.OperationalError:
            return False

    def create_table(self, name: str, columns: dict) -> None:
        """Create a new table
        columns: {name: (type, constraint, constraint, ...), name: (...), ...}"""

        def parse_column(col, specs):
            self.validator.validate_iterable(specs)

            type_, constraints = specs[0], specs[1:]

            self.validator.validate_type(type_)
            for c in constraints:
                self.validator.validate_constraint(c)

            return (col, type_, *constraints)

        column_strs = [
            " ".join(parse_column(col, specs)) for col, specs in columns.items()
        ]
        columns_str = ", ".join(column_strs)

        query = f"create table if not exists {{table}} ({columns_str});"
        self._execute(query, name)

    def delete_table(self, name: str) -> None:
        """Delete table"""
        query = "drop table {table};"
        self._execute(query, name)

    def add_row(self, table: str, values: list, columns: list = []) -> None:
        """Add row to table"""
        query = SqlController._build_add_query(values, columns)
        self._execute(query, table, values)

    def add_rows(self, table: str, valuelists: list, columns: list = []) -> None:
        """Add multiple rows to table"""
        query = SqlController._build_add_query(valuelists, columns)
        self._executemany(query, table, valuelists)

    def get_row(self, table: str, where_clause: str) -> None:
        """Get first matching row from a table"""
        query = f"select * from {{table}} where {where_clause}"
        self._execute(query, table)

        return self.cursor.fetchone()

    def get_rows(self, table: str, where_clause: str) -> list:
        """Get all matching rows from a table"""
        query = f"select * from {{table}} where {where_clause}"
        self._execute(query, table)

        return self.cursor.fetchall()

    def get_all_rows(self, table: str) -> list:
        """Get all rows from a table"""
        query = "select * from {{table}}"
        self._execute(query, table)
        return self.cursor.fetchall()

    def update_rows(
        self,
        table: str,
        values: dict,
        where_clause: str,
        order: str,
        limit: int = -1,
        offset: int = 0,
    ) -> None:
        """Update row values in a table"""

        values_str = ",".join([f"{k} = {v}" for k, v in values.items()])

        query = f"update {{table}} set {values_str} where {where_clause} order by {order} limit {limit} offset {offset}"
        self._execute(query, table)

    def delete_rows(self, table: str, where_clause: str) -> None:
        """Remove matching rows from a table"""
        query = f"delete from {{table}} where {where_clause}"
        self._execute(query, table)

    def delete_all_rows(self, table: str) -> None:
        """Remove all matching rows from a table"""
        query = "delete from {{table}}"
        self._execute(query, table)
