"""Provide fixtures for testing"""

import os
import pytest
from sqlcontroller.sqltable import SqliteTable
from sqlcontroller.sqlcontroller import SqliteController
from sqlcontroller.sqlfield import Field

# pylint: disable=missing-function-docstring
# pylint: disable=redefined-outer-name


@pytest.fixture
def database():
    return "testDatabase.db"


@pytest.fixture
def table_name():
    return "TestTable"


@pytest.fixture
def fields():
    return [
        Field("id", "text", ["primary key"]),
        Field("name", "text", ["not null", "unique"]),
        Field("age", "integer", ["not null"]),
    ]


@pytest.fixture
def field():
    return Field("id", "text", ["primary key"])


@pytest.fixture
def person_fields():
    return ("id", "name", "age")


@pytest.fixture
def person_data():
    return ("a19c9z", "Joe", 30)


@pytest.fixture
def people_data():
    return [("a19c9z", "Joe", 30), ("b01d0y", "Mia", 27), ("c55x3x", "Bud", 26)]


@pytest.fixture
def age_increment():
    return {"age": "age + 1"}


@pytest.fixture
def people_data_older():
    return [("a19c9z", "Joe", 31), ("b01d0y", "Mia", 28), ("c55x3x", "Bud", 27)]


@pytest.fixture
def people_data_some_older():
    return [("a19c9z", "Joe", 30), ("b01d0y", "Mia", 28), ("c55x3x", "Bud", 27)]


@pytest.fixture
def name_clause():
    clause = SqliteController.build_query_clauses("name = 'Mia'")
    return clause


@pytest.fixture
def older_clause():
    clause = SqliteController.build_query_clauses("age > 10")
    return clause


@pytest.fixture
def younger_clause():
    clause = SqliteController.build_query_clauses("age < 29")
    return clause


@pytest.fixture
def invalid_string_clause():
    return "inject SQL code"


@pytest.fixture
def invalid_clause():
    clause = SqliteController.build_query_clauses("inject SQL code")
    return clause


@pytest.fixture
def people_older_result():
    return [("a19c9z", "Joe", 30)]


@pytest.fixture
def sql_controller(database, table_name) -> SqliteController:
    class SqliteControllerContext(SqliteController):
        """Provide a handy SqliteController with context"""

        def __enter__(self):
            """Opens db connection and creates a test table"""
            super().__enter__()
            query = (
                f"create table if not exists {table_name} "
                "(id text primary key, name text not null, age integer)"
            )
            self.cursor.execute(query)
            return self

        def __exit__(self, *args):
            """Closes db connection if it hasn't been already, deletes test database"""
            if hasattr(self, "connection") and hasattr(self, "cursor"):
                super().__exit__(*args)

            os.remove(database)

    return SqliteControllerContext(database)


@pytest.fixture
def sql_table(table_name, sql_controller) -> SqliteTable:
    class SqliteTableContext(SqliteTable):
        """Provide a handy SqliteTable with context and a controller"""

        def __enter__(self):
            """Setup controller"""
            self.controller.__enter__()
            return self

        def __exit__(self, *args):
            """Breakdown controller"""
            self.controller.__exit__(*args)

    return SqliteTableContext(table_name, sql_controller)
