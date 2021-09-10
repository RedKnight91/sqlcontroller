from sqlcontroller.sqlcontroller import SqliteController
import pytest
import os


@pytest.fixture
def database():
    return "testDatabase.db"


@pytest.fixture
def table():
    return "TestTable"


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
def sql_controller(database, table) -> SqliteController:
    class OpenCloseController(SqliteController):
        def __enter__(self):
            """Opens db connection and creates a test table"""
            super().__enter__()
            query = f"create table if not exists {table} (id text primary key, name text not null, age integer)"
            self.cursor.execute(query)
            return self

        def __exit__(self, *args):
            """Closes db connection if it hasn't been already, deletes test database"""
            if hasattr(self, "connection") and hasattr(self, "cursor"):
                super().__exit__(*args)

            os.remove(database)

    return OpenCloseController(database)
