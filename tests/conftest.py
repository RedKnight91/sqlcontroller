from sqlcontroller.sqlcontroller import SqliteController
import pytest
import os


@pytest.fixture
def db():
    return "testDatabase.db"


@pytest.fixture
def table():
    return "TestTable"


@pytest.fixture
def person_data():
    return ("a19c9z", "Joe", 30)


@pytest.fixture
def people_data():
    return [("a19c9z", "Joe", 30), ("b01d0y", "Mia", 27), ("c55x3x", "Bud", 26)]


@pytest.fixture
def people_data_older():
    return [("a19c9z", "Joe", 31), ("b01d0y", "Mia", 28), ("c55x3x", "Bud", 27)]


@pytest.fixture
def name_clause():
    clause = SqliteController.build_query_clauses("name = 'Mia'")
    return clause


@pytest.fixture
def age_clause():
    clause = SqliteController.build_query_clauses("age > 10")
    return clause


@pytest.fixture
def sql_controller(db, table) -> SqliteController:
    class OpenCloseController(SqliteController):
        def __enter__(self):
            """Opens db connection and creates a test table"""
            super().__enter__()
            self.cursor.execute(
                f"create table if not exists {table} (id text primary key, name text not null, age integer)"
            )
            return self

        def __exit__(self, *args):
            """Closes db connection if it hasn't been already, deletes test database"""
            if self.connection and self.cursor:
                super().__exit__(*args)

            os.remove(db)

    return OpenCloseController(db)
