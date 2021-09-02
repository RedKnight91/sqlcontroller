from sqlcontroller import SqlController
import pytest
import os

@pytest.fixture
def db():
	return 'testDatabase.db'

@pytest.fixture
def table():
	return 'TestTable'

@pytest.fixture
def person_data():
	return ('a19c9z', 'Joe', 30)

@pytest.fixture
def people_data():
	return [
		('a19c9z', 'Joe', 30),
		('b01d0y', 'Mia', 27),
		('c55x3x', 'Bud', 26)
	]

@pytest.fixture
def sql_controller(db, table) -> SqlController:
	class OpenCloseController(SqlController):
		def __enter__(self):
			'''Opens db connection and creates a test table'''
			super().__enter__()
			self.cursor.execute(f'create table if not exists {table} (id text primary key, name text not null, age integer)')
			return self

		def __exit__(self, *args):
			'''Closes db connection if it hasn't been already, deletes test database'''
			if self.connection and self.cursor:
				super().__exit__(*args)

			os.remove(db)

	return OpenCloseController(db)