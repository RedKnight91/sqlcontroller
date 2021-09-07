import pytest
import sqlite3
from sqlcontroller.sqlcontroller import SqlController
from sqlcontroller.sqlcontroller import InvalidAlphanumericError, InvalidSqlDataConstraintError, InvalidSqlDataTypeError

def test_init(db):
	sql = SqlController(db)
	
	assert sql.db == db
	assert sql.connection is None
	assert sql.cursor is None

def test_contextmanager(db):
	with SqlController(db) as sql:
		assert isinstance(sql, SqlController)
		assert sql.db == db
		assert isinstance(sql.connection, sqlite3.Connection)
		assert isinstance(sql.cursor, sqlite3.Cursor)

	assert sql.connection is None
	assert sql.cursor is None

def test__execute(sql_controller, table, person_data):
	with sql_controller as sql:
		sql._execute('insert into {table} values (?,?,?)', table, person_data)
		rows = sql._execute('select * from {table}', table)
		assert list(rows) == [person_data]

def test__execute_fail(sql_controller):
	with sql_controller as sql:
		with pytest.raises(Exception):
			sql._execute('select from {table}', 'NoTable')

def test__executemany(sql_controller, table, people_data):
	with sql_controller as sql:
		sql._executemany('insert into {table} values (?,?,?)', table, [])
		sql._executemany('insert into {table} values (?,?,?)', table, people_data)
		rows = sql._execute('select * from {table}', table)
		assert list(rows) == people_data

def test__executemany_fail(sql_controller, table, person_data):
	with sql_controller as sql:
		with pytest.raises(sqlite3.ProgrammingError):
			sql._executemany('insert into {table} values (?,?,?)', table, 'Joe')
			
		with pytest.raises(sqlite3.ProgrammingError):
			sql._executemany('insert into {table} values (?,?,?)', table, person_data)


def test_validate_alphanum():
	validate_alphanum = SqlController.validate_alphanum

	assert validate_alphanum('Hello') is None
	assert validate_alphanum('H3110') is None
	assert validate_alphanum('A') is None
	assert validate_alphanum('3') is None

def test_validate_alphanum_errors():
	validate_alphanum = SqlController.validate_alphanum

	with pytest.raises(InvalidAlphanumericError):
		validate_alphanum('Hel_o')

	with pytest.raises(InvalidAlphanumericError):
		validate_alphanum('_')

	with pytest.raises(TypeError):
		validate_alphanum(3)

def test_validate_type():
	validate_type = SqlController.validate_type

	assert validate_type('TEXT') is None
	assert validate_type('Text') is None
	assert validate_type('text') is None
	assert validate_type('real') is None

def test_validate_type_errors():
	validate_type = SqlController.validate_type

	with pytest.raises(InvalidSqlDataTypeError):
		validate_type('Hello')

	with pytest.raises(InvalidSqlDataTypeError):
		validate_type('TEXT.')

	with pytest.raises(TypeError):
		validate_type(0)

def test_validate_constraint():
	validate_constraint = SqlController.validate_constraint

	assert validate_constraint('NOT NULL') is None
	assert validate_constraint('not null') is None
	assert validate_constraint('unique') is None

def test_validate_constraint_errors():
	validate_constraint = SqlController.validate_constraint

	with pytest.raises(InvalidSqlDataConstraintError):
		validate_constraint('primary')

	with pytest.raises(InvalidSqlDataConstraintError):
		validate_constraint('PRIMARY_KEY')

	with pytest.raises(TypeError):
		validate_constraint(0)

def test_validate_iterable():
	validate_iterable = SqlController.validate_iterable

	assert validate_iterable([]) is None
	assert validate_iterable('') is None

def test_validate_iterable_errors():
	validate_iterable = SqlController.validate_iterable

	with pytest.raises(TypeError):
		validate_iterable(3)




def test_connect_db(sql_controller):
	with sql_controller as sql:
		connection = sql.connect_db()
		assert isinstance(connection, sqlite3.Connection)
		assert sql.connection is connection

def test_disconnect_db(sql_controller):
	with sql_controller as sql:
		sql.disconnect_db()
		assert sql.cursor is None
		assert sql.connection is None

def test_save_db(sql_controller, table, person_data):
	with sql_controller as sql:
		sql.add_row(table, person_data)
		sql.save_db()

		sql.connection.close()
		sql.__enter__()

		rows = sql.cursor.execute(f'select * from {table}')
		assert list(rows) == [person_data]

def test_not_save_db(sql_controller, table, person_data):
	with sql_controller as sql:
		sql.add_row(table, person_data)

		sql.connection.close()
		sql.__enter__()

		rows = sql.cursor.execute(f'select * from {table}')
		assert list(rows) == []

def test_get_cursor(sql_controller):
	with sql_controller as sql:
		cursor = sql.get_cursor()
		assert isinstance(cursor, sqlite3.Cursor)
		assert cursor is sql.cursor

def test_has_table(sql_controller, table):
	with sql_controller as sql:
		assert sql.has_table(table)

def test_has_table_false(sql_controller):
	with sql_controller as sql:
		assert not sql.has_table('NoTable')

def test_create_table(sql_controller):
	with sql_controller as sql:
		table = 'MyNewTable'
		sql.create_table(table, {'name': ('text',), 'age': ('integer',)})
		assert sql.has_table(table)

def test_delete_table(sql_controller, table):
	with sql_controller as sql:
		sql.delete_table(table)
		assert not sql.has_table(table)

def test_add_row(sql_controller, table, person_data):
	with sql_controller as sql:
		sql.add_row(table, person_data)
		rows = sql.cursor.execute(f'select * from {table}')
		assert list(rows) == [person_data]

def test_add_rows(sql_controller, table, people_data):
	with sql_controller as sql:
		sql.add_rows(table, people_data)
		rows = sql.cursor.execute(f'select * from {table}')
		assert sorted(rows) == sorted(people_data)

def test_get_row(sql_controller):
	with sql_controller as sql:
		with pytest.raises(NotImplementedError):
			sql.get_row('', [])

def test_get_rows(sql_controller):
	with sql_controller as sql:
		with pytest.raises(NotImplementedError):
			sql.get_rows('', [])

def test_get_all_rows(sql_controller, table, people_data):
	with sql_controller as sql:
		sql.cursor.executemany(f'insert into {table} values (?,?,?)', people_data)
		rows = sql.cursor.execute(f'select * from {table}')
		assert sorted(rows) == sorted(people_data)

def test_update_row(sql_controller):
	with sql_controller as sql:
		with pytest.raises(NotImplementedError):
			sql.update_row('', [], [], [])

def test_delete_rows(sql_controller):
	with sql_controller as sql:
		with pytest.raises(NotImplementedError):
			sql.delete_rows('', [])

def test_delete_all_rows_empty(sql_controller, table, people_data):
	with sql_controller as sql:
		sql.delete_all_rows(table)

def test_delete_all_rows(sql_controller, table, people_data):
	with sql_controller as sql:
		sql.cursor.executemany(f'insert into {table} values (?,?,?)', people_data)
		sql.delete_all_rows(table)
	
		rows = sql.cursor.execute(f'select * from {table}')
		assert list(rows) == []