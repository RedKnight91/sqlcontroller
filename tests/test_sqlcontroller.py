import pytest
import sqlite3
from sqlcontroller.sqlcontroller import SqliteController


def test_init(database):
    sql = SqliteController(database)

    assert sql.database == database
    assert sql.connection is None
    assert sql.cursor is None


def test_contextmanager(database):
    with SqliteController(database) as sql:
        assert isinstance(sql, SqliteController)
        assert sql.database == database
        assert isinstance(sql.connection, sqlite3.Connection)
        assert isinstance(sql.cursor, sqlite3.Cursor)

    assert sql.connection is None
    assert sql.cursor is None


def test__execute(sql_controller, table, person_data):
    with sql_controller as sql:
        sql._execute("insert into {table} values (?,?,?)", table, person_data)
        rows = sql._execute("select * from {table}", table)
        assert list(rows) == [person_data]


def test__execute_fail(sql_controller):
    with sql_controller as sql:
        with pytest.raises(Exception):
            sql._execute("select from {table}", "NoTable")


def test__executemany(sql_controller, table, people_data):
    with sql_controller as sql:
        sql._executemany("insert into {table} values (?,?,?)", table, [])
        sql._executemany("insert into {table} values (?,?,?)", table, people_data)
        rows = sql._execute("select * from {table}", table)
        assert list(rows) == people_data


def test__executemany_fail(sql_controller, table, person_data):
    with sql_controller as sql:
        with pytest.raises(sqlite3.ProgrammingError):
            sql._executemany("insert into {table} values (?,?,?)", table, "Joe")

        with pytest.raises(sqlite3.ProgrammingError):
            sql._executemany("insert into {table} values (?,?,?)", table, person_data)


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

        rows = sql.cursor.execute(f"select * from {table}")
        assert list(rows) == [person_data]


def test_not_save_db(sql_controller, table, person_data):
    with sql_controller as sql:
        sql.add_row(table, person_data)

        sql.connection.close()
        sql.__enter__()

        rows = sql.cursor.execute(f"select * from {table}")
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
        assert not sql.has_table("NoTable")


def test_create_table(sql_controller):
    with sql_controller as sql:
        table = "MyNewTable"
        sql.create_table(table, {"name": ("text",), "age": ("integer",)})
        assert sql.has_table(table)


def test_delete_table(sql_controller, table):
    with sql_controller as sql:
        sql.delete_table(table)
        assert not sql.has_table(table)


def test_add_row(sql_controller, table, person_data):
    with sql_controller as sql:
        sql.add_row(table, person_data)
        rows = sql.cursor.execute(f"select * from {table}")
        assert list(rows) == [person_data]


def test_add_rows(sql_controller, table, people_data):
    with sql_controller as sql:
        sql.add_rows(table, people_data)
        rows = sql.cursor.execute(f"select * from {table}")
        assert sorted(rows) == sorted(people_data)


def test_get_row(sql_controller, table, people_data, name_clause):
    with sql_controller as sql:
        sql.add_rows(table, people_data)
        row = sql.get_row(table, name_clause)
        assert row in people_data


def test_get_rows(sql_controller, table, people_data, age_clause):
    with sql_controller as sql:
        sql.add_rows(table, people_data)
        rows = sql.get_rows(table, age_clause)
        assert sorted(rows) == sorted(people_data)


def test_get_all_rows(sql_controller, table, people_data):
    with sql_controller as sql:
        sql.add_rows(table, people_data)
        rows = sql.get_all_rows(table)
        assert sorted(rows) == sorted(people_data)


def test_update_rows(sql_controller, table, people_data, people_data_older):
    with sql_controller as sql:
        sql.add_rows(table, people_data)
        sql.update_rows(table, valuedict)
        rows = sql.get_all_rows(table)
        assert sorted(rows) == sorted(people_data_older)


def test_delete_rows(sql_controller):
    with sql_controller as sql:
        with pytest.raises(NotImplementedError):
            sql.delete_rows("", [])


def test_delete_all_rows_empty(sql_controller, table, people_data):
    with sql_controller as sql:
        sql.delete_all_rows(table)


def test_delete_all_rows(sql_controller, table, people_data):
    with sql_controller as sql:
        sql.cursor.executemany(f"insert into {table} values (?,?,?)", people_data)
        sql.delete_all_rows(table)

        rows = sql.cursor.execute(f"select * from {table}")
        assert list(rows) == []
