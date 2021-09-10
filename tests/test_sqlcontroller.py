from sqlcontroller.sqlvalidator import InvalidSqlNameError
import pytest
import sqlite3
from sqlcontroller.sqlcontroller import SqliteController


def test_init(database):
    sql = SqliteController(database)

    assert sql.database == database
    assert not hasattr(sql, "connection")
    assert not hasattr(sql, "cursor")


def test_contextmanager(database):
    with SqliteController(database) as sql:
        assert isinstance(sql, SqliteController)
        assert sql.database == database
        assert isinstance(sql.connection, sqlite3.Connection)
        assert isinstance(sql.cursor, sqlite3.Cursor)

    assert not hasattr(sql, "connection")
    assert not hasattr(sql, "cursor")


def test_execute(sql_controller, table, person_data):
    with sql_controller as sql:
        sql.execute("insert into {table} values (?,?,?)", table, person_data)
        rows = sql.execute("select * from {table}", table)
        assert list(rows) == [person_data]


def test_execute_fail(sql_controller):
    with sql_controller as sql:
        with pytest.raises(Exception):
            sql.execute("select from {table}", "NoTable")


def test_executemany(sql_controller, table, people_data):
    with sql_controller as sql:
        sql.executemany("insert into {table} values (?,?,?)", table, people_data)
        rows = sql.execute("select * from {table}", table)
        assert list(rows) == people_data


def test_executemany_fail(sql_controller, table, person_data):
    with sql_controller as sql:
        with pytest.raises(sqlite3.ProgrammingError):
            sql.executemany("insert into {table} values (?,?,?)", table, [])

        with pytest.raises(sqlite3.ProgrammingError):
            sql.executemany("insert into {table} values (?,?,?)", table, "Joe")

        with pytest.raises(sqlite3.ProgrammingError):
            sql.executemany("insert into {table} values (?,?,?)", table, person_data)


def test_connect_db(sql_controller):
    with sql_controller as sql:
        connection = sql.connect_db()
        assert isinstance(connection, sqlite3.Connection)
        assert sql.connection is connection


def test_disconnect_db(sql_controller):
    with sql_controller as sql:
        sql.disconnect_db()
        assert not hasattr(sql, "connection")
        assert not hasattr(sql, "cursor")


def test_save_db(sql_controller, table, person_fields, person_data):
    with sql_controller as sql:
        sqltable = sql.get_table(table)
        sqltable.add_row(person_data, person_fields)
        sql.save_db()

        sql.connection.close()
        sql.__enter__()

        rows = sql.cursor.execute(f"select * from {table}")
        assert list(rows) == [person_data]


def test_not_save_db(sql_controller, table, person_fields, person_data):
    with sql_controller as sql:
        sqltable = sql.get_table(table)
        sqltable.add_row(person_data, person_fields)

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
        sql.create_table(table, {"name": ("text", "primary key"), "age": ("integer", "not null")})
        assert sql.has_table(table)

def test_create_table_fail(sql_controller):
    with sql_controller as sql:
        table = "_invalid table name_"
        with pytest.raises(InvalidSqlNameError):
            sql.create_table(table, {"name": ("text",), "age": ("integer",)})

        assert not sql.has_table(table)


def test_delete_table(sql_controller, table):
    with sql_controller as sql:
        sql.delete_table(table)
        assert not sql.has_table(table)
