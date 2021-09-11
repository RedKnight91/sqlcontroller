"""Test sqlcontroller module"""

import sqlite3
import pytest
from sqlcontroller.sqlcontroller import NonExistentTableError, SqliteController
from sqlcontroller.sqltable import SqliteTable
from sqlcontroller.sqlfield import Field
from sqlcontroller.sqlvalidator import InvalidSqlFieldError, InvalidSqlNameError

# pylint: disable=missing-function-docstring


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
        name = Field("name", "text", ["primary key"])
        age = Field("age", "integer", ["not null"])

        tbl = sql.create_table(table, [name, age])
        assert isinstance(tbl, SqliteTable)
        assert sql.has_table(table)


def test_create_table_fail_name(sql_controller):
    with sql_controller as sql:
        table = "_invalid table name_"
        with pytest.raises(InvalidSqlNameError):
            id_ = Field("ID", "text", ["primary key"])
            sql.create_table(table, [id_])

        assert not sql.has_table(table)


def test_create_table_fail_fields(sql_controller):
    with sql_controller as sql:
        table = "new"
        with pytest.raises(InvalidSqlFieldError):
            id_ = Field("I. D.", "text", ["unique"])
            sql.create_table(table, [id_])

        with pytest.raises(InvalidSqlFieldError):
            id_ = Field("ID", "string", ["unique"])
            sql.create_table(table, [id_])

        with pytest.raises(InvalidSqlFieldError):
            id_ = Field("ID", "text", ["none"])
            sql.create_table(table, [id_])

        assert not sql.has_table(table)


def test_get_table(sql_controller, table):
    with sql_controller as sql:
        tbl = sql.get_table(table)
        assert isinstance(tbl, SqliteTable)


def test_get_table_errors(
    sql_controller,
):
    with pytest.raises(InvalidSqlNameError):
        with sql_controller as sql:
            sql.get_table("_invalid table name_")

    with pytest.raises(NonExistentTableError):
        with sql_controller as sql:
            sql.get_table("nonexistent")


def test_delete_table(sql_controller, table):
    with sql_controller as sql:
        sql.delete_table(table)
        assert not sql.has_table(table)
