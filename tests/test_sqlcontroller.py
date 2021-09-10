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
        assert not hasattr(sql, "connection")
        assert not hasattr(sql, "cursor")


def test_save_db(sql_controller, table, person_fields, person_data):
    with sql_controller as sql:
        sql.add_row(table, person_data, person_fields)
        sql.save_db()

        sql.connection.close()
        sql.__enter__()

        rows = sql.cursor.execute(f"select * from {table}")
        assert list(rows) == [person_data]


def test_not_save_db(sql_controller, table, person_fields, person_data):
    with sql_controller as sql:
        sql.add_row(table, person_data, person_fields)

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


def test_add_row(sql_controller, table, person_fields, person_data):
    with sql_controller as sql:
        sql.add_row(table, person_data, person_fields)
        rows = sql.cursor.execute(f"select * from {table}")
        assert list(rows) == [person_data]


def test_add_rows(sql_controller, table, person_fields, people_data):
    with sql_controller as sql:
        sql.add_rows(table, people_data, person_fields)
        rows = sql.cursor.execute(f"select * from {table}")
        assert sorted(rows) == sorted(people_data)


def test_get_row(sql_controller, table, person_fields, people_data, name_clause):
    with sql_controller as sql:
        sql.add_rows(table, people_data, person_fields)
        row = sql.get_row(table, name_clause)
        assert row in people_data


def test_get_rows(sql_controller, table, person_fields, people_data, older_clause):
    with sql_controller as sql:
        sql.add_rows(table, people_data, person_fields)
        rows = sql.get_rows(table, older_clause)
        assert sorted(rows) == sorted(people_data)


def test_get_all_rows(sql_controller, table, person_fields, people_data):
    with sql_controller as sql:
        sql.add_rows(table, people_data, person_fields)
        rows = sql.get_all_rows(table)
        assert sorted(rows) == sorted(people_data)


def test_update_rows(
    sql_controller, table, person_fields, people_data, age_increment, people_data_older
):
    with sql_controller as sql:
        sql.add_rows(table, people_data, person_fields)
        sql.update_rows(table, age_increment)

        rows = sql.get_all_rows(table)
        assert sorted(rows) == sorted(people_data_older)


def test_update_rows_clause(
    sql_controller,
    table,
    person_fields,
    people_data,
    age_increment,
    younger_clause,
    people_data_some_older,
):
    with sql_controller as sql:
        sql.add_rows(table, people_data, person_fields)
        sql.update_rows(table, age_increment, younger_clause)

        rows = sql.get_all_rows(table)
        assert sorted(rows) == sorted(people_data_some_older)


def test_delete_rows_empty(sql_controller, table, younger_clause):
    """Delete rows from empty table"""
    with sql_controller as sql:
        sql.delete_rows(table, younger_clause)

        rows = sql.cursor.execute(f"select * from {table}")
        assert list(rows) == []


def test_delete_rows_clause(
    sql_controller, table, people_data, younger_clause, people_older_result
):
    with sql_controller as sql:
        sql.cursor.executemany(f"insert into {table} values (?,?,?)", people_data)
        sql.delete_rows(table, younger_clause)

        rows = sql.cursor.execute(f"select * from {table}")
        assert list(rows) == people_older_result


def test_delete_rows_str_clause(sql_controller, table, people_data, invalid_string_clause):
    with sql_controller as sql:
        with pytest.raises(sqlite3.OperationalError):
            sql.cursor.executemany(f"insert into {table} values (?,?,?)", people_data)
            sql.delete_rows(table, invalid_string_clause)


def test_delete_rows_invalid_clause(sql_controller, table, people_data, invalid_clause):
    with sql_controller as sql:
        with pytest.raises(sqlite3.OperationalError):

            sql.cursor.executemany(f"insert into {table} values (?,?,?)", people_data)
            sql.delete_rows(table, invalid_clause)


def test_delete_all_rows_empty(sql_controller, table):
    """Delete all rows from empty table"""
    with sql_controller as sql:
        sql.delete_all_rows(table)

        rows = sql.cursor.execute(f"select * from {table}")
        assert list(rows) == []


def test_delete_all_rows(sql_controller, table, people_data):
    with sql_controller as sql:
        sql.cursor.executemany(f"insert into {table} values (?,?,?)", people_data)
        sql.delete_all_rows(table)

        rows = sql.cursor.execute(f"select * from {table}")
        assert list(rows) == []
