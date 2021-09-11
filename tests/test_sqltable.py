"""Test sqltable module"""

import sqlite3
import pytest

# pylint: disable=missing-function-docstring
# pytest: disable=too-many-arguments


def test_add_row(sql_table, person_fields, person_data):
    with sql_table as table:
        table.add_row(person_data, person_fields)
        rows = table.get_all_rows()
        assert list(rows) == [person_data]


def test_add_rows(sql_table, person_fields, people_data):
    with sql_table as table:
        table.add_rows(people_data, person_fields)
        rows = table.get_all_rows()
        assert sorted(rows) == sorted(people_data)


def test_get_row(sql_table, person_fields, people_data, name_clause):
    with sql_table as table:
        table.add_rows(people_data, person_fields)
        row = table.get_row(name_clause)
        assert row in people_data


def test_get_rows(sql_table, person_fields, people_data, older_clause):
    with sql_table as table:
        table.add_rows(people_data, person_fields)
        rows = table.get_rows(older_clause)
        assert sorted(rows) == sorted(people_data)


def test_get_all_rows(sql_table, person_fields, people_data):
    with sql_table as table:
        table.add_rows(people_data, person_fields)
        rows = table.get_all_rows()
        assert sorted(rows) == sorted(people_data)


def test_update_rows(
    sql_table, person_fields, people_data, age_increment, people_data_older
):
    with sql_table as table:
        table.add_rows(people_data, person_fields)
        table.update_rows(age_increment)

        rows = table.get_all_rows()
        assert sorted(rows) == sorted(people_data_older)


def test_update_rows_clause(
    sql_table,
    person_fields,
    people_data,
    age_increment,
    younger_clause,
    people_data_some_older,
):
    with sql_table as table:
        table.add_rows(people_data, person_fields)
        table.update_rows(age_increment, younger_clause)

        rows = table.get_all_rows()
        assert sorted(rows) == sorted(people_data_some_older)


def test_delete_rows_empty(sql_table, younger_clause):
    """Delete rows from empty table"""
    with sql_table as table:
        table.delete_rows(younger_clause)

        rows = table.get_all_rows()
        assert list(rows) == []


def test_delete_rows_clause(
    sql_table, people_data, younger_clause, people_older_result
):
    with sql_table as table:
        table.add_rows(people_data)
        table.delete_rows(younger_clause)

        rows = table.get_all_rows()
        assert list(rows) == people_older_result


def test_delete_rows_str_clause(sql_table, people_data, invalid_string_clause):
    with sql_table as table:
        with pytest.raises(sqlite3.OperationalError):
            table.add_rows(people_data)
            table.delete_rows(invalid_string_clause)


def test_delete_rows_invalid_clause(sql_table, people_data, invalid_clause):
    with sql_table as table:
        with pytest.raises(sqlite3.OperationalError):
            table.add_rows(people_data)
            table.delete_rows(invalid_clause)


def test_delete_all_rows_empty(sql_table):
    """Delete all rows from empty table"""
    with sql_table as table:
        table.delete_all_rows()

        rows = table.get_all_rows()
        assert list(rows) == []


def test_delete_all_rows(sql_table, people_data):
    with sql_table as table:
        table.add_rows(people_data)
        table.delete_all_rows()

        rows = table.get_all_rows()
        assert list(rows) == []
