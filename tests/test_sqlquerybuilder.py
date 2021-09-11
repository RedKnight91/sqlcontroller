"""Test sqlquerybuilder module"""

from sqlcontroller.sqlfield import Field
from sqlcontroller.sqlquerybuilder import SqliteQueryBuilder


# pylint: disable=missing-function-docstring


def test_build_table_create_query():
    fields = [
        Field("name", "integer", ["primary key"]),
        Field("age", "integer", ["not null", "unique"]),
    ]
    query = "create table if not exists {table} "\
            "(name integer primary key, age integer not null unique);"
    assert SqliteQueryBuilder.build_table_create_query(fields) == query


def test_build_insert_query():
    fields = ("name", "age")
    values = ("Joe", 33)

    query = "insert into {table} (name,age) values (?,?);"
    assert SqliteQueryBuilder.build_insert_query(values, fields) == query

    query = "insert into {table} values (?,?);"
    assert SqliteQueryBuilder.build_insert_query(values) == query


def test_build_query_clauses():
    where = "where name = 'Mia'"
    where_bare = "name = 'Mia'"
    order = "age desc"
    limit = 10
    offset = 3

    query = "where name = 'Mia'; order by age desc; limit 10; offset 3;"
    assert SqliteQueryBuilder.build_query_clauses(where, order, limit, offset) == query

    query = "where name = 'Mia'; order by age desc; limit 10; offset 3;"
    assert (
        SqliteQueryBuilder.build_query_clauses(where_bare, order, limit, offset)
        == query
    )

    query = "where name = 'Mia'; order by age desc; limit 10;"
    assert SqliteQueryBuilder.build_query_clauses(where, order, limit) == query

    query = "where name = 'Mia'; order by age desc;"
    assert SqliteQueryBuilder.build_query_clauses(where, order) == query

    query = "where name = 'Mia';"
    assert SqliteQueryBuilder.build_query_clauses(where) == query

    query = ""
    assert SqliteQueryBuilder.build_query_clauses("") == query
