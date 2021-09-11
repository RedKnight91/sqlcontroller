"""Test sqlvalidity module"""

import pytest
from sqlcontroller.sqlfield import Field
from sqlcontroller.sqlvalidator import SqliteValidity


# pylint: disable=missing-function-docstring
# pylint: disable=too-many-function-args


def test_valid_alphanum_true():
    assert SqliteValidity.valid_alphanum("Hello")
    assert SqliteValidity.valid_alphanum("H3110")
    assert SqliteValidity.valid_alphanum("my_name", underscore=True)
    assert SqliteValidity.valid_alphanum("my name", space=True)
    assert SqliteValidity.valid_alphanum("_my name_", True, True)


def test_valid_alphanum_false():
    assert not SqliteValidity.valid_alphanum("%myname%")
    assert not SqliteValidity.valid_alphanum("my name", underscore=True)
    assert not SqliteValidity.valid_alphanum("my_name", space=True)
    assert not SqliteValidity.valid_alphanum("%my name%", True, True)


def test_valid_name():
    assert SqliteValidity.valid_name("Joe_Black")


def test_valid_name_false():
    assert not SqliteValidity.valid_name("Joe Black")


def test_valid_type():
    assert SqliteValidity.valid_type("integer")
    assert SqliteValidity.valid_type("tExT")


def test_valid_type_false():
    assert not SqliteValidity.valid_type("json")


def test_valid_constraint():
    assert SqliteValidity.valid_constraint("not null")
    assert SqliteValidity.valid_constraint("Primary Key")


def test_valid_constraint_false():
    assert not SqliteValidity.valid_constraint("notnull")


def test_valid_field():
    assert SqliteValidity.valid_field(Field("ID", "text", ["not null", "unique"]))
    assert SqliteValidity.valid_field(Field("name", "text", "unique"))


def test_valid_field_false():
    assert not SqliteValidity.valid_field(Field("name", "string", ["unique"]))


def test_valid_values():
    assert SqliteValidity.valid_values(["Joe", 33, "Senior dev"])
    assert SqliteValidity.valid_values(("Joe", 33, "Senior dev"))


def test_valid_values_false():
    assert not SqliteValidity.valid_values([dict(a=1, b=2)])


def test_valid_values_error():
    with pytest.raises(TypeError):
        SqliteValidity.valid_values(1)

    with pytest.raises(TypeError):
        SqliteValidity.valid_values(1, 2, 3)


def test_valid_comparison_operator():
    assert SqliteValidity.valid_comparison_operator("IN")
    assert SqliteValidity.valid_comparison_operator("between")


def test_valid_comparison_operator_false():
    assert not SqliteValidity.valid_comparison_operator("sameas")


def test_valid_bool_operator():
    assert SqliteValidity.valid_bool_operator("or")
    assert SqliteValidity.valid_bool_operator("AND")


def test_valid_bool_operator_false():
    assert not SqliteValidity.valid_bool_operator("xor")


def test_valid_iterable():
    assert SqliteValidity.valid_iterable((1,))
    assert SqliteValidity.valid_iterable([1])
    assert SqliteValidity.valid_iterable({1})


def test_valid_iterable_false():
    assert not SqliteValidity.valid_iterable(33)


def test_valid_str():
    assert SqliteValidity.valid_str("1")
    assert SqliteValidity.valid_str("abc")


def test_valid_str_false():
    assert not SqliteValidity.valid_str([])
