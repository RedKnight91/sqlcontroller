"""Test sqlvalidator module"""

import pytest
from sqlcontroller.sqlfield import Field
from sqlcontroller.sqlvalidator import (
    is_numeric,
    InvalidSqlFieldError,
    InvalidSqlNameError,
    InvalidSqlOperandError,
    InvalidSqlOperatorError,
    SqliteValidator,
    InvalidAlphanumericError,
    InvalidSqlDataConstraintError,
    InvalidSqlDataTypeError,
)


# pylint: disable=missing-function-docstring


def test_is_numeric():
    assert is_numeric("-3.6")
    assert not is_numeric("-3.6 people")


def test_is_numeric_fail():
    with pytest.raises(TypeError):
        assert is_numeric([])


def test_validate_alphanum():
    SqliteValidator.validate_alphanum("Hello")
    SqliteValidator.validate_alphanum("H3110")
    SqliteValidator.validate_alphanum("my_name", underscore=True)
    SqliteValidator.validate_alphanum("my name", space=True)
    SqliteValidator.validate_alphanum("_my name_", True, True)


def test_validate_alphanum_errors():
    with pytest.raises(InvalidAlphanumericError):
        SqliteValidator.validate_alphanum("Hel_o")

    with pytest.raises(InvalidAlphanumericError):
        SqliteValidator.validate_alphanum("my name", underscore=True)

    with pytest.raises(InvalidAlphanumericError):
        SqliteValidator.validate_alphanum("my_name", space=True)

    with pytest.raises(InvalidAlphanumericError):
        SqliteValidator.validate_alphanum("?%my^^name%?", True, True)

    with pytest.raises(TypeError):
        SqliteValidator.validate_alphanum(3)


def test_validate_field_name():
    """Test that name contains only alphanumeric or underscore"""
    SqliteValidator.validate_field_name("my_table")


def test_validate_field_name_fail():
    with pytest.raises(InvalidSqlOperandError):
        SqliteValidator.validate_field_name("%my_table%")


def test_validate_type():
    SqliteValidator.validate_type("TEXT")
    SqliteValidator.validate_type("Text")
    SqliteValidator.validate_type("text")
    SqliteValidator.validate_type("real")


def test_validate_type_errors():
    with pytest.raises(InvalidSqlDataTypeError):
        SqliteValidator.validate_type("Hello")

    with pytest.raises(InvalidSqlDataTypeError):
        SqliteValidator.validate_type("TEXT.")

    with pytest.raises(TypeError):
        SqliteValidator.validate_type(0)


def test_validate_constraint():
    SqliteValidator.validate_constraint("NOT NULL")
    SqliteValidator.validate_constraint("not null")
    SqliteValidator.validate_constraint("unique")


def test_validate_constraint_errors():
    with pytest.raises(InvalidSqlDataConstraintError):
        SqliteValidator.validate_constraint("primary")

    with pytest.raises(InvalidSqlDataConstraintError):
        SqliteValidator.validate_constraint("PRIMARY_KEY")

    with pytest.raises(TypeError):
        SqliteValidator.validate_constraint(0)


def test_validate_field():
    SqliteValidator.validate_field(Field("ID", "text", ["not null", "unique"]))


def test_validate_field_error():
    with pytest.raises(InvalidSqlFieldError):
        SqliteValidator.validate_field(Field("name", "string", ["unique"]))


def test_validate_table_params(table, field, fields):
    SqliteValidator.validate_table_params(table, field)
    SqliteValidator.validate_table_params(table, fields)


def test_validate_table_params_errors(table, fields):
    with pytest.raises(InvalidSqlNameError):
        SqliteValidator.validate_table_params("_invalid table name_", fields)

    with pytest.raises(InvalidSqlFieldError):
        SqliteValidator.validate_table_params(table, "name")

    with pytest.raises(InvalidSqlFieldError):
        SqliteValidator.validate_table_params(table, ["name", "age"])

    with pytest.raises(InvalidSqlFieldError):
        SqliteValidator.validate_table_params(
            table, [Field("name", "string", "unique")]
        )


def test_validate_iterable():
    SqliteValidator.validate_iterable([])
    SqliteValidator.validate_iterable({})


def test_validate_iterable_errors():
    with pytest.raises(TypeError):
        SqliteValidator.validate_iterable(3)


def test_validate_values():
    SqliteValidator.validate_values(["Joe", 33, "Senior dev"])
    SqliteValidator.validate_values(("Joe", 33, "Senior dev"))


def test_validate_values_erro():
    with pytest.raises(InvalidSqlOperandError):
        SqliteValidator.validate_values([dict(a=1, b=2)])


def test_validate_comparison_operator():
    SqliteValidator.validate_comparison_operator("IN")
    SqliteValidator.validate_comparison_operator("between")


def test_validate_comparison_operator_error():
    with pytest.raises(InvalidSqlOperatorError):
        SqliteValidator.validate_comparison_operator("sameas")


def test_validate_bool_operator():
    SqliteValidator.validate_bool_operator("or")
    SqliteValidator.validate_bool_operator("AND")


def test_validate_bool_operator_error():
    with pytest.raises(InvalidSqlOperatorError):
        SqliteValidator.validate_bool_operator("xor")


def test_validate_condition():
    SqliteValidator.validate_condition("name", "=", "Joe")


def test_validate_condition_errors():
    with pytest.raises(InvalidSqlOperandError):
        SqliteValidator.validate_condition("My name", "=", "Joe")

    with pytest.raises(InvalidSqlOperatorError):
        SqliteValidator.validate_condition("name", "lookslike", "Joe")

    with pytest.raises(InvalidSqlOperandError):
        SqliteValidator.validate_condition("name", "=", [{}])

    with pytest.raises(TypeError):
        SqliteValidator.validate_condition("name", "=", 3)
