import pytest
from sqlcontroller.sqlvalidator import SqlValidator
from sqlcontroller.sqlvalidator import (
    InvalidAlphanumericError,
    InvalidSqlDataConstraintError,
    InvalidSqlDataTypeError,
)


def test_validate_alphanum():
    validate_alphanum = SqlValidator.validate_alphanum

    assert validate_alphanum("Hello") is None
    assert validate_alphanum("H3110") is None
    assert validate_alphanum("A") is None
    assert validate_alphanum("3") is None


def test_validate_alphanum_errors():
    validate_alphanum = SqlValidator.validate_alphanum

    with pytest.raises(InvalidAlphanumericError):
        validate_alphanum("Hel_o")

    with pytest.raises(InvalidAlphanumericError):
        validate_alphanum("_")

    with pytest.raises(TypeError):
        validate_alphanum(3)


def test_validate_type():
    validate_type = SqlValidator.validate_type

    assert validate_type("TEXT") is None
    assert validate_type("Text") is None
    assert validate_type("text") is None
    assert validate_type("real") is None


def test_validate_type_errors():
    validate_type = SqlValidator.validate_type

    with pytest.raises(InvalidSqlDataTypeError):
        validate_type("Hello")

    with pytest.raises(InvalidSqlDataTypeError):
        validate_type("TEXT.")

    with pytest.raises(TypeError):
        validate_type(0)


def test_validate_constraint():
    validate_constraint = SqlValidator.validate_constraint

    assert validate_constraint("NOT NULL") is None
    assert validate_constraint("not null") is None
    assert validate_constraint("unique") is None


def test_validate_constraint_errors():
    validate_constraint = SqlValidator.validate_constraint

    with pytest.raises(InvalidSqlDataConstraintError):
        validate_constraint("primary")

    with pytest.raises(InvalidSqlDataConstraintError):
        validate_constraint("PRIMARY_KEY")

    with pytest.raises(TypeError):
        validate_constraint(0)


def test_validate_iterable():
    validate_iterable = SqlValidator.validate_iterable

    assert validate_iterable([]) is None
    assert validate_iterable("") is None


def test_validate_iterable_errors():
    validate_iterable = SqlValidator.validate_iterable

    with pytest.raises(TypeError):
        validate_iterable(3)
