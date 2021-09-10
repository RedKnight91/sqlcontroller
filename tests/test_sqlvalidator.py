import pytest
from sqlcontroller import sqlcontroller, sqlvalidator
from sqlcontroller.sqlvalidator import InvalidSqlOperandError, InvalidSqlOperatorError, SqliteValidator
from sqlcontroller.sqlvalidator import is_numeric
from sqlcontroller.sqlvalidator import (
    InvalidAlphanumericError,
    InvalidSqlDataConstraintError,
    InvalidSqlDataTypeError,
)


def test_is_numeric():
    assert is_numeric("-3.6")
    assert not is_numeric("-3.6 people")

def test_is_numeric_fail():
    with pytest.raises(TypeError):
        assert is_numeric([])

def test_valid_alphanum_true():
    valid_alphanum = SqliteValidator.valid_alphanum

    assert valid_alphanum("Hello")
    assert valid_alphanum("H3110")
    assert valid_alphanum("my_name", underscore=True)
    assert valid_alphanum("my name", space=True)
    assert valid_alphanum("_my name_", True, True)

def test_valid_alphanum_false():
    valid_alphanum = SqliteValidator.valid_alphanum

    assert not valid_alphanum("%myname%")
    assert not valid_alphanum("my name", underscore=True)
    assert not valid_alphanum("my_name", space=True)
    assert not valid_alphanum("%my name%", True, True)

def test_valid_name():
    valid_name = SqliteValidator.valid_name
    assert valid_name('Joe_Black')
    assert not valid_name('Joe Black')

def test_valid_type():
    valid_type = SqliteValidator.valid_type
    assert valid_type('integer')
    assert valid_type('tExT')
    assert not valid_type('json')

def test_valid_constraint():
    valid_constraint = SqliteValidator.valid_constraint
    assert valid_constraint('not null')
    assert valid_constraint('Primary Key')
    assert not valid_constraint('notnull')

def test_valid_values():
    valid_values = SqliteValidator.valid_values
    assert valid_values(['Joe', 33, 'Senior dev'])
    assert valid_values(('Joe', 33, 'Senior dev'))
    assert not valid_values([dict(a=1,b=2)])

def test_valid_values_error():
    with pytest.raises(TypeError):
        SqliteValidator.valid_values(1)

    with pytest.raises(TypeError):
        SqliteValidator.valid_values(1, 2, 3)

def test_valid_comparison_operator():
    valid_comparison_operator = SqliteValidator.valid_comparison_operator

    assert valid_comparison_operator('IN')
    assert valid_comparison_operator('between')
    assert not valid_comparison_operator('sameas')

def test_valid_bool_operator():
    valid_bool_operator = SqliteValidator.valid_bool_operator

    assert valid_bool_operator('or')
    assert valid_bool_operator('AND')
    assert not valid_bool_operator('xor')

def test_valid_iterable():
    valid_iterable = SqliteValidator.valid_iterable

    assert valid_iterable((1,))
    assert valid_iterable([1])
    assert valid_iterable({1})
    assert valid_iterable("test")
    assert not valid_iterable(33)

def test_valid_str():
    valid_str = SqliteValidator.valid_str

    assert valid_str("1")
    assert valid_str("abc")
    assert not valid_str([])


def test_validate_alphanum():
    validate_alphanum = SqliteValidator.validate_alphanum

    assert validate_alphanum("Hello") is None
    assert validate_alphanum("H3110") is None
    assert validate_alphanum("my_name", underscore=True) is None
    assert validate_alphanum("my name", space=True) is None
    assert validate_alphanum("_my name_", True, True) is None


def test_validate_alphanum_errors():
    validate_alphanum = SqliteValidator.validate_alphanum

    with pytest.raises(InvalidAlphanumericError):
        validate_alphanum("Hel_o")

    with pytest.raises(InvalidAlphanumericError):
        validate_alphanum("my name", underscore=True)

    with pytest.raises(InvalidAlphanumericError):
        validate_alphanum("my_name", space=True)

    with pytest.raises(InvalidAlphanumericError):
        validate_alphanum("?%my^^name%?", True, True)

    with pytest.raises(TypeError):
        validate_alphanum(3)


def test_validate_field_name():
    """Test that name contains only alphanumeric or underscore"""
    assert SqliteValidator.validate_field_name('my_table') is None

def test_validate_field_name():
    with pytest.raises(InvalidSqlOperandError):
        SqliteValidator.validate_field_name('%my_table%')


def test_validate_type():
    validate_type = SqliteValidator.validate_type

    assert validate_type("TEXT") is None
    assert validate_type("Text") is None
    assert validate_type("text") is None
    assert validate_type("real") is None


def test_validate_type_errors():
    validate_type = SqliteValidator.validate_type

    with pytest.raises(InvalidSqlDataTypeError):
        validate_type("Hello")

    with pytest.raises(InvalidSqlDataTypeError):
        validate_type("TEXT.")

    with pytest.raises(TypeError):
        validate_type(0)


def test_validate_constraint():
    validate_constraint = SqliteValidator.validate_constraint

    assert validate_constraint("NOT NULL") is None
    assert validate_constraint("not null") is None
    assert validate_constraint("unique") is None


def test_validate_constraint_errors():
    validate_constraint = SqliteValidator.validate_constraint

    with pytest.raises(InvalidSqlDataConstraintError):
        validate_constraint("primary")

    with pytest.raises(InvalidSqlDataConstraintError):
        validate_constraint("PRIMARY_KEY")

    with pytest.raises(TypeError):
        validate_constraint(0)


def test_validate_iterable():
    validate_iterable = SqliteValidator.validate_iterable

    assert validate_iterable([]) is None
    assert validate_iterable("") is None


def test_validate_iterable_errors():
    validate_iterable = SqliteValidator.validate_iterable

    with pytest.raises(TypeError):
        validate_iterable(3)

def test_validate_values():
    validate_values = SqliteValidator.validate_values
    assert validate_values(['Joe', 33, 'Senior dev']) is None
    assert validate_values(('Joe', 33, 'Senior dev')) is None

def test_validate_values_erro():
    with pytest.raises(InvalidSqlOperandError):
        SqliteValidator.validate_values([dict(a=1,b=2)])

def test_validate_comparison_operator():
    validate_comparison_operator = SqliteValidator.validate_comparison_operator

    assert validate_comparison_operator('IN') is None
    assert validate_comparison_operator('between') is None

    
def test_validate_comparison_operator_error():
    with pytest.raises(InvalidSqlOperatorError):
        SqliteValidator.validate_comparison_operator('sameas')

def test_validate_bool_operator():
    validate_bool_operator = SqliteValidator.validate_bool_operator

    assert validate_bool_operator('or') is None
    assert validate_bool_operator('AND') is None

def test_validate_bool_operator_error():    
    with pytest.raises(InvalidSqlOperatorError):
        SqliteValidator.validate_bool_operator('xor')

def test_validate_condition():
    validate_condition = SqliteValidator.validate_condition

    assert validate_condition('name', '=', 'Joe') is None

def test_validate_condition_errors():
    validate_condition = SqliteValidator.validate_condition

    with pytest.raises(InvalidSqlOperandError):
        validate_condition('My name', '=', 'Joe')

    with pytest.raises(InvalidSqlOperatorError):
        validate_condition('name', 'lookslike', 'Joe')

    with pytest.raises(InvalidSqlOperandError):
        validate_condition('name', '=', [dict()])
        
    with pytest.raises(TypeError):
        validate_condition('name', '=', 3)