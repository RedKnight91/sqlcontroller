"""SQL validation"""

import re
from abc import ABC
from typing import Any, Iterable
from sqlcontroller.field import Field


class InvalidAlphanumericError(Exception):
    """Error for invalid alphanumeric strings"""


class InvalidSqlDataTypeError(Exception):
    """Error for invalid SQL data types"""


class InvalidSqlDataConstraintError(Exception):
    """Error for invalid SQL data constraints"""


class InvalidSqlOperatorError(Exception):
    """Error for invalid SQL operators"""


class InvalidSqlOperandError(Exception):
    """Error for invalid SQL operands"""


class InvalidSqlNameError(Exception):
    """Error for invalid SQL names"""


class InvalidSqlFieldError(Exception):
    """Error for invalid SQL fields"""


math_comparison = {"=", "<>", "!=", "<", ">", "<=", ">="}
logic_comparison = {"BETWEEN", "EXISTS", "IN", "LIKE"}
types = {"NULL", "INTEGER", "REAL", "TEXT", "BLOB"}
bool_operators = {"ALL", "AND", "ANY", "OR", "NOT"}
constraints = {"NOT NULL", "UNIQUE", "PRIMARY KEY"}


def is_numeric(num: str) -> bool:
    """Return True if string only contains a number"""
    try:
        float(num)
        return True
    except ValueError:
        return False


class AbstractValidity(ABC):  # pylint: disable=too-few-public-methods
    """Abstract class for SQL Validity classes"""


class AbstractValidator(ABC):  # pylint: disable=too-few-public-methods
    """Abstract class for SQL Validator classes"""


class SqliteValidity(AbstractValidity):
    """Check valid SQL keywords, names, types and values"""

    alphanum = re.compile(r"[a-zA-Z0-9]+")
    alphanum_underscore = re.compile(r"[a-zA-Z0-9_]+")
    alphanum_space = re.compile(r"[a-zA-Z0-9\s]+")
    alphanum_underscore_space = re.compile(r"[a-zA-Z0-9\s_]+")

    @staticmethod
    def valid_iterable(var: Any) -> bool:
        """Validate an iterable type"""
        return hasattr(var, "__iter__") and not isinstance(var, str)

    @staticmethod
    def valid_str(var: Any) -> bool:
        """Validate a string type"""
        return isinstance(var, str)

    @staticmethod
    def valid_alphanum(
        name: str, underscore: bool = False, space: bool = False
    ) -> bool:
        """Validate alphanumeric strings (with/without underscores, spaces)"""
        if underscore and space:
            pattern = SqliteValidity.alphanum_underscore_space
        elif underscore:
            pattern = SqliteValidity.alphanum_underscore
        elif space:
            pattern = SqliteValidity.alphanum_space
        else:
            pattern = SqliteValidity.alphanum

        return isinstance(name, str) and bool(re.fullmatch(pattern, name))

    @staticmethod
    def valid_name(name: str) -> bool:
        """Validate sql name"""
        return SqliteValidity.valid_alphanum(name, True, False)

    @staticmethod
    def valid_type(name: str) -> bool:
        """Validate field type"""
        return isinstance(name, str) and name.upper() in types

    @staticmethod
    def valid_constraint(name: str) -> bool:
        """Validate field constraint"""
        return isinstance(name, str) and name.upper() in constraints

    @staticmethod
    def valid_field(field: Field) -> bool:
        """Validate field instance"""
        val = SqliteValidity

        valid_constraints_iterable = lambda: (
            val.valid_iterable(field.constraints)
            and all(list(map(val.valid_constraint, field.constraints)))
        )

        valid_constraints_str = lambda: (
            val.valid_str(field.constraints)
            and val.valid_constraint(field.constraints) # type: ignore
        )

        valid = (
            isinstance(field, Field)
            and val.valid_name(field.name)
            and val.valid_type(field.type)
            and (valid_constraints_iterable() or valid_constraints_str())
        )

        return valid

    @staticmethod
    def valid_values(values: Iterable) -> bool:
        """Validate values"""

        def is_valid(val: Any) -> bool:
            try:
                return isinstance(val, str) or is_numeric(val)
            except TypeError:
                return False

        return all(map(is_valid, values))

    @staticmethod
    def valid_comparison_operator(comp_op: str) -> bool:
        """Validate comparison operator"""
        return comp_op.upper() in {*math_comparison, *logic_comparison}

    @staticmethod
    def valid_bool_operator(bool_op: str) -> bool:
        """Validate bool operator"""
        return bool_op.upper() in bool_operators


class SqliteValidator(AbstractValidator):
    """Validate SQL keywords, names, types and values"""

    @staticmethod
    def validate_iterable(var: Any) -> None:
        """Validate iterable type"""
        if not SqliteValidity.valid_iterable(var):
            raise TypeError(f"{var} is not iterable")

    @staticmethod
    def validate_str(var: Any) -> None:
        """Validate string type"""
        if not SqliteValidity.valid_str(var):
            raise TypeError(f"{var} is not a string")

    @staticmethod
    def validate_alphanum(
        str_: str, underscore: bool = False, space: bool = False
    ) -> None:
        """Validate alphanumeric string"""
        SqliteValidator.validate_str(str_)

        if not SqliteValidity.valid_alphanum(str_, underscore, space):
            error = f"Not alphanumeric: {str_}"
            raise InvalidAlphanumericError(error)

    @staticmethod
    def validate_table_name(name: str) -> None:
        """Validate field name"""
        if not SqliteValidity.valid_name(name):
            error = f"{name} is not a valid table name."
            raise InvalidSqlNameError(error)

    @staticmethod
    def validate_field_name(field: str) -> None:
        """Validate field name"""
        if not SqliteValidity.valid_name(field):
            error = f"{field} is not a valid field name."
            raise InvalidSqlOperandError(error)

    @staticmethod
    def validate_type(name: str) -> None:
        """Validate field type"""
        SqliteValidator.validate_str(name)

        if not SqliteValidity.valid_type(name):
            error = f"{name} is not a valid field type."
            raise InvalidSqlDataTypeError(error)

    @staticmethod
    def validate_constraint(name: str) -> None:
        """Validate field constraint"""
        SqliteValidator.validate_str(name)

        name = name.upper()

        if not SqliteValidity.valid_constraint(name):
            error = f"{name} is not a valid field constraint."
            raise InvalidSqlDataConstraintError(error)

    @staticmethod
    def validate_field(field: Field) -> None:
        """Validate Field instance"""
        if not SqliteValidity.valid_field(field):
            error = f"{field} is not a valid field."
            raise InvalidSqlFieldError(error)

    @staticmethod
    def validate_table_params(name: str, fields: Iterable[Field]) -> None:
        """Validate table name and fields"""
        SqliteValidator.validate_table_name(name)

        # In case a single field is passed
        if isinstance(fields, Field):
            fields = [fields]

        for field in fields:
            SqliteValidator.validate_field(field)

    @staticmethod
    def validate_values(values: str) -> None:
        """Validate field values"""
        if not SqliteValidity.valid_values(values):
            error = f"{values} is not a valid clause values operand."
            raise InvalidSqlOperandError(error)

    @staticmethod
    def validate_comparison_operator(comp_op: str) -> None:
        """Validate clause comparison operator"""
        if not SqliteValidity.valid_comparison_operator(comp_op):
            error = f"{comp_op} is not a valid comparison operator."
            raise InvalidSqlOperatorError(error)

    @staticmethod
    def validate_bool_operator(bool_op: str) -> None:
        """Validate clause boolean operator"""
        if not SqliteValidity.valid_bool_operator(bool_op):
            error = f"{bool_op} is not a valid boolean operator."
            raise InvalidSqlOperatorError(error)

    @staticmethod
    def validate_condition(field: str, operator: str, values: str) -> None:
        """Validate clause condition"""
        SqliteValidator.validate_field_name(field)
        SqliteValidator.validate_comparison_operator(operator)
        SqliteValidator.validate_values(values)
