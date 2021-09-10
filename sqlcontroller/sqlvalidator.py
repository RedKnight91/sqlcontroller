"""SQL validation"""

from abc import ABC
import re
from typing import Any, Iterable


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


class AbstractValidator(ABC):  # pylint: disable=too-few-public-methods
    """Abstract class for SQL Validators"""


class SqliteValidator(AbstractValidator):
    """Validate SQL keywords, values and clauses"""

    alphanum = re.compile(r"[a-zA-Z0-9]+")
    alphanum_underscore = re.compile(r"[a-zA-Z0-9_]+")
    alphanum_space = re.compile(r"[a-zA-Z0-9\s]+")
    alphanum_underscore_space = re.compile(r"[a-zA-Z0-9\s_]+")

    @staticmethod
    def valid_iterable(var: Any) -> bool:
        """Validate an iterable type"""
        return hasattr(var, "__iter__")

    @staticmethod
    def valid_str(var: Any) -> bool:
        """Validate a string type"""
        return isinstance(var, str)

    @staticmethod
    def valid_alphanum(name, underscore: bool = False, space: bool = False) -> bool:
        """Validate alphanumeric strings (with/without underscores, spaces)"""
        if underscore and space:
            pattern = SqliteValidator.alphanum_underscore_space
        elif underscore:
            pattern = SqliteValidator.alphanum_underscore
        elif space:
            pattern = SqliteValidator.alphanum_space
        else:
            pattern = SqliteValidator.alphanum

        return isinstance(name, str) and bool(re.fullmatch(pattern, name))

    @staticmethod
    def valid_name(field: str) -> bool:
        """Validate sql name"""
        return SqliteValidator.valid_alphanum(field, True, False)

    @staticmethod
    def valid_type(name) -> bool:
        """Validate field type"""
        return isinstance(name, str) and name.upper() in types

    @staticmethod
    def valid_constraint(name) -> bool:
        """Validate field constraint"""
        return isinstance(name, str) and name.upper() in constraints

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

    @staticmethod
    def validate_iterable(var: Any) -> None:
        """Validate iterable type"""
        if not SqliteValidator.valid_iterable(var):
            raise TypeError(f"{var} is not iterable")

    @staticmethod
    def validate_str(var: Any) -> None:
        """Validate string type"""
        if not SqliteValidator.valid_str(var):
            raise TypeError(f"{var} is not a string")

    @staticmethod
    def validate_alphanum(
        str_: str, underscore: bool = False, space: bool = False
    ) -> None:
        """Validate alphanumeric string"""
        SqliteValidator.validate_str(str_)

        if not SqliteValidator.valid_alphanum(str_, underscore, space):
            error = f"Not alphanumeric: {str_}"
            raise InvalidAlphanumericError(error)

    @staticmethod
    def validate_table_name(name: str) -> None:
        """Validate field name"""
        if not SqliteValidator.valid_name(name):
            error = f"{name} is not a valid table name."
            raise InvalidSqlNameError(error)

    @staticmethod
    def validate_field_name(field: str) -> None:
        """Validate field name"""
        if not SqliteValidator.valid_name(field):
            error = f"{field} is not a valid field name."
            raise InvalidSqlOperandError(error)

    @staticmethod
    def validate_type(name) -> None:
        """Validate field type"""
        SqliteValidator.validate_str(name)

        if not SqliteValidator.valid_type(name):
            error = f"{name} is not a valid field type."
            raise InvalidSqlDataTypeError(error)

    @staticmethod
    def validate_constraint(name) -> None:
        """Validate field constraint"""
        SqliteValidator.validate_str(name)

        name = name.upper()

        if not SqliteValidator.valid_constraint(name):
            error = f"{name} is not a valid field constraint."
            raise InvalidSqlDataConstraintError(error)

    @staticmethod
    def validate_values(values: str) -> None:
        """Validate field values"""
        if not SqliteValidator.valid_values(values):
            error = f"{values} is not a valid clause values operand."
            raise InvalidSqlOperandError(error)

    @staticmethod
    def validate_comparison_operator(comp_op) -> None:
        """Validate clause comparison operator"""
        if not SqliteValidator.valid_comparison_operator(comp_op):
            error = f"{comp_op} is not a valid comparison operator."
            raise InvalidSqlOperatorError(error)

    @staticmethod
    def validate_bool_operator(bool_op) -> None:
        """Validate clause boolean operator"""
        if not SqliteValidator.valid_bool_operator(bool_op):
            error = f"{bool_op} is not a valid boolean operator."
            raise InvalidSqlOperatorError(error)

    @staticmethod
    def validate_condition(field: str, operator: str, values: str) -> None:
        """Validate clause condition"""
        SqliteValidator.validate_field_name(field)
        SqliteValidator.validate_comparison_operator(operator)
        SqliteValidator.validate_values(values)

    # @staticmethod
    # def validate_where_clause(clause) -> None:
    #     words = deque(re.sub("[()]", "", clause).split())

    #     if words[0].upper() == "WHERE":
    #         words.popleft()

    #     Condition = namedtuple('Condition', ('field', 'operator', 'values'))
    #     found_conditions, found_bools = [], []

    #     def get_condition_values():
    #         while words and words[0].upper() not in bool_operators:
    #             values.append(words.popleft().replace(',',''))

    #     def get_bool_operators():
    #         while words and words[0].upper() in bool_operators:
    #             found_bools.append(words.popleft())

    #     while words:
    #         field, operator, values = words.popleft(), words.popleft(), []
    #         found_conditions.append(Condition(field, operator, values))

    #         get_condition_values()
    #         get_bool_operators()

    #     for con in found_conditions:
    #         SqliteValidator.validate_condition(*con)

    #     for bool_op in found_bools:
    #         SqliteValidator.validate_bool_operator(bool_op)
