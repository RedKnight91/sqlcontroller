"""SQL validation"""

from abc import ABC
import re


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


class AbstractValidator(ABC):
    """Abstract class for SQL Validators"""


class SqlValidator(AbstractValidator):
    """Validate SQL keywords, values and clauses"""

    alphanum = re.compile(r"[a-zA-Z0-9]+")
    alphanum_underscore = re.compile(r"[a-zA-Z0-9_]+")
    alphanum_space = re.compile(r"[a-zA-Z0-9\s]+")
    alphanum_underscore_space = re.compile(r"[a-zA-Z0-9\s_]+")

    @staticmethod
    def valid_iterable(var) -> bool:
        """Validate an iterable"""
        return hasattr(var, "__iter__")

    @staticmethod
    def valid_alphanum(name, underscore: bool = False, space: bool = False) -> bool:
        """Validate alphanumeric strings (with/without underscores, spaces)"""
        if underscore and space:
            pattern = SqlValidator.alphanum_underscore_space
        elif underscore:
            pattern = SqlValidator.alphanum_underscore
        elif space:
            pattern = SqlValidator.alphanum_space
        else:
            pattern = SqlValidator.alphanum

        return isinstance(name, str) and bool(re.fullmatch(pattern, name))

    @staticmethod
    def valid_field(field: str) -> bool:
        """Validate field name"""
        return SqlValidator.valid_alphanum(field, True, False)

    @staticmethod
    def valid_type(name) -> bool:
        """Validate field type"""
        return isinstance(name, str) and name.upper() in types

    @staticmethod
    def valid_constraint(name) -> bool:
        """Validate field constraint"""
        return isinstance(name, str) and name.upper() in constraints

    @staticmethod
    def valid_values(values: list) -> bool:
        """Validate values"""

        def is_valid(val) -> bool:
            quotes = ('"', "'")
            has_quotes = val.startswith(quotes) and val.endswith(quotes)
            return has_quotes if isinstance(val, str) else is_numeric(val)

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
    def validate_iterable(var) -> None:
        """Validate iterable type"""
        if not SqlValidator.valid_iterable(var):
            raise TypeError(f"{var} is not iterable")

    @staticmethod
    def validate_str(str_: str) -> None:
        """Validate string type"""
        if not isinstance(str_, str):
            raise TypeError(f"{str_} is not a string")

    @staticmethod
    def validate_alphanum(
        str_: str, underscore: bool = False, space: bool = False
    ) -> None:
        """Validate alphanumeric string"""
        SqlValidator.validate_str(str_)

        if not SqlValidator.valid_alphanum(str_, underscore, space):
            error = f"Not alphanumeric: {str_}"
            raise InvalidAlphanumericError(error)

    @staticmethod
    def validate_field(field: str) -> None:
        """Validate field name"""
        if not SqlValidator.valid_field(field):
            error = f"{field} is not a valid clause field operand."
            raise InvalidSqlOperandError(error)

    @staticmethod
    def validate_type(name) -> None:
        """Validate field type"""
        SqlValidator.validate_str(name)

        if not SqlValidator.valid_type(name):
            error = f"{name} is not a valid data type."
            raise InvalidSqlDataTypeError(error)

    @staticmethod
    def validate_constraint(name) -> None:
        """Validate field constraint"""
        SqlValidator.validate_str(name)

        name = name.upper()

        if not SqlValidator.valid_constraint(name):
            error = f"{name} is not a valid data constraint."
            raise InvalidSqlDataConstraintError(error)

    @staticmethod
    def validate_values(values: str) -> None:
        """Validate field values"""
        if not SqlValidator.valid_values(values):
            error = f"{values} is not a valid clause values operand."
            raise InvalidSqlOperandError(error)

    @staticmethod
    def validate_comparison_operator(comp_op) -> None:
        """Validate clause comparison operator"""
        if not SqlValidator.valid_comparison_operator(comp_op):
            error = f"{comp_op} is not a valid comparison operator."
            raise InvalidSqlOperatorError(error)

    @staticmethod
    def validate_bool_operator(bool_op) -> None:
        """Validate clause boolean operator"""
        if not SqlValidator.valid_bool_operator(bool_op):
            error = f"{bool_op} is not a valid boolean operator."
            raise InvalidSqlOperatorError(error)

    @staticmethod
    def validate_condition(field: str, operator: str, values: str) -> None:
        """Validate clause condition"""
        SqlValidator.validate_field(field)
        SqlValidator.validate_comparison_operator(operator)
        SqlValidator.validate_values(values)

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
    #         SqlValidator.validate_condition(*con)

    #     for bool_op in found_bools:
    #         SqlValidator.validate_bool_operator(bool_op)
