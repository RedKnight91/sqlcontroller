"""Build SQL queries"""

import re
from abc import ABC
from typing import Collection
from sqlcontroller.sqlvalidator import AbstractValidator


class BaseSqlQueryBuilder(ABC):  # pylint: disable=too-few-public-methods
    """Abstract query builder"""


class SqliteQueryBuilder(BaseSqlQueryBuilder):  # pylint: disable=too-few-public-methods
    """Build Sqlite query strings"""

    @staticmethod
    def build_table_create_query(validator: AbstractValidator, fields: dict) -> str:
        """Build a create table query"""

        def parse_field(col, specs):
            validator.validate_iterable(specs)

            type_, constraints = specs[0], specs[1:]

            validator.validate_type(type_)
            for con in constraints:
                validator.validate_constraint(con)

            return (col, type_, *constraints)

        fields_strs = [
            " ".join(parse_field(col, specs)) for col, specs in fields.items()
        ]
        fields_str = ", ".join(fields_strs)

        query = f"create table if not exists {{table}} ({fields_str});"
        return query

    @staticmethod
    def build_insert_query(fields: Collection) -> str:
        """Build an insert query string"""
        fields_str = "" if not fields else f"({','.join(fields)})"
        qmarks = ",".join(["?"] * len(fields))

        query = f"insert into {{table}} {fields_str} values ({qmarks});"
        return query

    @staticmethod
    def build_query_clauses(
        where: str, order: str = "", limit: int = 0, offset: int = 0
    ) -> str:
        """Build a query's clauses string"""

        where = re.sub("^where ", "", where, flags=re.IGNORECASE)
        order = re.sub("^order by ", "", order, flags=re.IGNORECASE)

        parts = []
        if where:
            parts.append(f"where {where};")

        if order:
            parts.append(f"order by {order};")

        if limit:
            parts.append(f"limit {limit};")

        if offset:
            parts.append(f"offset {offset};")

        clause = " ".join(parts)
        return clause
