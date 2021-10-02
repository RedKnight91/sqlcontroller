"""Build SQL queries"""

import re
from abc import ABC
from typing import Collection, Iterable, Optional
from sqlcontroller.field import Field


class BaseSqlQueryBuilder(ABC):  # pylint: disable=too-few-public-methods
    """Abstract query builder"""


class SqliteQueryBuilder(BaseSqlQueryBuilder):  # pylint: disable=too-few-public-methods
    """Build Sqlite query strings"""

    @staticmethod
    def build_table_create_query(fields: Iterable[Field]) -> str:
        """Build a create table query"""

        fields_strs = [" ".join([f.name, f.type, *f.constraints]) for f in fields]
        fields_str = ", ".join(fields_strs)

        query = f"create table if not exists {{table}} ({fields_str})"
        return query

    @staticmethod
    def build_insert_query(
        values: Collection, fields: Optional[Iterable] = None
    ) -> str:
        """Build an insert query string"""

        parts = ["insert into {table}"]

        if fields:
            fields_str = ",".join(fields)
            parts.append(f"({fields_str})")

        parts.append("values")

        qmarks = ",".join(["?"] * len(values))
        parts.append(f"({qmarks})")

        query = " ".join(parts)
        return query

    @staticmethod
    def build_query_clauses(
        where: str = "", order: str = "", limit: int = 0, offset: int = 0
    ) -> str:
        """Build a query's clauses string"""

        where = re.sub("^where ", "", where, flags=re.IGNORECASE)
        order = re.sub("^order by ", "", order, flags=re.IGNORECASE)

        parts = []
        if where:
            parts.append(f"where {where}")

        if order:
            parts.append(f"order by {order}")

        if limit:
            parts.append(f"limit {limit}")

        if offset:
            parts.append(f"offset {offset}")

        clause = " ".join(parts)
        return clause
