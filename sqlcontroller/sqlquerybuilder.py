"""Build SQL queries"""

from abc import ABC


class BaseSqlClause(ABC):
    """Abstract query clause"""


class SqliteClause(BaseSqlClause):
    """Store a query clause string"""

    def __init__(self, clause: str) -> None:
        self.clause = clause


class BaseSqlQueryBuilder(ABC):
    """Abstract query builder"""


class SqliteQueryBuilder(BaseSqlQueryBuilder):
    """Build Sqlite query strings"""

    @staticmethod
    def build_insert_query(values: list, columns: list = []) -> str:
        """Build an insert query string"""
        column_str = "" if not columns else f"({','.join(columns)})"
        qmarks = ",".join(["?"] * len(values))

        query = f"insert into {{table}}{column_str} values ({qmarks})"
        return query

    @staticmethod
    def build_query_clauses(where: str, order: str = '', limit: int = 0, offset: int = 0) -> str:
        """Build a query's clauses string"""
        where_str = f"where {where}" if where else ""
        order_str = f"order by {order}" if order else ""
        limit_str = f"limit {limit}" if limit else ""
        offset_str = f"offset {offset}" if offset else ""

        clause = "".join([where_str, order_str, limit_str, offset_str])
        return SqliteClause(clause)
