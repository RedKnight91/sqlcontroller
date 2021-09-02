import sqlite3
from abc import ABC, abstractmethod


class InvalidAlphanumericError(Exception):
	pass

class InvalidSqlDataTypeError(Exception):
	pass

class InvalidSqlDataConstraintError(Exception):
	pass


class _BaseSqlController(ABC):

	def __init__(self, db):
		self.db = db
		self.connection = self.cursor = None

	def __enter__(self) -> '_BaseSqlController':
		self.connection = self.connect_db()
		self.cursor = self.get_cursor()
		return self

	def __exit__(self, exc_type, exc_val, exc_tb) -> None:
		self.disconnect_db()

	@abstractmethod
	def connect_db(self) -> sqlite3.Connection:
		pass

	@abstractmethod
	def disconnect_db(self) -> None:
		pass

	@abstractmethod
	def save_db(self) -> None:
		pass

	@abstractmethod
	def get_cursor(self) -> sqlite3.Cursor:
		pass

	@abstractmethod
	def create_table(self, name: str, columns: dict) -> None:
		pass

	@abstractmethod
	def delete_table(self, name: str) -> None:
		pass

	@abstractmethod
	def add_row(self, table: str, data: list) -> None:
		pass

	@abstractmethod
	def get_row(self, table: str, conditions: list) -> None:
		pass

	@abstractmethod
	def get_rows(self, table: str, conditions: list) -> list:
		pass

	@abstractmethod
	def get_all_rows(self, table: str) -> list:
		pass

	@abstractmethod
	def update_row(self, table: str, values: dict, search, order, limit = -1, offset = 0) -> None:
		pass

	@abstractmethod
	def delete_rows(self, table: str, conditions: list) -> None:
		pass

	@abstractmethod
	def delete_all_rows(self, table: str) -> None:
		pass

	def _execute(self, query: str, table: str = None, values:list = []) -> None:
		self.validate_alphanum(table)
		query = query.format(table = table)
		return self.cursor.execute(query, values)

	def _executemany(self, query: str, table: str = None, valuelists:list = []) -> None:
		self.validate_alphanum(table)
		query = query.format(table = table)
		return self.cursor.executemany(query, valuelists)

	@staticmethod
	def validate_alphanum(name) -> None:
		if not isinstance(name, str):
			raise TypeError(f'{name} is not a string')

		for c in name:
			if not c.isalnum():
				raise InvalidAlphanumericError(f'{c} is not an alphanumeric character.')

	@staticmethod
	def validate_type(name) -> None:
		if not isinstance(name, str):
			raise TypeError(f'{name} is not a string')

		types = {
			'NULL',
			'INTEGER',
			'REAL',
			'TEXT',
			'BLOB'
		}
		if name.upper() not in types:
			raise InvalidSqlDataTypeError(f'{name} is not a valid data type.')

	@staticmethod
	def validate_constraint(name) -> None:
		if not isinstance(name, str):
			raise TypeError(f'{name} is not a string')
		
		constraints = {
			'NOT NULL',
			'UNIQUE',
			'PRIMARY KEY',
			# 'FOREING KEY',
			# 'CHECK',
			# 'DEFAULT'
		}
		if name.upper() not in constraints:
			raise InvalidSqlDataConstraintError(f'{name} is not a valid data constraint.')

	@staticmethod
	def validate_iterable(var) -> None:
		try:
			iter(var)
		except TypeError:
			raise TypeError(f'{var} is not iterable')



class SqlController(_BaseSqlController):
	
	def connect_db(self) -> sqlite3.Connection:
		self.connection = sqlite3.connect(self.db)
		return self.connection
	
	def disconnect_db(self) -> None:
		self.cursor = None
		self.connection.close()
		self.connection = None

	def save_db(self) -> None:
		self.connection.commit()

	def get_cursor(self) -> sqlite3.Cursor:
		self.cursor = self.connection.cursor()
		return self.cursor

	def has_table(self, name: str) -> bool:
		try:
			self._execute('select * from {table}', name)
			return True
		except sqlite3.OperationalError:
			return False


	def create_table(self, name: str, columns: dict) -> None:
		'''Supply columns as {name: (type, constraint, constraint, ...), name: (...), ...}'''

		def parse_column(col, specs):
			self.validate_iterable(specs)

			type_ = specs[0]
			constraints = specs[1:]

			self.validate_type(type_)
			[self.validate_constraint(c) for c in constraints]

			return (col, type_, *constraints)

		columnStrs = [''.join(parse_column(col, specs)) for col, specs in columns.items()]
		columnsStr = ','.join(columnStrs)

		q = f'create table if not exists {{table}} ({columnsStr});'
		self._execute(q, name)

	def delete_table(self, name: str) -> None:
		q = 'drop table {table};'
		self._execute(q, name)

	def _build_add_query(self, table: str, values: list, columns: list = []) -> str:
		columnStr = '' if not columns else f"({','.join(columns)})"
		qmarks = ','.join(['?'] * len(values))

		query = f'insert into {{table}}{columnStr} values ({qmarks})'
		return query

	def add_row(self, table: str, values: list, columns: list = []) -> None:
		q = self._build_add_query(table, values, columns)
		self._execute(q, table, values)

	def add_rows(self, table: str, valuelists: list, columns: list = []) -> None:
		q = self._build_add_query(table, valuelists, columns)
		self._executemany(q, table, valuelists)

	def get_row(self, table: str, conditions: list) -> None:
		raise NotImplementedError

		# TODO How to supply conditions
		q = f'select * from {{table}} where {conditions}'
		self._execute(q, table)
		return self.cursor.fetchone()

	def get_rows(self, table: str, conditions: list) -> list:
		raise NotImplementedError

		# TODO How to supply conditions
		q = f'select * from {{table}} where {conditions}'
		self._execute(q, table)
		return self.cursor.fetchall()

	def get_all_rows(self, table: str) -> list:
		q = f'select * from {{table}}'
		self._execute(q, table)
		return self.cursor.fetchall()

	def update_row(self, table: str, values: dict, search, order, limit = -1, offset = 0) -> None:
		raise NotImplementedError

		valuesStr = ','.join([f'{k} = {v}' for k,v in values.items()])

		# TODO How to supply search, order
		# TODO Default search/order values
		q = f'update {{table}} set {valuesStr} where {search} order by {order} limit {limit} offset {offset}'
		self._execute(q, table)

	def delete_rows(self, table: str, conditions: list) -> None:
		raise NotImplementedError

		# TODO How to supply conditions
		q = f'delete from {{table}} where {conditions}'
		self._execute(q, table)

	def delete_all_rows(self, table: str) -> None:
		q = f'delete from {{table}}'
		self._execute(q, table)