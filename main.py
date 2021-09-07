from sqlcontroller.sqlcontroller import SqlController

with SqlController('test.db') as sql:
	columns = {
		"hash": ("text", "primary key"),
		"name": ("text", "not null"),
		"interval": ("text", "not null"),
		"strategy": ("text", "not null"),
		"indicators": ("text", "not null"),
		"asset": ("text", "not null"),
		"funds": ("text", "not null"),
		"gains": ("real", "not null"),
		"gainsPct": ("real", "not null"),
		"topGainPct": ("real", "not null"),
		"topLossPct": ("real", "not null"),
		"efficiency": ("real", "not null"),
	}

	sql.create_table('testTable', columns)