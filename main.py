from sqlcontroller.sqlcontroller import SqliteController

with SqliteController('test.db') as sql:
    fields = {
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

    sql.create_table('testTable', fields)