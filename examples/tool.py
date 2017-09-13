"""
This is a debugging tool, doesn't do much for now ..
"""

from pymamba import Database, size_mb

db = Database('databases/contacts_db', size=size_mb(10))
for name in db.tables_all:
    if name[0] == '@':
        continue
    table = db.table(name)
    print(name, table.records)
    if name[:4] == 'rel_':
        for i in table.find():
            print( "  ", i)
