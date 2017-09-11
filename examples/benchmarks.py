#!/usr/bin/python3

from pymamba import Database
from time import time
from random import random
from subprocess import call


def chunk(tbl, start, count):
    """
    Create a chunk of data
    
    :param tbl: Table to operate on
    :type tbl: Table
    :param start: Starting index 
    :type start: int
    :param count: Number of items
    :type count: int
    """
    begin = time()
    for index, session in enumerate(range(start, start + count)):
        record = {
            'origin': 'linux.co.uk',
            'sid': start + count - index,
            'when': time(),
            'day': int(random() * 6),
            'hour': int(random() * 24)
        }
        tbl.append(record)

    finish = time()
    print("  - {:5}:{:5} - Append Speed/sec = {:.0f}".format(start, count, count / (finish - begin)))


call(['rm', '-rf', 'databases/perfDB'])
print("* No Indecies")
db = Database('databases/ls scperfDB')
table = db.table('sessions')
chunk(table, 0, 5000)
chunk(table, 5000, 5000)
chunk(table, 10000, 5000)
db.close()

call(['rm', '-rf', 'databases/perfDB'])
print("* Indexed by sid, day, hour")
db = Database('databases/perfDB')
table = db.table('sessions')
table.index('by_sid', '{sid}')
table.index('by_day', '{day}')
table.index('by_hour', '{hour}')
chunk(table, 0, 5000)
chunk(table, 5000, 5000)
chunk(table, 10000, 5000)
db.close()

call(['rm', '-rf', 'databases/perfDB'])
print("* Indexed by function")
db = Database('databases/perfDB')
table = db.table('sessions')
table.index('by_multiple', '!{origin}|{day:02}|{hour:02}|{sid:05}')
chunk(table, 0, 5000)
print("")
for doc in table.find('by_multiple', limit=10):
    print('{origin} {day} {hour} {sid} {when}'.format(**doc))


start = 0
count = 5000
begin = time()
for doc in table.find('by_multiple'): pass
finish = time()
print("  - {:5}:{:5} - Append Speed/sec = {:.0f}".format(start, count, count / (finish - begin)))

db.close()


