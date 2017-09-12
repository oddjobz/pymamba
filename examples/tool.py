from lmdb import Cursor, Environment
from sys import argv
from json import loads

env = Environment(argv[1], **{
    'map_size': 1024 * 1024 * 10,
    'subdir': True,
    'metasync': False,
    'sync': True,
    'lock': True,
    'max_dbs': 64,
    'writemap': True,
    'map_async': True
})

db = env.open_db()

with env.begin() as txn:
    with Cursor(db, txn) as cursor:
        if cursor.first():
            while True:
                name = cursor.key().decode()
                print(name)
                if not cursor.next():
                    break

db = env.open_db('_rel_users_addresses_addresses'.encode())
with env.begin() as txn:
    cursor = Cursor(db, txn)
    for key, val in cursor:
        print('addresses-Key="{}" Val="{}"'.format(key, val))

db = env.open_db('_rel_users_addresses_users'.encode())
with env.begin() as txn:
    cursor = Cursor(db, txn)
    for key, val in cursor:
        print('users-----Key="{}" Val="{}"'.format(key, val))

db = env.open_db('rel_users_addresses'.encode())
with env.begin() as txn:
    cursor = Cursor(db, txn)
    for key, val in cursor:
        print('link------Key="{}" Val="{}"'.format(key, val))
