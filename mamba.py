#!/usr/bin/python3

from unittest import TestCase
from lmdb import Cursor, Environment
from ujson import loads, dumps
from time import time
from sys import _getframe
from random import random
from uuid import uuid1 as UUID
from pprint import PrettyPrinter

##############################################################################
#
#   utility routines
#
##############################################################################
def debug(self, msg):
    """debug routine"""
    if not self._debug: return
    line = _getframe(1).f_lineno
    name = _getframe(1).f_code.co_name
    print("{}: #{} - {}".format(name, line, msg))

def anonymous(text):
    scope = {}
    exec("def func" + text, scope)
    return scope['func']

##############################################################################
#
#   CLASS :: Index
#
##############################################################################
class Index(object):

    _debug = False

    def __init__(self, env, index):
        self._env = env
        self._func = anonymous(index['func'])
        self._name = index['name']
        self._conf = index['config']
        self.init_index()

    def init_index(self):
        """open the index"""
        self._conf['key'] = self._name.encode()
        self._db = self._env.open_db(**self._conf)

    def drop(self, txn):
        """drop a database index"""
        return txn.drop(self._db, delete=False)

    def put(self, txn, key, record):
        """put a new record in the index"""
        return txn.put(self._func(record).encode(), key.encode(), db=self._db)

    def delete(self, txn, key, record):
        """delete a record from the index"""
        return txn.delete(self._func(record).encode(), key, db=self._db)

    def get(self, txn, record):
        """read a record based on the primary key"""
        return txn.get(self._func(record).encode(), db=self._db)

    def cursor(self, txn):
        """return a cursor into the index"""
        return Cursor(self._db, txn)

    @property
    def count(self, txn):
        """return the number of items present in the index"""
        return txn.stat(self._db).get('entries', 0)

##############################################################################
#
#   CLASS :: Table
#
##############################################################################
class Table(object):

    _debug = True
    _index_array = {}

    def __init__(self, env, name, indexes):
        """set up access to a table object"""
        self._env = env
        self._name = name
        self._indexes = indexes
        self._init_autoinc()
        self._init_indexes()

    def _init_autoinc(self):
        """set the autoinc sequence # to the last id on file"""
        self._db = self._env.open_db(self._name.encode('ascii'))

    def _init_indexes(self):
        """set up the indexes relating to this table"""
        for name, index in self._indexes.items():
            debug(self, '++ {}'.format(index))
            self._index_array[name] = Index(self._env, index)

    def open_index(self, index):
        self._index_array[index['name']] = Index(self._env, index)

    def get(self, key, txn=None):
        """recover a record using the primary key"""
        def get_actual():
            record = txn.get(key.encode(), db=self._db)
            if not record: return None
            return loads(bytes(record))
        if txn: return get_actual()
        with self._env.begin() as txn: return get_actual()

    def drop(self, txn=None):
        """drop all records from the table - USE WITH CARE!"""
        def drop_actual():
            txn.drop(self._db, delete=False)
            for _, index in list(self._index_array.items()):
                index.drop(txn)
        if txn: drop_actual()
        with self._env.begin(write=True) as txn: drop_actual()

    def put(self, record, txn=None):
        """put a new record into the table"""
        def put_actual():
            key = str(UUID())
            txn.put(key.encode(), dumps(record).encode(), db=self._db, append=True)
            for _, index in self._index_array.items():
                index.put(txn, key, record)
            record['_id'] = key
        if txn: put_actual()
        with self._env.begin(write=True) as txn: put_actual()

    def put_raw(self, key, record):
        with self._env.begin(write=True) as txn:
            txn.put(key.encode(), dumps(record).encode(), db=self._db)

    def get_raw(self, key):
        with self._env.begin(write=True) as txn:
            record = txn.get(key.encode(), db=self._db)
            if not record: return None
            return loads(bytes(record))

    def upd(self, record, txn=None):
        """put a new record into the table"""
        def upd_actual():
            key = record.get('_id', None)
            if not key: return False
            return txn.replace(key.encode(), dumps(record).encode(), db=self._db)
        if txn: upd_actual()
        with self._env.begin(write=True) as txn: upd_actual()

    def delete(self, key, record, txn=None):
        """remove a record from the table"""
        def delete_actual():
            txn.delete(key.encode(), db=self._db)
            for _, index in self._index_array.items():
                index.delete(txn, key, record)
        if txn: delete_actual()
        with self._env.begin(write=True) as txn: delete_actual()

    @property
    def count(self, txn=None):
        """return a count of the number of records in the table"""
        if txn: return txn.stat(self._db).get('entries', 0)
        with self._env.begin(write=True) as txn: return txn.stat(self._db).get('entries', 0)

    @property
    def counts(self, txn=None):
        """return a count of the number of records in each index"""
        def counts_actual():
            results = {'_id': txn.stat(self._db).get('entries', 0)}
            for name, index in self._index_array.items():
                results[name] = index.count
            return results
        if txn: return counts_actual()
        with self._env.begin(write=True) as txn: return counts_actual()

    def reindex(self, txn=None):
        """regenerate all indexes"""
        def reindex_actual():
            for _, index in self._index_array.items():
                index.drop(txn)
                with Cursor(self._db, txn) as cursor:
                    if not cursor.first(): continue
                    while True:
                        key, record = cursor.item()
                        record = loads(bytes(record))
                        index.put(txn, key, record)
                        if not cursor.next(): break
        if txn: return reindex_actual()
        with self._env.begin(write=True) as txn: return reindex_actual()

    def create_index(self, name, index):
        """create a new index"""
        if name in self._index_array:
            return False
        self._index_array[index['name']] = Index(self._env, index)
        return index

    def iterate_callback(self, data):
        if data:
            self.values.append(data)
            for k,v in data.items():
                kl = len(k)
                vl = len(str(v))
                mx = max(kl, vl)
                if ((k in self.lengths) and (mx > self.lengths[k])) or (k not in self.lengths):
                    self.lengths[k] = mx
            return
        #
        separator = ''
        fmt = ''
        data = {}
        #
        for k,v in self.lengths.items():
            separator += '+'+'-'*(v+2)
        for k,v in self.lengths.items():
            fmt += '| {'+k+':'+str(v)+'} '
            data[k] = k
        separator += '+'
        fmt += '|'
        print(separator)
        print(fmt.format(**data))
        print(separator)
        for item in self.values:
            print(fmt.format(**item))
        print(separator)

    def iterate(self, callback=None):
        self.values = []
        self.lengths = {}
        count = 0
        if not callback: callback = self.iterate_callback
        with self._env.begin() as txn:
            with Cursor(self._db, txn) as cursor:
                if not cursor.first(): return
                while True:
                    key, record = cursor.item()
                    record = loads(bytes(record))
                    callback(record)
                    if not cursor.next(): break
                    count += 1
                    if count>10: break
        callback(None)

##############################################################################
#
#   CLASS :: Schema
#
##############################################################################
class Schema(object):

    _tables = {}
    _schema = {}

    def __init__(self, db):
        self._db = db
        self._table = db.open('_schema', {})
        self._schema = self._table.get_raw('schema')
        if not self._schema: self._schema = {}
        if not 'tables' in self._schema: self._schema['tables'] = {}

    def _flush(self):
        self._table.put_raw('schema', self._schema)

    def create_table(self, table_name):
        if table_name in self._tables:
            return {'status': 'fail', 'reason': 'table exists'}
        self._schema['tables'][table_name] = {'indexes': {}}
        self._flush()
        self._db.open(table_name, {})
        return {'status': 'ok'}

    def create_index(self, table_name, index_name, index):
        if table_name not in self._schema['tables']:
            return {'status': 'fail', 'reason': 'no such table'}
        if index_name in self._schema['tables'][table_name]['indexes']:
            return {'status': 'fail', 'reason': 'index exists'}
        self._schema['tables'][table_name]['indexes'][index_name] = index
        self._flush()
        table = self._db.use(table_name)
        table.open_index(index)
        return {'status': 'ok'}

    def open(self):
        for table in self._schema['tables']:
            indexes = self._schema['tables'][table]['indexes']
            self._tables[table] = self._db.open(table, indexes)

##############################################################################
#
#   CLASS :: Database
#
##############################################################################
class Database(object):

    _tables = {}
    _config = {
        'map_size': 1024*1024*1024 * 2,
        'subdir': True,
        'metasync': False,
        'sync': False,
        'max_dbs': 12,
        'writemap': True
    }
    def __init__(self, *args, **argv):
        _config = dict(self._config, **argv.get('env', {}))
        self._env = Environment(args[0], **_config)
        self._schema = Schema(self)
        self._schema.open()

    def __del__(self):
        self.close()

    def close(self):
        if not self._env: return
        self._env.close()
        self._env = None

    def open(self, name, indexes):
        self._tables[name] = Table(self._env, name, indexes)
        return self._tables[name]

    def use(self, name):
        return self._tables[name]

    def schema(self):
        """return the schema for the current db"""
        return self._schema

    def create_table(self, name):
        return self._schema.create_table(name)

    def create_index(self, table, name, field, duplicates):
        """create a new index"""
        index = {
            'name': name,
            'func': field,
            'config': {'dupsort': duplicates}
        }
        return self._schema.create_index(table, name, index)

import itertools

class MDB:
    """class for chaining operatins together"""
    def __init__(self, database):
        self._db = Database(database)
        self._pipe = []

    def use(self, table):
        self._table = self._db.use(table)
        return self

    def scan(self):
        def seq(item=None):
            with self._db._env.begin() as txn:
                with Cursor(self._table._db, txn) as cursor:
                    if not cursor.first(): return
                    while True:
                        key,val = cursor.item()
                        yield (key, loads(bytes(val)))
                        if not cursor.next(): return
        
        self._pipe.append(seq)
        return self

    def pp(self):
        def pp(item=None):
            if item[1]['index'] == 5: return
            yield item
        self._pipe.append(pp)
        return self

    def filter(self, expression):
        def fil(item=None):
            if not expression(item): return
            yield item
        self._pipe.append(fil)
        return self

    def print(self, callback=print):
        def pri(item=None):
            callback(item)
        self._pipe.append(pri)
        self.run()

    def run(self):
        def recurse(i, item):
            for obj in self._pipe[i](item):
                if (i+2)<len(self._pipe):
                    recurse(i+1, obj)
                else:
                    self._pipe[i+1](obj)
        recurse(0, None)


pp = PrettyPrinter(indent=1, width=120)
db = Database('demodb')
print(db.create_table('my_test'))
print(db.create_index('my_test', name='by_origin', field="(record):return record.get('origin','')", duplicates=True))
pp.pprint(db.schema()._schema)
table = db.use('my_test')
for i in range(10):
    table.put({'index':i, 'origin': 'long string '+str(i)})
db.use('my_test').iterate()

def myfilter(item):
    k, v = item
    return v['index']>2 and v['index']<6

def myp(item):
    print(item[0])

MDB('demodb').use('my_test').scan().filter(myfilter).print(myp)

